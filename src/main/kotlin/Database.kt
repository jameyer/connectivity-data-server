import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.ntnu.jameyer.master.shared.MeasurementData
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IdTable
import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.statements.BatchInsertStatement
import org.jetbrains.exposed.sql.transactions.transaction
import org.nield.kotlinstatistics.median
import org.postgis.PGgeometry
import org.postgis.Point
import org.postgresql.util.PGobject
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.lang.Math.abs
import java.net.URL
import java.util.*
import kotlin.math.roundToInt
import org.jetbrains.exposed.sql.Database as _Database

/**
 * Responsible for interacting with database.
 *
 * Exposed library used as SQL library on top of JDBC driver.
 * HikariCP library used for connection pooling.
 * Underlying PostgreSQL database with PostGIS for geographical data enhancements is required.
 */

private const val WGS84_SRID = 4326

class Database {
    companion object {
        /** Radius (size) of an area. */
        private const val AREA_RADIUS_METERS = 50.0f

        /**
         * Maximum segment length for routes when detecting dead spots. Smaller value gives more segments
         * and thereby more accurate detection, but it does increase amount of vertexes and thus makes
         * computation more costly.
         */
        private const val ROUTE_SEGMENT_LENGTH_METERS = 10

        /**
         * If an area is not found within this distance (for each vertex) of route's polyline a dead spot
         * has been found.
         */
        private const val DEAD_SPOT_DISTANCE_THRESHOLD_METERS = 50

        /** The UDP send interval that the measurement client uses in milliseconds. */
        private const val MEASUREMENT_UDP_SEND_INTERVAL_MS = 250

        private const val GOOGLE_DIRECTIONS_API_KEY_FILE = "google-directions-api-key.txt"
    }

    private var googleDirectionsApiKey: String? = null

    private val logger = LoggerFactory.getLogger(Database::class.java)

    private val hikariConfig: HikariConfig = HikariConfig().apply {
        // (Replace with your own database parameters)
        jdbcUrl = "jdbc:postgresql:master3"
        driverClassName = "org.postgresql.Driver"
        username = "postgres"
        password = "1234"
        maximumPoolSize = 3

        // TODO:
        // hikariConfig.maxLifetime = ..
        // "We strongly recommend setting this value, and it should be several seconds shorter than any database
        // or infrastructure imposed connection time limit."
    }

    init {
        // Read Google Directions API Key
        try {
            googleDirectionsApiKey = File(GOOGLE_DIRECTIONS_API_KEY_FILE).readLines().firstOrNull()
            if (googleDirectionsApiKey == null) {
                logger.warn("Could not read Google Directions API Key.")
            } else {
                logger.info("Read Google Directions API Key: $googleDirectionsApiKey")
            }
        } catch (ex: FileNotFoundException) {
            logger.warn("Could not read Google Directions API Key.")
        }

        HikariDataSource(hikariConfig).apply {
            _Database.connect(this)
        }

        createTablesIfNotExist()
    }

    private fun createTablesIfNotExist() {
        transaction {
            addLogger(StdOutSqlLogger)

            SchemaUtils.create(Measurements)
            SchemaUtils.create(Areas)
            SchemaUtils.create(AreaPaths)
        }
    }

    private fun getNextTripId(): Int {
        return transaction {
            addLogger(StdOutSqlLogger)

            val max = Measurements
                    .slice(Measurements.tripId.max())
                    .selectAll()
                    .firstOrNull()?.get(Measurements.tripId.max()) ?: 0

            return@transaction max + 1
        }
    }

    fun insertMeasurements(measurements: List<MeasurementData>) {
        if (measurements.isEmpty()) return

        val startTime = System.currentTimeMillis()
        val tripId = getNextTripId()
        val batchInsertMeasurements = BatchInsertStatement(Measurements)
        val batchInsertNextArea = BatchInsertStatement(AreaPaths)

        var newAreaId: EntityID<Int>?
        var lastAreaId: EntityID<Int>? = null
        var previousAreaId: EntityID<Int>? = null
        var lastRelevantBearing = 0.0f
        var previousData: MeasurementData? = null

        measurements.sortedBy { it.packetId }.forEach { m ->
            // Discard WiFi measurements
            if (m.networkType == "WiFi") return@forEach

            newAreaId = getOrCreateArea(m.latitude, m.longitude, m.accuracy, m.networkType)
            if (newAreaId == null) {
                println("Could not create or get area for ${m.latitude}, ${m.longitude}.")

                return@forEach
            }

            if (newAreaId!!.value != lastAreaId?.value) {
                if (lastAreaId != null) {
                    batchInsertNextArea.addBatch()
                    batchInsertNextArea[AreaPaths.areaId] = lastAreaId!!
                    batchInsertNextArea[AreaPaths.nextAreaId] = newAreaId
                    batchInsertNextArea[AreaPaths.previousAreaId] = previousAreaId
                    batchInsertNextArea[AreaPaths.bearing] = lastRelevantBearing
                    batchInsertNextArea[AreaPaths.tripId] = tripId

                    previousAreaId = lastAreaId
                }
            }

            lastAreaId = newAreaId!!
            if (m.bearing > 0.0f) {
                lastRelevantBearing = m.bearing
            }

            val ipdv =
                    if (previousData != null
                            && m.packetId - previousData!!.packetId == 1
                            && previousData!!.serverReplyTime != null
                            && m.serverReplyTime != null) {
                        (m.serverReplyTime!! - previousData!!.serverReplyTime!! - MEASUREMENT_UDP_SEND_INTERVAL_MS).toInt()
                    } else null

            batchInsertMeasurements.addBatch()
            batchInsertMeasurements[Measurements.areaId] = newAreaId!!
            batchInsertMeasurements[Measurements.packetId] = m.packetId
            batchInsertMeasurements[Measurements.tripId] = tripId
            batchInsertMeasurements[Measurements.speed] = m.speed
            batchInsertMeasurements[Measurements.bearing] = m.bearing
            batchInsertMeasurements[Measurements.roundTripTime] = m.roundTripTime
            batchInsertMeasurements[Measurements.serverReplyTime] = m.serverReplyTime
            batchInsertMeasurements[Measurements.signalStrength] = m.signalStrength
            batchInsertMeasurements[Measurements.networkType] = m.networkType
            batchInsertMeasurements[Measurements.ipdv] = ipdv
            batchInsertMeasurements[Measurements.jitter] = ipdv?.let{ abs(it) }
            batchInsertMeasurements[Measurements.gsmAsuLevel] = m.gsmAsuLevel
            batchInsertMeasurements[Measurements.lteAsuLevel] = m.lteAsuLevel

            previousData = m
        }

        try {
            transaction {
                addLogger(StdOutSqlLogger)

                batchInsertMeasurements.execute(this)
            }

            transaction {
                addLogger(StdOutSqlLogger)

                batchInsertNextArea.execute(this)
            }
        } catch (ex: Exception) {
            println("${::insertMeasurements.name}: $ex")
        }

        println("${::insertMeasurements.name}: ${System.currentTimeMillis() - startTime} ms")
    }

    private data class AreaData(var averageRoundTripTime: Double = 1.0,
                                var medianRoundTripTime: Double = 1.0,
                                var roundTripTimeQuality: Double = 1.0,
                                var averageSignalStrength: Double = 1.0,
                                var jitterRatio: Double = 0.0,
                                var packetLossRatio: Double = 0.0,
                                var commonNetworkType: String? = null) {
        val performance: Double
            get() = (1.0 - packetLossRatio) *
                    (1.0 - jitterRatio) *
                    roundTripTimeQuality *
                    averageSignalStrength
    }

    private fun getAreaData(areaId: Int?, tripIds: List<Int>?, networkType: String?): Map<Int, AreaData> {
        val startTime = System.currentTimeMillis()
        val retval = transaction {
            // addLogger(StdOutSqlLogger)

            val query = Measurements.select { Measurements.networkType neq "WiFi" } // Filter out any WiFi measurements
            if (areaId != null) query.andWhere { Measurements.areaId eq areaId }
            if (networkType != null) query.andWhere { Measurements.networkType eq networkType }
            if (tripIds != null) addTripIdsToQuery(tripIds, query)

            return@transaction query.groupBy { it[Measurements.areaId].value }.entries.associate { entry ->
                val rowList = entry.value
                val rowCount = rowList.size
                val rows = rowList.asSequence()
                val data = AreaData()

                if (rows.any()) {
                    val roundTripTimeRows = rows
                            .filter { it[Measurements.roundTripTime] != null }
                    val roundTripTimes = roundTripTimeRows
                            .map { it[Measurements.roundTripTime]!!.toDouble() }
                    data.averageRoundTripTime = roundTripTimes
                            .run { if (any()) average() else 1.0 }
                    data.medianRoundTripTime = roundTripTimes
                            .run { if (any()) median() else 1.0 }
                    data.roundTripTimeQuality = roundTripTimeRows.map {
                        getRoundTripTimeQuality(it[Measurements.roundTripTime]!!, it[Measurements.networkType])
                    }.run { if (any()) average() else 1.0 }
                    data.averageSignalStrength = rows
                            .mapNotNull {
                                val itNetworkType = it[Measurements.networkType]
                                if (itNetworkType == "LTE" && it[Measurements.lteAsuLevel] != null) {
                                    return@mapNotNull it[Measurements.lteAsuLevel]!! / 97.0 // lteAsuToFraction(it[Measurements.lteAsuLevel]!!)
                                } else if (it[Measurements.gsmAsuLevel] != null) {
                                    return@mapNotNull it[Measurements.gsmAsuLevel]!! / 31.0 // gsmAsuToFraction(it[Measurements.gsmAsuLevel]!!)
                                }

                                return@mapNotNull it[Measurements.signalStrength]?.toDouble()
                            }.run { if (any()) average() else 1.0 }

                    data.packetLossRatio = rows.count { it[Measurements.roundTripTime] == null }.toDouble() / rowCount
                    data.jitterRatio = getJitterRatio(rows)
                    data.commonNetworkType = rows
                            .map { it[Measurements.networkType] }
                            .groupingBy { it }
                            .eachCount() // gives Map<String, Int> with counts
                                         // for each value.
                            .maxBy { it.value }?.key
                }

                return@associate entry.key to data
            }
        }

        println("${::getAreaData.name}($areaId, $tripIds, $networkType): ${System.currentTimeMillis() - startTime} ms")

        return retval
    }

    // Values below are tentative.
    private fun getRoundTripTimeQuality(roundTripTime: Int, networkType: String?): Double {
        when (networkType) {
            "EDGE" -> {
                if (roundTripTime < 200) return 1.0
                if (roundTripTime < 400) return 0.8
                if (roundTripTime < 600) return 0.6
                if (roundTripTime < 800) return 0.4

                return 0.2
            }

            "HSPA", "HSUPA", "HSDPA" -> {
                if (roundTripTime < 150) return 1.0
                if (roundTripTime < 300) return 0.8
                if (roundTripTime < 450) return 0.6
                if (roundTripTime < 600) return 0.4

                return 0.2
            }

            "HSPA+" -> {
                if (roundTripTime < 100) return 1.0
                if (roundTripTime < 200) return 0.8
                if (roundTripTime < 300) return 0.6
                if (roundTripTime < 400) return 0.4

                return 0.2
            }

            else -> {
                if (roundTripTime < 80) return 1.0
                if (roundTripTime < 120) return 0.8
                if (roundTripTime < 160) return 0.6
                if (roundTripTime < 200) return 0.4

                return 0.2
            }
        }
    }

    /** Convert a GSM ASU level (0 - 31) to a number between 0 and 1 (non-linearly).
     *  Quadratic regression was used to obtain a non-linear function for ASU levels based on Google
     *  source code (SignalStrength.java) depicting what ASU levels are considered "excellent", "good", "moderate" etc. */
    private fun gsmAsuToFraction(x: Int): Double {
        // Use Max, Min to make sure number is between 0 and 1.
        return Math.max(0.0, Math.min(1.0,
                // Function found with quadratic regression based on:
                // https://www.sbc-support.com/en/faq/101171/
                // and also
                // https://android.googlesource.com/platform/frameworks/base.git/+/master/telephony/java/android/telephony/SignalStrength.java (see method getGsmLevel)
                -0.1457 + 0.06973 * x - 0.0005951 * Math.pow(x.toDouble(), 2.0)
        ))
    }

    /** Convert an LTE ASU level (0 - 97) to a number between 0 and 1.
     *  Quadratic regression was used to obtain a non-linear function for ASU levels based on Google
     *  source code (CellSignalStrengthLte.java) depicting what ASU levels are considered "excellent", "good", "moderate" etc. */
    private fun lteAsuToFraction(x: Int): Double {
        // Use Max, Min to make sure number is between 0 and 1.
        return Math.max(0.0, Math.min(1.0,
                // Function found with quadratic regression based on:.
                // https://android.googlesource.com/platform/frameworks/base/+/master/telephony/java/android/telephony/CellSignalStrengthLte.java (see method getLevel)
                -0.01668 + 0.01567 * x + 0.0001228 * Math.pow(x.toDouble(), 2.0)
        ))
    }

    /** Adds (trip_id = tripIds[0] or trip_id = tripIds[1] ..) to query. */
    private fun addTripIdsToQuery(tripIds: List<Int>, query: Query) {
        val tripIdSize = tripIds.size
        if (tripIdSize > 0) {
            query.andWhere { tripIds.subList(1, tripIdSize).fold(Measurements.tripId eq tripIds[0]) { accumulator, next ->
                accumulator or (Measurements.tripId eq next) }
            }
        }
    }

    /**
     * Finds jitter percentage by using interarrival times for consecutive packets for each trip
     * in the given (measurement) rows and returns the average of these.
     */
    private fun getJitterRatio(rows: Sequence<ResultRow>): Double {
        return rows
                .groupBy { it[Measurements.tripId] }.values // Interarrival times are only relevant to each other within a trip, so group by that.
                .map { list ->
                    if (list.any { it[Measurements.jitter] != null }) {
                        return@map list
                                .mapNotNull { it[Measurements.jitter]?.let { j -> j.toDouble() / MEASUREMENT_UDP_SEND_INTERVAL_MS } }
                                .average()
                    }

                    val sortedValues = list.sortedBy { r -> r[Measurements.packetId] }
                    val interarrivalTimes = mutableListOf<Long>()

                    var i = 0
                    while (i < sortedValues.size - 1) {
                        val packet1 = sortedValues[i]
                        val packet2 = sortedValues[i + 1]
                        i++

                        if (packet2[Measurements.packetId] - packet1[Measurements.packetId] != 1
                                || packet1[Measurements.serverReplyTime] == null
                                || packet2[Measurements.serverReplyTime] == null) {
                            continue
                        }

                        interarrivalTimes.add(packet2[Measurements.serverReplyTime]!! - packet1[Measurements.serverReplyTime]!!)
                    }

                    if (interarrivalTimes.size == 0) {
                        return@map 0.0
                    } else {
                        val meanJitter = interarrivalTimes.map { abs(it - MEASUREMENT_UDP_SEND_INTERVAL_MS) }.average()
                        // Jitter percentage: How much is the mean jitter compared to the send interval (period).
                        return@map meanJitter / MEASUREMENT_UDP_SEND_INTERVAL_MS
                    }
                }.average()
    }

    /**
     * Uses database CORR(x, y) function to calculate correlations for aggregated data on the given trip.
     */
    fun getCorrelations(tripIds: List<Int>?, networkType: String?): String {
        val data = getAreaData(null, tripIds, networkType).values
        val types = listOf("averageRoundTripTime", "medianRoundTripTime", "averageSignalStrength", "jitterRatio", "packetLossRatio")

        // Combine all items in types list with each other (except self).
        val combinations = mutableListOf<Pair<String, String>>()
        for (i in 0 until types.size) {
            for (j in i + 1 until types.size) {
                combinations.add(Pair(types[i], types[j]))
            }
        }

        // Select data from AreaData based on type string.
        val mapType = { type: String, singleData: AreaData ->
            when (type) {
                "averageRoundTripTime" -> singleData.averageRoundTripTime
                "medianRoundTripTime" -> singleData.medianRoundTripTime
                "averageSignalStrength" -> singleData.averageSignalStrength
                "jitterRatio" -> singleData.jitterRatio
                "packetLossRatio" -> singleData.packetLossRatio
                else -> 0.0
            }
        }

        val sqlUnnest =   { type: String, dataList: List<Double?> ->
            "UNNEST(ARRAY[${dataList.joinToString()}]) AS $type"
        }

        val results = mutableListOf<String>()
        transaction {
            addLogger(StdOutSqlLogger)

            combinations.forEach { c ->
                val firstData = data.map { mapType(c.first, it) }
                val secondData = data.map { mapType(c.second, it) }
                val rawSql = "SELECT CORR(${c.first}, ${c.second}) FROM (SELECT ${sqlUnnest(c.first, firstData)}, ${sqlUnnest(c.second, secondData)}) AS Q"

                exec(rawSql) {
                    if (it.next()) {
                        val result = it.getDouble(1)

                        results.add("${c.first}, ${c.second}: $result")
                    }
                }
            }
        }

        return results.joinToString("\n")
    }

    private fun getOrCreateArea(latitude: Double, longitude: Double, accuracy: Float, networkType: String?): EntityID<Int>? {
        val foundAreaRow = getArea(latitude, longitude)
        val areaId: EntityID<Int>?

        if (foundAreaRow != null) {
            areaId = foundAreaRow[Areas.id]

            /** Lower number for accuracy is better (@see [Areas.accuracy]). */
            val oldAccuracy = foundAreaRow[Areas.accuracy]
            if (oldAccuracy > AREA_RADIUS_METERS && accuracy < oldAccuracy) {
                // Old accuracy was poor, update the area to the given location.
                updateAreaLocation(foundAreaRow[Areas.id], latitude, longitude, accuracy)
            }

            // Note that areaRow might contain outdated data as this point, so update its content (location, accuracy)
            // if you plan on returning it in the future..
        } else {
            // No area found, create it.
            areaId = createArea(latitude, longitude, accuracy, networkType)
        }

        return areaId
    }

    /**
     * Try to find area within [AREA_RADIUS_METERS] of given location.
     *
     * @return EntityID (row id) of area
     */
    private fun getArea(latitude: Double, longitude: Double): ResultRow? {
        return transaction {
            // addLogger(StdOutSqlLogger)

            Areas.select(object : Op<Boolean>() {
                override fun toSQL(queryBuilder: QueryBuilder): String {
                    val newGeography = "ST_SetSRID(ST_Point($longitude, $latitude), $WGS84_SRID)::geography"

                    // Note that the last parameter of ST_DWithin must be a literal argument (constant value).
                    // see https://stackoverflow.com/questions/40919364/st-dwithin-does-not-use-index-with-non-literal-argument
                    // Using a function like GREATEST(..) will prevent the correct index from being used
                    // and cause worse performance.
                    return "ST_DWithin(${Areas.location.name}, $newGeography, $AREA_RADIUS_METERS)"
                }
            }).limit(1).firstOrNull()
        }
    }

    private fun createArea(latitude: Double, longitude: Double, accuracy: Float, networkType: String?): EntityID<Int>? {
        return transaction {
            addLogger(StdOutSqlLogger)

            Areas.insert {
                // Important that PostGIS Point takes longitude as x, and latitude as y, not the other way around.
                it[Areas.location] = Point(longitude, latitude)
                it[Areas.accuracy] = accuracy
            } get Areas.id
        }
    }

    private fun updateAreaLocation(areaId: EntityID<Int>, latitude: Double, longitude: Double, accuracy: Float) {
        transaction {
            addLogger(StdOutSqlLogger)

            Areas.update({ Areas.id eq areaId }) {
                it[Areas.location] = Point(longitude, latitude)
                it[Areas.accuracy] = accuracy
            }
        }
    }

    fun findDeadSpotsOnRoute(fromLat: Double, fromLon: Double, toLat: Double, toLng: Double, tripIds: List<Int>?, minPerformance: Float): String {
        val url = "https://maps.googleapis.com/maps/api/directions/json?origin=$fromLat,$fromLon&destination=$toLat,$toLng&key=$googleDirectionsApiKey"
        val googleStartTime = System.currentTimeMillis()
        val jsonString = URL(url).readText()
        println("Google Directions API: ${System.currentTimeMillis() - googleStartTime} ms")

        val json = JsonParser().parse(jsonString).asJsonObject

        return try {
            val points = json
                    .getAsJsonArray("routes")
                    .get(0).asJsonObject
                    .getAsJsonObject("overview_polyline")
                    .get("points").asString

            getDeadSpots(points, tripIds, minPerformance)
        } catch (ex: Exception) {
            "Exception: ${ex.stackTrace}"
        }
    }

    private fun getDeadSpots(encodedPolylineString: String, tripIds: List<Int>?, minPerformance: Float): String {
        val startTime = System.currentTimeMillis()
        val deadSpots = mutableListOf<String>()
        val segmentize = "ST_Segmentize(ST_LineFromEncodedPolyline('$encodedPolylineString', 5)::geography, $ROUTE_SEGMENT_LENGTH_METERS)"
        val fullSql = "SELECT ST_X(geom), ST_Y(geom) FROM ST_DumpPoints($segmentize::geometry)"
        var isDeadSpot = false
        var lastValidArea: ResultRow? = null

        transaction {
            addLogger(StdOutSqlLogger)

            exec(fullSql) { vertex ->
                while (vertex.next()) {
                    val longitude = vertex.getDouble(1)
                    val latitude = vertex.getDouble(2)

                    // Check if there is an area within the distance threshold for the current vertex on the
                    // segmented line.
                    val area = Areas.select(object : Op<Boolean>() {
                        override fun toSQL(queryBuilder: QueryBuilder): String {
                            val newGeography = "ST_SetSRID(ST_Point($longitude, $latitude), $WGS84_SRID)::geography"

                            // Note that the last parameter of ST_DWithin must be a literal argument (constant value).
                            // see https://stackoverflow.com/questions/40919364/st-dwithin-does-not-use-index-with-non-literal-argument
                            // Using a function like GREATEST(..) will prevent the correct index from being used
                            // thus having much worse performance.
                            return "ST_DWithin(${Areas.location.name}, $newGeography, $DEAD_SPOT_DISTANCE_THRESHOLD_METERS)"
                        }
                    }).limit(1).firstOrNull()

                    if (area == null) {
                        // No area found within distance treshold.
                        isDeadSpot = true
                        continue
                    }

                    // Check if this area is valid in terms of performance.
                    val areaId = area[Areas.id].value
                    val areaData = getAreaData(areaId, tripIds, null)[areaId]
                    val performance = areaData?.performance
                    if (performance ?: 0.0 < minPerformance) {
                        isDeadSpot = true
                        continue
                    }

                    if (isDeadSpot && lastValidArea != null) {
                        isDeadSpot = false
                        // Get distance between last valid area and this area.
                        val geog1 = "ST_SetSRID(ST_Point(${area[Areas.location].x}, ${area[Areas.location].y}), $WGS84_SRID)::geography"
                        val geog2 = "ST_SetSRID(ST_Point(${lastValidArea!![Areas.location].x}, ${lastValidArea!![Areas.location].y}), $WGS84_SRID)::geography"
                        val rawSql = "SELECT ST_Distance($geog1, $geog2)"

                        var distance: Double? = null
                        exec(rawSql) { r ->
                            if (r.next()) {
                                distance = r.getDouble(1)
                            }
                        }

                        deadSpots.add("Dead spot between ${lastValidArea!![Areas.id]} (${lastValidArea!![Areas.location].y}, ${lastValidArea!![Areas.location].x}), ${area[Areas.id]} (${area[Areas.location].y}, ${area[Areas.location].x}), length: ${distance?.roundToInt()} m")
                    }

                    lastValidArea = area
                }
            }
        }

        println("${::getDeadSpots.name}: ${System.currentTimeMillis() - startTime} ms")

        return deadSpots.joinToString("\n")
    }

    fun areasToJson(areaId: Int?, tripIds: List<Int>?, networkType: String?): String {
        val startTime = System.currentTimeMillis()
        val areasJsonArray = JsonArray()
        val data = getAreaData(areaId, tripIds, networkType)
        transaction {
            addLogger(StdOutSqlLogger)

            val areas = Areas.select { Areas.id inList(data.keys) }.associateBy {
                it[Areas.id].value
            }

            data.forEach {
                val jsonObject = JsonObject()
                val areaData = it.value
                val areaRow = areas[it.key] ?: return@forEach
                jsonObject.addProperty("id", it.key.toString())
                jsonObject.addProperty("lat", "%.4f".format(Locale.ENGLISH, areaRow[Areas.location].latitude))
                jsonObject.addProperty("lng", "%.4f".format(Locale.ENGLISH, areaRow[Areas.location].longitude))
                jsonObject.addProperty("acc", areaRow[Areas.accuracy].toString())
                jsonObject.addProperty("net", areaData.commonNetworkType)
                jsonObject.addProperty("perf", "%.4f".format(Locale.ENGLISH, areaData.performance))
                jsonObject.addProperty("rttq", "%.4f".format(Locale.ENGLISH, areaData.roundTripTimeQuality))
                jsonObject.addProperty("signal", "%.4f".format(Locale.ENGLISH, areaData.averageSignalStrength))
                jsonObject.addProperty("jitter", "%.4f".format(Locale.ENGLISH, areaData.jitterRatio))
                jsonObject.addProperty("loss", "%.4f".format(Locale.ENGLISH, areaData.packetLossRatio))

                areasJsonArray.add(jsonObject)
            }
        }

        val root = JsonObject().apply { add("areas", areasJsonArray) }

        println("${::areasToJson.name}: ${System.currentTimeMillis() - startTime} ms")

        return Gson().toJson(root)
    }

    fun measurementsToJson(areaId: Int?, tripIds: List<Int>?, networkType: String?): String {
        val measurementsJsonArray = JsonArray()

        transaction {
            addLogger(StdOutSqlLogger)

            val query = Measurements.selectAll()
            if (areaId != null) query.andWhere { Measurements.areaId eq areaId }
            if (networkType != null) query.andWhere { Measurements.networkType eq networkType }
            if (tripIds != null) addTripIdsToQuery(tripIds, query)

            query.forEach {
                val jsonObject = JsonObject()
                jsonObject.addProperty("areaId", it[Measurements.areaId].value)
                jsonObject.addProperty("packetId", it[Measurements.packetId])
                jsonObject.addProperty("tripId", it[Measurements.tripId])
                jsonObject.addProperty("speed", it[Measurements.speed])
                jsonObject.addProperty("bearing", it[Measurements.bearing])
                jsonObject.addProperty("commonNetworkType", it[Measurements.networkType])
                jsonObject.addProperty("signalStrength", it[Measurements.signalStrength])
                jsonObject.addProperty("roundTripTime", it[Measurements.roundTripTime])
                jsonObject.addProperty("jitter", it[Measurements.jitter])

                measurementsJsonArray.add(jsonObject)
            }
        }

        val root = JsonObject().apply { add("measurements", measurementsJsonArray) }

        return Gson().toJson(root)
    }

    private fun columnFromString(s: String): Column<*>? {
        return when (s.toLowerCase()) {
            "roundtriptime" -> Measurements.roundTripTime
            "signalstrength" -> Measurements.signalStrength
            "gsmasulevel" -> Measurements.gsmAsuLevel
            "lteasulevel" -> Measurements.lteAsuLevel
            "ipdv" -> Measurements.ipdv
            "jitter" -> Measurements.jitter

            else -> null
        }
    }

    private fun formatMetricName(s: String): String? {
        return when (s.toLowerCase()) {
            "roundtriptime" -> "Round-trip time (ms)"
            "signalstrength" -> "Signal strength"
            "gsmasulevel" -> "GSM ASU Level (0-31)"
            "lteasulevel" -> "LTE ASU Level (0-97)"
            "ipdv" -> "IPDV (ms)"
            "jitter" -> "Jitter (ms)"

            else -> null
        }
    }

    /** TODO: Shares a lot of code with [chartXy], refactor. */
    fun chartDistribution(metric: String, tripIds: List<Int>?, networkTypes: List<String>?, xMin: Int?, xMax: Int?, cMin: Int?): String {
        val column = columnFromString(metric) ?: return ""
        val xData = JsonArray().apply { add("x") }
        val yData = JsonArray().apply { add("y") }
        var mean = 0.0
        var median = 0.0

        transaction {
            addLogger(StdOutSqlLogger)

            val networkTypeWhere = if (networkTypes != null) {
                networkTypes.subList(1, networkTypes.size).fold("(${Measurements.networkType.name} = '${networkTypes[0]}'") { accumulator, next ->
                    "$accumulator OR ${Measurements.networkType.name} = '$next'"
                } + ")"
            } else {
                null
            }

            // val areaIdWhere = if (areaId != null) "${Measurements.areaId.name} = $areaId" else null
            val xMinWhere = if (xMin != null) "${column.name} >= $xMin" else null
            val xMaxWhere = if (xMax != null) "${column.name} <= $xMax" else null
            val having = if (cMin != null) "HAVING COUNT(*) > $cMin" else ""

            // Build the text (trip_id = tripIds[0] OR trip_id = tripIds[1] .. OR trip_id = tripIds[n])
            val tripIdWhere = if (tripIds != null) {
                tripIds.subList(1, tripIds.size).fold("(${Measurements.tripId.name} = ${tripIds[0]}") { accumulator, next ->
                    "$accumulator OR ${Measurements.tripId.name} = $next"
                } + ")"
            } else {
                null
            }

            // Combine all "wheres"
            val whereConcat = listOf(tripIdWhere, networkTypeWhere, xMinWhere, xMaxWhere).reduce { acc, s ->
                if (s == null) {
                    acc
                } else {
                    if (acc != null)"$acc AND $s" else "$s"
                }
            }

            val where = if (whereConcat != null) ("WHERE $whereConcat") else ""

            val sqlMean = "SELECT AVG(${column.name}) " +
                    "FROM Measurements $where"
            exec (sqlMean) {
                while (it.next()) {
                    mean = it.getDouble(1)
                }
            }

            val sqlMedian = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${column.name}) " +
                    "FROM Measurements $where"
            exec (sqlMedian) {
                while (it.next()) {
                    median = it.getDouble(1)
                }
            }

            val sql = "SELECT COUNT(*) AS C, ${column.name} " +
                    "FROM Measurements $where GROUP BY ${column.name} " + having

            exec (sql) {
                while (it.next()) {
                    val xVal = it.getDouble(2)
                    if (it.wasNull()) continue /** See [java.sql.ResultSet.wasNull] as .getDouble returns 0.0 if value is null. */
                    val yVal = it.getDouble(1)
                    if (it.wasNull()) continue

                    xData.add(xVal)
                    yData.add(yVal)
                }
            }
        }

        val root = JsonObject().apply {
            addProperty("title", "$metric (${networkTypes?.joinToString()}, ${tripIds?.joinToString()})")
            add("x", xData)
            add("y", yData)
            addProperty("xLabel", formatMetricName(metric))
            addProperty("yLabel", "count")
            addProperty("type", "bar")
            addProperty("mean", mean)
            addProperty("median", median)
        }

        return Gson().toJson(root)
    }

    /** TODO: Shares a lot of code with [chartDistribution], refactor. */
    fun chartXy(type: String, areaId: Int?, tripIds: List<Int>?, networkType: String?, x: String, y: String, xMin: Int?, xMax: Int?, cMin: Int?): String {
        val xColumn = columnFromString(x) ?: return ""
        val yColumn = columnFromString(y) ?: return ""
        val xData = JsonArray().apply { add("x") }
        val yData = JsonArray().apply { add("y") }
        val cData = JsonArray()

        transaction {
            addLogger(StdOutSqlLogger)

            val networkTypeWhere = if (networkType != null) "${Measurements.networkType.name} = '$networkType'" else null
            val areaIdWhere = if (areaId != null) "${Measurements.areaId.name} = $areaId" else null
            val xMinWhere = if (xMin != null) "${xColumn.name} >= $xMin" else null
            val xMaxWhere = if (xMax != null) "${xColumn.name} <= $xMax" else null
            val having = if (cMin != null) "HAVING COUNT(*) > $cMin" else ""

            // Build the text (trip_id = tripIds[0] OR trip_id = tripIds[1] .. OR trip_id = tripIds[n])
            val tripIdWhere = if (tripIds != null) {
                tripIds.subList(1, tripIds.size).fold("(${Measurements.tripId.name} = ${tripIds[0]}") { accumulator, next ->
                    "$accumulator OR ${Measurements.tripId.name} = $next"
                } + ")"
            } else {
                null
            }

            // Combine all "wheres"
            val whereConcat = listOf(tripIdWhere, networkTypeWhere, areaIdWhere, xMinWhere, xMaxWhere).reduce { acc, s ->
                if (s == null) {
                    acc
                } else {
                    if (acc != null)"$acc AND $s" else "$s"
                }
            }

            val where = if (whereConcat != null) ("WHERE $whereConcat") else ""

            val sql =
                    if (type == "median") {
                        "SELECT COUNT(*) AS C, ${xColumn.name}, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${yColumn.name}) " +
                                "FROM Measurements $where GROUP BY ${xColumn.name} " + having + " ORDER BY ${xColumn.name}"
                    } else {
                        // average
                        "SELECT COUNT(*) AS C, ${xColumn.name}, AVG(${yColumn.name}) " +
                                "FROM Measurements $where GROUP BY ${xColumn.name} " + having + " ORDER BY ${xColumn.name}"
                    }

            exec (sql) {
                while (it.next()) {
                    val xVal = it.getDouble(2)
                    if (it.wasNull()) continue /** See [java.sql.ResultSet.wasNull] as .getDouble returns 0.0 if value is null. */
                    val yVal = it.getDouble(3)
                    if (it.wasNull()) continue
                    val count = it.getInt(1)

                    xData.add(xVal)
                    yData.add(yVal)
                    cData.add(count)
                }
            }
        }

        val root = JsonObject().apply {
            addProperty("title", "$type($y) as a function of $x")
            add("x", xData)
            add("y", yData)
            add("c", cData)
            addProperty("xLabel", formatMetricName(x))
            addProperty("yLabel", formatMetricName(y))
            addProperty("type", "line")
        }

        return Gson().toJson(root)
    }

    fun chartOverTime(metric: String, tripId: Int, xMin: Int?, xMax: Int?): String {
        val column = columnFromString(metric) ?: return ""
        val xData = JsonArray().apply { add("x") }
        val yData = JsonArray().apply { add("y") }

        transaction {
            addLogger(StdOutSqlLogger)

            val minPacketId = Measurements
                    .slice(Measurements.packetId.min())
                    .select { Measurements.tripId eq tripId }
                    .firstOrNull()?.get(Measurements.packetId.min()) ?: 0
            val min = xMin ?: minPacketId
            val query = Measurements
                    .slice(column, Measurements.packetId)
                    .select {(Measurements.tripId eq tripId) and (Measurements.packetId greaterEq min) }
                    .orderBy(Measurements.packetId)

            if (xMax != null) query.limit(xMax - min)

            query.forEach {
                xData.add(it[Measurements.packetId])
                val result = it[column] ?: return@forEach
                yData.add(result as Number)
            }
        }

        val root = JsonObject().apply {
            addProperty("title", "$metric for trip $tripId")
            add("x", xData)
            add("y", yData)
            addProperty("xLabel", "Packet ID")
            addProperty("yLabel", formatMetricName(metric))
            addProperty("type", "line")
        }

        return Gson().toJson(root)
    }

    fun listTrips(): String {
        return transaction {
            addLogger(StdOutSqlLogger)

            val measurements = Measurements
                    .slice(Measurements.id.count(), Measurements.tripId)
                    .selectAll()
                    .groupBy(Measurements.tripId)
                    .orderBy(Measurements.tripId)
                    .toList()

            // Get the most common network type for each trip
            val tripNetworkTypes = measurements.map { r ->
                val item = Measurements
                        .slice(Measurements.networkType, Measurements.networkType.count())
                        .select { Measurements.tripId eq r[Measurements.tripId] }
                        .groupBy(Measurements.networkType)
                        .orderBy(Measurements.networkType.count(), false)
                        .limit(1)
                        .firstOrNull()

                r[Measurements.tripId]!! to Pair(item?.get(Measurements.networkType), item?.get(Measurements.networkType.count()))
            }.toMap()

            val totalCount = measurements.fold(0) { accumulator, next ->
                accumulator + next[Measurements.id.count()] }

            return@transaction measurements
                    .joinToString("\n") {
                        val tripId = it[Measurements.tripId]
                        val count = it[Measurements.id.count()]
                        val type =  tripNetworkTypes[tripId]
                        val pctOfTotal = type?.second?.let { t -> ((t.toDouble() / count) * 100).toInt() }

                        "$tripId\t$count\t$pctOfTotal% ${type?.first}" } + "\n\nTotal:\t$totalCount"
            }
        }
    }

/**
 * Had some issues with PostgreSQL and camelCase column names, so better to keep them all lowercase.
 */

object Measurements : IntIdTable() {
    val areaId = reference("area_id", Areas).index("measurements_area_id")
    val packetId = integer("packet_id")
    val tripId = integer("trip_id").nullable().index("measurements_trip_id")
    val speed = float("speed").nullable()
    val bearing = float("bearing").nullable()
    val networkType = text("network_type").nullable().index("measurements_network_type")
    val signalStrength = float("signal_strength").nullable()
    val roundTripTime = integer("round_trip_time").nullable()
    val serverReplyTime = long("server_reply_time").nullable()
    val jitter = integer("jitter").nullable()
    val ipdv = integer("ipdv").nullable()
    val gsmAsuLevel = integer("gsm_asu_level").nullable()
    val lteAsuLevel = integer("lte_asu_level").nullable()
}

object Areas : IdTable<Int>() {
    override val id: Column<EntityID<Int>> = integer("id").autoIncrement().primaryKey().entityId().index("areas_id")
    // Note that index on location must be created manually in the DBMS (since using gist and not btree) and
    // this is important to do for improving performance.
    val location = point("location")
    // https://developer.android.com/reference/android/location/Location#getAccuracy()
    val accuracy = float("accuracy")
}

// Not currently in use
object AreaPaths : Table() {
    val areaId = reference("area_id", Areas).index("areapaths_area_id")
    val previousAreaId = reference("previous_area_id", Areas).nullable().index("areapaths_previous_area_id")
    val nextAreaId = reference("next_area_id", Areas).nullable().index("areapaths_next_area_id")
    val bearing = float("bearing")
    val tripId = integer("trip_id").nullable().index("areapaths_trip_id")
}

fun Table.point(name: String, srid: Int = WGS84_SRID): Column<Point> = registerColumn(name, PointColumnType(srid))

private class PointColumnType(val srid: Int = WGS84_SRID) : ColumnType() {
    override fun sqlType() = "GEOGRAPHY(Point, $srid)"

    override fun valueFromDB(value: Any): Any {
        return if (value is PGobject)
            PGgeometry.geomFromString(value.value).getPoint(0)
        else
            value
    }

    override fun notNullValueToDB(value: Any): Any {
        if (value is Point) {
            if (value.srid == Point.UNKNOWN_SRID) value.srid = srid

            return PGobject().apply {
                this.type = "geography"
                this.value = PGgeometry(value).toString()
            }
        }

        return value
    }
}

val Point.latitude: Double
    get() = this.y

val Point.longitude: Double
    get() = this.x