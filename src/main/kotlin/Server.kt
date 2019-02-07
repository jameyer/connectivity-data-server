import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.http.content.files
import io.ktor.http.content.static
import io.ktor.response.respondFile
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import no.ntnu.jameyer.master.shared.MeasurementData
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import java.io.File
import java.io.ObjectInputStream
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.concurrent.thread
import kotlin.streams.toList

fun main(args: Array<String>) {
    Server()
}

/**
 * Main server application. Consists of TCP server, database and REST functionality (Ktor library).
 */
class Server {
    companion object {
        /** Listening port for incoming TCP connections. */
        const val TCP_PORT = 1234
        /** Port used by embedded server for REST API. */
        const val REST_API_PORT = 8080
    }

    private val logger = LoggerFactory.getLogger(Server::class.java)

    private val database = Database()

    init {
        // REST functionality
        thread {
            embeddedServer(Netty, port = REST_API_PORT) {
                routing {
                    get("/dead-spots") {
                        val latlng1 = call.request.queryParameters["origin"]?.split(",")
                        if (latlng1 == null || latlng1.size != 2) {
                            call.respondText("Bad input for origin lat, lng pair.")
                            return@get
                        }

                        val latlng2 = call.request.queryParameters["destination"]?.split(",")
                        if (latlng2 == null || latlng2.size != 2) {
                            call.respondText("Bad input for destination lat, lng pair.")
                            return@get
                        }

                        val lat1 = latlng1[0].toDouble()
                        val lon1 = latlng1[1].toDouble()
                        val lat2 = latlng2[0].toDouble()
                        val lon2 = latlng2[1].toDouble()
                        val tripIds = getTripIdsFromQueryParameter(call.request.queryParameters["tripId"])
                        val minPerformance = call.request.queryParameters["minPerformance"]?.toFloat() ?: 0.0f

                        call.respondText(database.findDeadSpotsOnRoute(lat1, lon1, lat2, lon2, tripIds, minPerformance))
                    }

                    get("/areas") {
                        val areaId = call.request.queryParameters["areaId"]?.toInt()
                        val networkType = call.request.queryParameters["networkType"]
                        val tripIds = getTripIdsFromQueryParameter(call.request.queryParameters["tripId"])

                        call.respondText(database.areasToJson(areaId, tripIds, networkType), ContentType.Application.Json)
                    }

                    get("/measurements") {
                        val areaId = call.request.queryParameters["areaId"]?.toInt()
                        val networkType = call.request.queryParameters["networkType"]
                        val tripIds = getTripIdsFromQueryParameter(call.request.queryParameters["tripId"])

                        call.respondText(database.measurementsToJson(areaId, tripIds, networkType), ContentType.Application.Json)
                    }

                    get("/correlations") {
                        val tripIds = getTripIdsFromQueryParameter(call.request.queryParameters["tripId"])
                        val networkType = call.request.queryParameters["networkType"]

                        call.respondText(database.getCorrelations(tripIds, networkType))
                    }

                    get("/chartData") {
                        val type = call.request.queryParameters["type"]?.toLowerCase() ?: "average"
                        val xMax = call.request.queryParameters["xMax"]?.toInt()
                        val xMin = call.request.queryParameters["xMin"]?.toInt()

                        when (type) {
                            "time" -> {
                                val tripId = call.request.queryParameters["tripId"]?.toInt() ?: return@get
                                val metric = call.request.queryParameters["metric"] ?: return@get

                                call.respondText(database.chartOverTime(metric, tripId, xMin, xMax), ContentType.Application.Json)
                            }
                            "distribution" -> {
                                val networkTypes = call.request.queryParameters["networkType"]?.split(",")
                                val tripIds = getTripIdsFromQueryParameter(call.request.queryParameters["tripId"])
                                val metric = call.request.queryParameters["metric"] ?: return@get
                                val cMin = call.request.queryParameters["cMin"]?.toInt()

                                call.respondText(database.chartDistribution(metric, tripIds, networkTypes, xMin, xMax, cMin), ContentType.Application.Json)
                            }
                            else -> {
                                val areaId = call.request.queryParameters["areaId"]?.toInt()
                                val tripIds = getTripIdsFromQueryParameter(call.request.queryParameters["tripId"])
                                val networkType = call.request.queryParameters["networkType"]
                                val x = call.request.queryParameters["x"] ?: return@get
                                val y = call.request.queryParameters["y"] ?: return@get

                                call.respondText(database.chartXy(type, areaId, tripIds, networkType, x, y, xMin, xMax), ContentType.Application.Json)
                            }
                        }
                    }

                    get("/trips") {
                        call.respondText(database.listTrips())
                    }

                    get("/map") {
                        call.respondFile(File("web/map.html"))
                    }

                    get("/chart") {
                        call.respondFile(File("web/chart.html"))
                    }

                    static("web") {
                        files("web")
                    }
                }
            }.start(wait = true)
        }

        // TCP server functionality
        val serverSocket = ServerSocket(TCP_PORT)
        while (true) {
            val socket = serverSocket.accept().apply {
                soTimeout = 5000
            }

            // Run each Socket on a separate thread
            thread {
                logger.info("Client connected: ${socket.inetAddress} (${DateTime()})")

                ClientHandler(socket, database)
            }
        }
    }

    private fun getTripIdsFromQueryParameter(value: String?): List<Int>? {
        if (value == null) {
            return null
        }

        val tripIds = mutableListOf<Int>()
        value.split(",").forEach {
            if (it.contains("-")) {
                val range = it.split("-")
                if (range.size != 2) {
                    return@forEach
                }

                val subList = (range[0].toInt() .. range[1].toInt()).toList()
                tripIds.addAll(subList)
            } else {
                tripIds.add(it.toInt())
            }
        }

        return tripIds
    }

    class ClientHandler(socket: Socket, database: Database) {
        private val logger = LoggerFactory.getLogger(ClientHandler::class.java)

        init {
            var connected = true
            val reader = ObjectInputStream(socket.inputStream)

            while (connected) {
                try {
                    val data = reader.readObject() as Array<String>
                    val measurements = data.map { MeasurementData.fromString(it) }

                    database.insertMeasurements(measurements)
                } catch (ex: Exception) {
                    logger.info("Client disconnected: ${socket.inetAddress} ($ex) (${DateTime()})")

                    connected = false
                }
            }

            socket.close()
        }
    }

    // Format for CSV files from Ergys & Peter:
    // val format = CsvMeasurementFormat(0, 4, 5, 9, 8, 7, 2, null, 1, 3, null)
    private data class CsvMeasurementFormat(val keyIndex: Int?,
                                            val latitudeIndex: Int,
                                            val longitudeIndex: Int,
                                            val accuracyIndex: Int,
                                            val speedIndex: Int,
                                            val bearingIndex: Int,
                                            val roundTripTimeIndex: Int?,
                                            val serverReplyTimeIndex: Int?,
                                            val networkTypeIndex: Int?,
                                            val gsmAsuLevelIndex: Int?,
                                            val lteAsuLevelIndex: Int?)

    private fun readCsvFilesFromFolder(folder: String, format: CsvMeasurementFormat) {
        val path = Paths.get(folder)
        val seenKeys = mutableSetOf<Long>()

        Files.walk(path).filter{ Files.isRegularFile(it) }.forEach { file ->
            val measurements = mutableListOf<MeasurementData>()

            Files.lines(file)
                    .filter { it != null && it.isNotEmpty() }
                    .toList()
                    .forEachIndexed { index, s ->
                        val values = s.split(";")

                        // Duplicate data was found in some CSV files. Therefore this workaround was implemented
                        // to skip data having the same key.
                        val key = format.keyIndex?.let { values[it].toLong() }
                        if (key != null) {
                            if (seenKeys.contains(key)) {
                                return@forEachIndexed
                            }

                            seenKeys.add(key)
                        }

                        val latitude = parseParameter(values, format.latitudeIndex)!!.toDouble()
                        val longitude = parseParameter(values, format.longitudeIndex)!!.toDouble()
                        val accuracy = parseParameter(values, format.accuracyIndex)!!.toFloat()
                        val speed = parseParameter(values, format.speedIndex)!!.toFloat()
                        val bearing = parseParameter(values, format.bearingIndex)!!.toFloat()
                        val roundTripTime = parseParameter(values, format.roundTripTimeIndex)?.toInt()
                        val serverReplyTime = parseParameter(values, format.serverReplyTimeIndex)?.toLong()
                        val networkType = parseParameter(values, format.networkTypeIndex)
                        val gsmAsuLevel = parseParameter(values, format.gsmAsuLevelIndex)?.toInt()
                        val lteAsuLevel = parseParameter(values, format.lteAsuLevelIndex)?.toInt()

                        measurements.add(MeasurementData(index,
                                latitude,
                                longitude,
                                accuracy,
                                speed,
                                bearing,
                                roundTripTime,
                                serverReplyTime,
                                null,
                                networkType,
                                null,
                                null,
                                null,
                                gsmAsuLevel,
                                lteAsuLevel))
                    }

            database.insertMeasurements(measurements)
        }
    }

    private fun parseParameter(data: List<String>, index: Int?): String? {
        if (index == null || index >= data.size) return null
        val string = data[index]
        if (string == "null") {
            return null
        }

        return string
    }
}