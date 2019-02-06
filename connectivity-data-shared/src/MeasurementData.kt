// Used by client application(s) to send data in correct format to server.
data class MeasurementData(var packetId: Int,
                           var latitude: Double,
                           var longitude: Double,
                           var accuracy: Float,
                           var speed: Float,
                           var bearing: Float,
                           var roundTripTime: Int?,
                           var serverReplyTime: Long?,
                           var signalStrength: Float?, /* Only for backwards compatibility */
                           var networkType: String?,
                           var linkDownstreamKbps: Int?,
                           var linkUpstreamKbps: Int?,
                           var jitter: Float?, /* Only for backwards compatibility */
                           var gsmAsuLevel: Int?,
                           var lteAsuLevel: Int?) {
    companion object {
        private const val SEPARATOR_CHARACTER = ";"

        fun fromString(string: String) : MeasurementData {
            val data = string.split(SEPARATOR_CHARACTER)

            val packetId = parseParameter(data, 0)!!.toInt()
            val latitude = parseParameter(data, 1)!!.toDouble()
            val longitude = parseParameter(data, 2)!!.toDouble()
            val accuracy = parseParameter(data, 3)!!.toFloat()
            val speed = parseParameter(data, 4)!!.toFloat()
            val bearing = parseParameter(data, 5)!!.toFloat()
            val roundTripTime = parseParameter(data, 6)?.toInt()
            val serverReplyTime = parseParameter(data, 7)?.toLong()
            val signalStrength = parseParameter(data, 8)?.toFloat()
            val networkType = parseParameter(data, 9)
            val linkDownstreamKbps = parseParameter(data, 10)?.toInt()
            val linkUpstreamKbps = parseParameter(data, 11)?.toInt()
            val jitter = parseParameter(data, 12)?.toFloat()
            val gsmAsuLevel = parseParameter(data, 13)?.toInt()
            val lteAsuLevel = parseParameter(data, 14)?.toInt()

            return MeasurementData(packetId, latitude, longitude, accuracy, speed, bearing, roundTripTime, serverReplyTime, signalStrength, networkType, linkDownstreamKbps, linkUpstreamKbps, jitter, gsmAsuLevel, lteAsuLevel)
        }

        private fun parseParameter(data: List<String>, index: Int): String? {
            if (index >= data.size) return null
            val string = data[index]
            if (string == "null") {
                return null
            }

            return string
        }
    }

    override fun toString(): String {
        // Compiler/underlying method takes care of doing this efficiently with a StringBuilder.
        return arrayOf(
                packetId,
                latitude,
                longitude,
                accuracy,
                speed,
                bearing,
                roundTripTime,
                serverReplyTime,
                signalStrength,
                networkType,
                linkDownstreamKbps,
                linkUpstreamKbps,
                jitter,
                gsmAsuLevel,
                lteAsuLevel).joinToString(SEPARATOR_CHARACTER)
    }
}