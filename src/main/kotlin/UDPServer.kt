import java.net.DatagramPacket
import java.net.DatagramSocket
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    UDPServer()
}

class UDPServer {
    companion object {
        private const val PORT = 1235

        /**
         * UDP data length. Important that sent and received packets have the same length.
         * This should be set to the same value in the client.
         */
        private const val DATAGRAM_DATA_LENGTH = 32
    }

    init {
        val socket = DatagramSocket(PORT)

        while (true) {
            try {
                val buffer = ByteArray(DATAGRAM_DATA_LENGTH)
                val packet = DatagramPacket(buffer, buffer.size)
                socket.receive(packet)

                thread {
                    // System.nanoTime() is monotonically increasing,
                    // but we don't need/want the nanosecond precision for our jitter measurements
                    val time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
                    val packetId = ByteBuffer.wrap(packet.data).int
                    val idBytes = ByteBuffer.allocate(4).putInt(packetId).array()
                    val timeBytes = ByteBuffer.allocate(8).putLong(time).array()
                    val bytes = ByteArray(DATAGRAM_DATA_LENGTH)
                    System.arraycopy(idBytes, 0, bytes, 0, 4)
                    System.arraycopy(timeBytes, 0, bytes, 4, 8)

                    socket.send(
                            DatagramPacket(bytes, bytes.size, packet.address, packet.port)
                    )

                    println("Replied to ${packet.address}:${packet.port} ($packetId, $time)")
                }
            } catch (ex: Exception) {
                println("Exception: $ex")
            }
        }

        socket.close()
    }
}