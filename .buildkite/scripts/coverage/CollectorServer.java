import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import org.jacoco.cli.internal.core.data.ExecutionDataWriter;
import org.jacoco.cli.internal.core.runtime.RemoteControlReader;
import org.jacoco.cli.internal.core.runtime.RemoteControlWriter;

/**
 * Collects JaCoCo coverage from agents running in output=tcpclient mode.
 *
 * Why this exists: ES test-cluster nodes are separate processes that are torn down with
 * destroyForcibly, so a shutdown hook never runs and output=file,dumponexit never writes.
 * In tcpclient mode the agent dials out to us; we then request dumps while the node is
 * still alive, so nothing depends on how the process dies. Every node dials the same
 * port, which is also why output=tcpserver cannot work here - each node would try to bind
 * the same port.
 *
 * Usage: CollectorServer <port> <outfile>
 */
public final class CollectorServer {

    private static final AtomicInteger CONNECTIONS = new AtomicInteger();
    private static ExecutionDataWriter fileWriter;

    public static void main(String[] args) throws Exception {
        final int port = Integer.parseInt(args[0]);
        final OutputStream out = new BufferedOutputStream(new FileOutputStream(args[1]));
        fileWriter = new ExecutionDataWriter(out);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                synchronized (fileWriter) {
                    fileWriter.flush();
                }
                out.flush();
                out.close();
            } catch (Exception ignored) {
            }
            System.err.println("[collector] connections=" + CONNECTIONS.get());
        }));

        ServerSocket server = new ServerSocket(port, 64, InetAddress.getByName("127.0.0.1"));
        System.err.println("[collector] listening on 127.0.0.1:" + port + " -> " + args[1]);

        while (true) {
            final Socket socket = server.accept();
            CONNECTIONS.incrementAndGet();
            Thread t = new Thread(() -> handle(socket));
            t.setDaemon(true);
            t.start();
        }
    }

    private static void handle(Socket socket) {
        try {
            final RemoteControlWriter writer = new RemoteControlWriter(socket.getOutputStream());
            final RemoteControlReader reader = new RemoteControlReader(socket.getInputStream());
            reader.setSessionInfoVisitor(info -> {
                synchronized (fileWriter) {
                    fileWriter.visitSessionInfo(info);
                }
            });
            reader.setExecutionDataVisitor(data -> {
                synchronized (fileWriter) {
                    fileWriter.visitClassExecution(data);
                }
            });

            // Request a dump periodically so coverage is captured while the node lives,
            // rather than relying on it exiting cleanly.
            Thread poller = new Thread(() -> {
                try {
                    while (!socket.isClosed()) {
                        Thread.sleep(10_000L);
                        synchronized (writer) {
                            writer.visitDumpCommand(true, false);
                        }
                    }
                } catch (Exception ignored) {
                }
            });
            poller.setDaemon(true);
            poller.start();

            while (reader.read()) {
                // keep reading until the peer closes
            }
            // Final dump attempt before the socket goes away.
            try {
                synchronized (writer) {
                    writer.visitDumpCommand(true, false);
                }
                reader.read();
            } catch (Exception ignored) {
            }
            socket.close();
        } catch (Exception e) {
            System.err.println("[collector] connection ended: " + e);
        } finally {
            try {
                synchronized (fileWriter) {
                    fileWriter.flush();
                }
            } catch (Exception ignored) {
            }
        }
    }
}
