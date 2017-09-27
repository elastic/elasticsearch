/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.fixture;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.singleton;

public class CliFixture {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            throw new IllegalArgumentException("usage: <logdir> <clijar>");
        }
        Path dir = Paths.get(args[0]);
        Path cliJar = Paths.get(args[1]);
        int port = 0;
        if (args.length > 2) {
            port = Integer.parseInt(args[2]);
        }
        if (false == Files.exists(cliJar)) {
            throw new IllegalArgumentException(cliJar + " doesn't exist");
        }
        if (false == Files.isRegularFile(cliJar)) {
            throw new IllegalArgumentException(cliJar + " is not a regular file");
        }
        String javaExec = "java";
        boolean isWindows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");

        if (isWindows) {
            javaExec += ".exe";
        }
        Path javaExecutable = Paths.get(System.getProperty("java.home"), "bin", javaExec);
        if (false == Files.exists(javaExecutable)) {
            throw new IllegalArgumentException(javaExec + " doesn't exist");
        }
        if (false == Files.isExecutable(javaExecutable)) {
            throw new IllegalArgumentException(javaExec + " isn't executable");
        }

        try (ServerSocket server = new ServerSocket()) {
            server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            // write pid file
            Path tmp = Files.createTempFile(dir, null, null);
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            Files.write(tmp, Collections.singleton(pid));
            Files.move(tmp, dir.resolve("pid"), StandardCopyOption.ATOMIC_MOVE);

            // write port file
            tmp = Files.createTempFile(dir, null, null);
            InetSocketAddress bound = (InetSocketAddress) server.getLocalSocketAddress();
            if (bound.getAddress() instanceof Inet6Address) {
                Files.write(tmp, singleton("[" + bound.getHostString() + "]:" + bound.getPort()));
            }
            else {
                Files.write(tmp, singleton(bound.getHostString() + ":" + bound.getPort()));
            }
            Files.move(tmp, dir.resolve("ports"), StandardCopyOption.ATOMIC_MOVE);

            boolean run = true;
            // Run forever until killed
            while (run) {
                try {
                    println("accepting on localhost:" + server.getLocalPort());
                    Socket s = server.accept();
                    String url = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8)).readLine();
                    if (url == null || url.isEmpty()) {
                        continue;
                    }
                    List<String> command = new ArrayList<>();
                    command.add(javaExecutable.toString());
                    //                    command.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000");
                    // Force a specific terminal type so we have consistent responses for testing.
                    command.add("-Dorg.jline.terminal.type=xterm-256color");
                    // Disable terminal types that won't work with stdin isn't actually a tty
                    command.add("-Dorg.jline.terminal.jna=false");
                    command.add("-Dorg.jline.terminal.jansi=false");
                    command.add("-Dorg.jline.terminal.exec=false");
                    command.add("-Dorg.jline.terminal.dumb=true");
                    command.add("-jar");
                    command.add(cliJar.toString());
                    command.add(url);
                    ProcessBuilder cliBuilder = new ProcessBuilder(command);
                    cliBuilder.redirectErrorStream(true);
                    Process process = cliBuilder.start();
                    println("started " + command);
                    new Thread(() -> {
                        int i;
                        try {
                            while ((i = process.getInputStream().read()) != -1) {
                                s.getOutputStream().write(i);
                                s.getOutputStream().flush();
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("failed to copy from process to socket", e);
                        } finally {
                            process.destroyForcibly();
                        }
                    }).start();
                    new Thread(() -> {
                        int i;
                        try {
                            while ((i = s.getInputStream().read()) != -1) {
                                process.getOutputStream().write(i);
                                process.getOutputStream().flush();
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("failed to copy from socket to process", e);
                        } finally {
                            process.destroyForcibly();
                        }
                    }).start();
                    process.waitFor();
                } catch (IOException e) {
                    printStackTrace("error at the top level, continuing", e);
                }
            }
        }
    }

    @SuppressForbidden(reason = "cli application")
    private static void println(String line) {
        System.out.println(line);
    }

    @SuppressForbidden(reason = "cli application")
    private static void printStackTrace(String reason, Throwable t) {
        System.err.println(reason);
        t.printStackTrace();
    }
}
