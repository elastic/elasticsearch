/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessReaper {
    private static final String REAPER_CLASS = "org/elasticsearch/gradle/reaper/Reaper.class";
    private static final Pattern REAPER_JAR_PATH_PATTERN = Pattern.compile("file:(.*)!/" + REAPER_CLASS);
    private static final Logger LOGGER = LogManager.getLogger(ProcessReaper.class);
    private static final ProcessReaper INSTANCE = new ProcessReaper();
    private final Path reaperDir;

    private volatile Process process;

    private ProcessReaper() {
        try {
            this.reaperDir = Files.createTempDirectory("reaper-");
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create process reaper working directory.", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "process-reaper-shutdown"));
    }

    public static ProcessReaper instance() {
        return INSTANCE;
    }

    /**
     * Register a pid that will be killed by the reaper.
     */
    public void registerPid(String serviceId, long pid) {
        String[] killPidCommand = OS.conditional(
            c -> c.onWindows(() -> new String[] { "Taskkill", "/F", "/PID", String.valueOf(pid) })
                .onUnix(() -> new String[] { "kill", "-9", String.valueOf(pid) })
        );
        registerCommand(serviceId, killPidCommand);
    }

    public void unregister(String serviceId) {
        try {
            Files.deleteIfExists(getCmdFile(serviceId));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void registerCommand(String serviceId, String... command) {
        ensureReaperStarted();

        try {
            Files.writeString(getCmdFile(serviceId), String.join(" ", command));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getCmdFile(String serviceId) {
        return reaperDir.resolve(serviceId.replaceAll("[^a-zA-Z0-9]", "-") + ".cmd");
    }

    void shutdown() {
        if (process != null) {
            ensureReaperAlive();
            try {
                process.getOutputStream().close();
                LOGGER.info("Waiting for reaper to exit normally");
                if (process.waitFor() != 0) {
                    throw new RuntimeException("Reaper process failed. Check log at " + reaperDir.resolve("reaper.log") + " for details");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

    private synchronized void ensureReaperStarted() {
        if (process == null) {
            try {
                Path jarPath = locateReaperJar();

                // ensure the input directory exists
                Files.createDirectories(reaperDir);
                // start the reaper
                ProcessBuilder builder = new ProcessBuilder(
                    findJavaHome().resolve("bin")
                        .resolve(OS.<String>conditional(c -> c.onWindows(() -> "java.exe").onUnix(() -> "java")))
                        .toString(), // same jvm as gradle
                    "-Xms4m",
                    "-Xmx16m", // no need for a big heap, just need to read some files and execute
                    "-jar",
                    jarPath.toString(),
                    reaperDir.toString()
                );
                LOGGER.info("Launching reaper: " + String.join(" ", builder.command()));
                // be explicit for stdin, we use closing of the pipe to signal shutdown to the reaper
                builder.redirectInput(ProcessBuilder.Redirect.PIPE);
                File logFile = reaperDir.resolve("reaper.log").toFile();
                builder.redirectOutput(logFile);
                builder.redirectErrorStream();
                process = builder.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            ensureReaperAlive();
        }
    }

    private Path locateReaperJar() {
        URL main = this.getClass().getClassLoader().getResource(REAPER_CLASS);
        if (main != null) {
            String mainPath = main.getFile();
            Matcher matcher = REAPER_JAR_PATH_PATTERN.matcher(mainPath);

            if (matcher.matches()) {
                String path = matcher.group(1);
                return Path.of(OS.<String>conditional(c -> c.onWindows(() -> path.substring(1)).onUnix(() -> path)));
            }
        }

        throw new RuntimeException("Unable to locate " + REAPER_CLASS + " on classpath.");
    }

    private void ensureReaperAlive() {
        if (process.isAlive() == false) {
            throw new IllegalStateException("Reaper process died unexpectedly! Check the log at " + reaperDir.resolve("reaper.log"));
        }
    }

    private static Path findJavaHome() {
        Path javaBase = Path.of(System.getProperty("java.home"));
        return javaBase.endsWith("jre") && Files.exists(javaBase.getParent().resolve("bin/java")) ? javaBase.getParent() : javaBase;
    }
}
