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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public final class ProcessUtils {
    private static final Logger LOGGER = LogManager.getLogger(ProcessUtils.class);
    private static final Logger PROCESS_LOGGER = LogManager.getLogger("process-output");
    private static final Duration PROCESS_DESTROY_TIMEOUT = Duration.ofSeconds(20);

    private ProcessUtils() {}

    public static Process exec(Path workingDir, Path executable, Map<String, String> environment, boolean inheritIO, String... args) {
        return exec(null, workingDir, executable, environment, inheritIO, args);
    }

    public static Process exec(
        String input,
        Path workingDir,
        Path executable,
        Map<String, String> environment,
        boolean inheritIO,
        String... args
    ) {
        Process process;

        if (Files.exists(executable) == false) {
            throw new IllegalArgumentException("Can't run executable: `" + executable + "` does not exist.");
        }

        ProcessBuilder processBuilder = new ProcessBuilder();
        List<String> command = new ArrayList<>();
        command.addAll(
            OS.conditional(
                c -> c.onWindows(() -> List.of("cmd", "/c", workingDir.relativize(executable).toString()))
                    .onUnix(() -> List.of(workingDir.relativize(executable).toString()))
            )
        );
        command.addAll(Arrays.asList(args));

        processBuilder.command(command);
        processBuilder.directory(workingDir.toFile());
        processBuilder.environment().clear();
        processBuilder.environment().putAll(environment);

        try {
            process = processBuilder.start();

            startLoggingThread(
                process.getInputStream(),
                inheritIO ? System.out::println : PROCESS_LOGGER::info,
                executable.getFileName().toString()
            );

            startLoggingThread(
                process.getErrorStream(),
                inheritIO ? System.err::println : PROCESS_LOGGER::error,
                executable.getFileName().toString()
            );

            if (input != null) {
                try (BufferedWriter writer = process.outputWriter()) {
                    writer.write(input);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error executing process: " + executable.getFileName(), e);
        }

        return process;
    }

    public static void stopHandle(ProcessHandle processHandle, boolean forcibly) {
        // No-op if the process has already exited by itself.
        if (processHandle.isAlive() == false) {
            return;
        }

        // Stop all children last - if the ML processes are killed before the ES JVM then
        // they'll be recorded as having failed and won't restart when the cluster restarts.
        // ES could actually be a child when there's some wrapper process like on Windows,
        // and in that case the ML processes will be grandchildren of the wrapper.
        List<ProcessHandle> children = processHandle.children().toList();
        try {
            LOGGER.info("Terminating Elasticsearch process {}: {}", forcibly ? " forcibly " : "gracefully", processHandle.info());

            if (forcibly) {
                processHandle.destroyForcibly();
            } else {
                processHandle.destroy();
                waitForProcessToExit(processHandle);
                if (processHandle.isAlive() == false) {
                    return;
                }
                LOGGER.info("Process did not terminate after {}, stopping it forcefully", PROCESS_DESTROY_TIMEOUT);
                processHandle.destroyForcibly();
            }

            waitForProcessToExit(processHandle);

            if (processHandle.isAlive()) {
                throw new RuntimeException("Failed to terminate terminate elasticsearch process.");
            }
        } finally {
            children.forEach(each -> stopHandle(each, forcibly));
        }
    }

    public static void waitForExit(ProcessHandle processHandle) {
        // No-op if the process has already exited by itself.
        if (processHandle.isAlive() == false) {
            return;
        }

        waitForProcessToExit(processHandle);
    }

    private static void waitForProcessToExit(ProcessHandle processHandle) {
        try {
            Retry.retryUntilTrue(PROCESS_DESTROY_TIMEOUT, Duration.ofSeconds(1), () -> {
                processHandle.destroy();
                return processHandle.isAlive() == false;
            });
        } catch (ExecutionException e) {
            LOGGER.info("Failure while waiting for process to exist", e);
        } catch (TimeoutException e) {
            LOGGER.info("Timed out waiting for process to exit", e);
        }
    }

    private static void startLoggingThread(InputStream is, Consumer<String> logAppender, String name) {
        new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logAppender.accept(line);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Error reading output from process.", e);
            }
        }, name).start();
    }
}
