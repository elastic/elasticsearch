/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

    /**
     * The result of a captured tool process execution, containing the exit code and all lines
     * written to stderr by the process.
     */
    public record Result(int exitCode, List<String> stderr) {}

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

    /**
     * Executes a short-lived tool process and captures all stderr output for diagnostic purposes.
     * Standard output is logged asynchronously via the process logger. This method blocks until
     * the process exits and its entire stderr stream has been drained, so the returned
     * {@link Result#stderr()} list is guaranteed to be complete on return.
     *
     * @param input      optional text to write to the process's stdin, or {@code null}
     * @param workingDir the working directory for the process
     * @param executable the executable to run
     * @param environment the explicit environment for the process (parent environment is cleared)
     * @param args       additional command-line arguments
     * @return a {@link Result} with the process exit code and all captured stderr lines
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public static Result execAndCapture(String input, Path workingDir, Path executable, Map<String, String> environment, String... args)
        throws InterruptedException {
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

        Process process;
        List<String> stderrLines = new ArrayList<>();
        Thread stderrThread;
        try {
            process = processBuilder.start();

            // Drain stdout asynchronously — we do not need to capture it.
            startLoggingThread(process.getInputStream(), PROCESS_LOGGER::info, executable.getFileName().toString());

            // Drain stderr asynchronously while also collecting every line so that
            // callers can include the output in diagnostic error messages.
            stderrThread = startLoggingThread(process.getErrorStream(), line -> {
                PROCESS_LOGGER.error(line);
                stderrLines.add(line);
            }, executable.getFileName().toString());

            if (input != null) {
                try (BufferedWriter writer = process.outputWriter()) {
                    writer.write(input);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error executing process: " + executable.getFileName(), e);
        }

        int exit = process.waitFor();
        // Join the stderr thread so that all lines are guaranteed to be in stderrLines
        // before we return the Result to the caller.
        stderrThread.join();

        return new Result(exit, List.copyOf(stderrLines));
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
                LOGGER.info(
                    "Process did not terminate after {}, stopping it forcefully: {}",
                    PROCESS_DESTROY_TIMEOUT,
                    processHandle.info()
                );
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
            LOGGER.info("Failure while waiting for process to exit: {}", processHandle.info(), e);
        } catch (TimeoutException e) {
            LOGGER.info("Timed out waiting for process to exit: {}", processHandle.info(), e);
        }
    }

    private static Thread startLoggingThread(InputStream is, Consumer<String> logAppender, String name) {
        Thread thread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logAppender.accept(line);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Error reading output from process.", e);
            }
        }, name + "-log-forwarder");
        thread.start();
        return thread;
    }
}
