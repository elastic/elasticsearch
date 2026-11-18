/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.run;

import org.elasticsearch.gradle.ReaperService;
import org.elasticsearch.gradle.internal.LoggingOutputStream;
import org.elasticsearch.gradle.testclusters.MockApmServer;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

/**
 * Runs the {@code :test:test-clusters-cli} cluster runner until it exits.
 * <p>
 * This is a worker action, rather than running in the task action, so that independent {@link RunTask}s (e.g. the two clusters
 * of {@code run-ccs}) execute concurrently. Every process the runner reports in its pid file is registered with the build's
 * {@link ReaperService}, so the cluster is killed even if the Gradle daemon dies before it could stop the runner.
 */
public abstract class RunClusterWorkAction implements WorkAction<RunClusterWorkAction.Parameters> {

    private static final Logger LOGGER = Logging.getLogger(RunTask.class);
    private static final long PID_FILE_POLL_INTERVAL_MILLIS = 500;

    public interface Parameters extends WorkParameters {
        Property<String> getTaskPath();

        Property<String> getClusterName();

        Property<String> getExecutable();

        ConfigurableFileCollection getClasspath();

        Property<String> getMainClass();

        ListProperty<String> getArguments();

        MapProperty<String, String> getSystemProperties();

        RegularFileProperty getPidFile();

        Property<Boolean> getApmServerEnabled();

        Property<String> getApmServerMetrics();

        Property<ReaperService> getReaper();
    }

    private final ExecOperations execOperations;

    @Inject
    public RunClusterWorkAction(ExecOperations execOperations) {
        this.execOperations = execOperations;
    }

    @Override
    public void execute() {
        Parameters params = getParameters();
        String clusterName = params.getClusterName().get();
        List<String> arguments = new ArrayList<>(params.getArguments().get());

        MockApmServer apmServer = null;
        if (params.getApmServerEnabled().getOrElse(false)) {
            apmServer = new MockApmServer(params.getApmServerMetrics().getOrNull());
            try {
                apmServer.start();
            } catch (IOException e) {
                throw new GradleException("Unable to start APM server: " + e.getMessage(), e);
            }
            apmSettings(apmServer.getGrpcPort()).forEach((k, v) -> {
                arguments.add("--setting");
                arguments.add(k + "=" + v);
            });
        }

        Path pidFile = params.getPidFile().get().getAsFile().toPath();
        PidRegistrar registrar = new PidRegistrar(params.getReaper().get(), params.getTaskPath().get(), pidFile);
        try {
            Files.deleteIfExists(pidFile);
            registrar.start();

            ExecResult result = execOperations.javaexec(spec -> {
                spec.setExecutable(params.getExecutable().get());
                spec.setClasspath(params.getClasspath());
                spec.getMainClass().set(params.getMainClass());
                spec.setArgs(arguments);
                spec.systemProperties(params.getSystemProperties().get());
                spec.setStandardOutput(lifecycleOutput());
                spec.setErrorOutput(lifecycleOutput());
                spec.setIgnoreExitValue(true);
            });
            if (result.getExitValue() != 0) {
                throw new GradleException(
                    "Elasticsearch cluster [" + clusterName + "] exited unexpectedly with exit code " + result.getExitValue()
                );
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            registrar.stop();
            if (apmServer != null) {
                apmServer.stop();
            }
        }
    }

    static Map<String, String> apmSettings(int grpcPort) {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("telemetry.metrics.enabled", "true");
        settings.put("telemetry.tracing.enabled", "true");
        settings.put("telemetry.export.endpoint", "http://127.0.0.1:" + grpcPort);
        // Sample everything so spans are actually emitted; the default is 0.001.
        settings.put("telemetry.tracing.sample_rate", "1.0");
        settings.put("telemetry.tracing.max_depth", "10");
        return settings;
    }

    private static OutputStream lifecycleOutput() {
        return new LoggingOutputStream() {
            @Override
            protected void logLine(String line) {
                LOGGER.lifecycle(line);
            }
        };
    }

    /**
     * Watches for the runner's pid file and registers every listed process with the reaper until stopped.
     */
    private static final class PidRegistrar {
        private final ReaperService reaper;
        private final String serviceIdPrefix;
        private final Path pidFile;
        private final Set<String> registered = ConcurrentHashMap.newKeySet();
        private final Thread thread;
        private volatile boolean stopped = false;

        PidRegistrar(ReaperService reaper, String taskPath, Path pidFile) {
            this.reaper = reaper;
            this.serviceIdPrefix = "run" + taskPath + ":";
            this.pidFile = pidFile;
            this.thread = new Thread(this::watch, "run-cluster-reaper-registrar" + taskPath);
            this.thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        private void watch() {
            while (stopped == false) {
                if (Files.exists(pidFile)) {
                    try {
                        for (String line : Files.readAllLines(pidFile)) {
                            int separator = line.lastIndexOf('=');
                            if (separator > 0) {
                                String serviceId = serviceIdPrefix + line.substring(0, separator);
                                if (registered.add(serviceId)) {
                                    reaper.registerPid(serviceId, Long.parseLong(line.substring(separator + 1).trim()));
                                }
                            }
                        }
                        return;
                    } catch (IOException | NumberFormatException e) {
                        LOGGER.warn("Unable to read cluster pid file {}", pidFile, e);
                        return;
                    }
                }
                try {
                    Thread.sleep(PID_FILE_POLL_INTERVAL_MILLIS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        void stop() {
            stopped = true;
            thread.interrupt();
            try {
                thread.join(PID_FILE_POLL_INTERVAL_MILLIS * 4);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            registered.forEach(reaper::unregister);
        }
    }
}
