/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.cli;

import org.elasticsearch.test.cluster.local.DefaultLocalClusterFactory;
import org.elasticsearch.test.cluster.local.DefaultLocalClusterHandle;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.SnapshotDistributionResolver;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Starts a local Elasticsearch cluster using the test-clusters library and keeps it running until this process is terminated.
 * <p>
 * This exists so that build tooling (e.g. the {@code ./gradlew run} task), which cannot depend on the test-clusters library
 * directly, can launch clusters configured the same way as those used by integration tests. See {@link ClusterRunnerArgs} for the
 * supported options.
 * <p>
 * The process exits with a non-zero status if any node stops unexpectedly. If the parent process goes away the cluster is shut
 * down, so a cluster does not outlive the build that started it.
 */
public final class ClusterRunner {

    private static final String DEBUG_SERVER_SYSPROP = "tests.cluster.debug.enabled";
    private static final String DEBUG_CLI_SYSPROP = "tests.cluster.cli.debug.enabled";
    private static final String TASK_SYSPROP = "tests.task";
    private static final long POLL_INTERVAL_MILLIS = 1000;

    private final ClusterRunnerArgs args;
    private final PrintStream out;

    ClusterRunner(ClusterRunnerArgs args, PrintStream out) {
        this.args = args;
        this.out = out;
    }

    public static void main(String[] argv) {
        ClusterRunnerArgs args;
        try {
            args = ClusterRunnerArgs.parse(argv);
        } catch (IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println();
            System.err.println(ClusterRunnerArgs.usage());
            System.exit(2);
            return;
        }

        if (args.help()) {
            System.out.println(ClusterRunnerArgs.usage());
            return;
        }

        System.exit(new ClusterRunner(args, System.out).run());
    }

    int run() {
        configureSystemProperties();

        DefaultLocalClusterHandle handle = createHandle();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopQuietly(handle), "cluster-runner-shutdown"));

        try {
            handle.start();
        } catch (Exception e) {
            System.err.println("ERROR: failed to start cluster [" + args.name() + "]");
            e.printStackTrace(System.err);
            return 1;
        }

        if (args.pidFile() != null) {
            writePidFile(handle, args.pidFile());
        }
        printBanner(handle);

        return awaitTermination(handle);
    }

    private void configureSystemProperties() {
        // Some library error messages are derived from the test task path, which isn't set outside of Gradle test tasks
        if (System.getProperty(TASK_SYSPROP) == null) {
            System.setProperty(TASK_SYSPROP, ":run");
        }
        // Debug ports are allocated as nodes are created, so these must be configured before building the handle
        if (args.debugServer()) {
            System.setProperty(DEBUG_SERVER_SYSPROP, "true");
            DefaultLocalClusterHandle.NEXT_DEBUG_PORT.set(args.debugServerPort());
        }
        if (args.debugCli()) {
            System.setProperty(DEBUG_CLI_SYSPROP, "true");
            DefaultLocalClusterHandle.NEXT_CLI_DEBUG_PORT.set(args.debugCliPort());
        }
    }

    DefaultLocalClusterHandle createHandle() {
        ClusterRunnerSpecBuilder builder = new ClusterRunnerSpecBuilder();
        builder.name(args.name());
        builder.distribution(args.distribution());
        builder.nodes(args.nodes());

        args.settings().forEach((key, value) -> builder.setting(key, value));
        args.systemProperties().forEach((key, value) -> builder.systemProperty(key, value));
        args.jvmArgs().forEach(arg -> builder.jvmArg(arg));
        args.keystoreSettings().forEach((key, value) -> builder.keystore(key, value));
        args.keystoreFiles().forEach((key, file) -> builder.keystore(key, Resource.fromFile(file)));
        if (args.keystorePassword() != null) {
            builder.keystorePassword(args.keystorePassword());
        }
        for (User user : args.users()) {
            builder.user(user.getUsername(), user.getPassword(), user.getRole(), user.isOperator());
        }
        args.plugins().forEach(plugin -> builder.plugin(plugin));
        args.modules().forEach(module -> builder.module(module));
        args.configFiles().forEach((name, file) -> builder.configFile(name, Resource.fromFile(file)));

        for (int i = 0; i < args.nodes(); i++) {
            final int index = i;
            builder.node(index, node -> {
                node.setting("http.port", String.valueOf(args.httpPort() + index));
                node.setting("transport.port", String.valueOf(args.transportPort() + index));
                if (args.remoteClusterPort() != null) {
                    node.setting("remote_cluster.port", String.valueOf(args.remoteClusterPort() + index));
                }
                args.nodeSettings().getOrDefault(index, Map.of()).forEach((key, value) -> node.setting(key, value));
                if (args.dataDir() != null) {
                    Path dataPath = args.nodes() == 1 ? args.dataDir() : args.dataDir().resolve(args.nodeName(index));
                    node.setting("path.data", dataPath.toAbsolutePath().toString());
                }
            });
        }

        DefaultLocalClusterFactory factory = new DefaultLocalClusterFactory(
            new LocalDistributionResolver(new SnapshotDistributionResolver(new ReleasedDistributionResolver())),
            args.workingDir() == null ? null : args.workingDir().toAbsolutePath(),
            args.preserveData()
        );
        return builder.createHandle(factory);
    }

    /**
     * Writes the runner's own pid followed by each node's pid, one {@code <name>=<pid>} entry per line. The file is written
     * atomically so readers never observe a partial file.
     */
    private void writePidFile(DefaultLocalClusterHandle handle, Path pidFile) {
        List<String> lines = new ArrayList<>();
        lines.add("runner=" + ProcessHandle.current().pid());
        for (int i = 0; i < handle.getNumNodes(); i++) {
            lines.add(handle.getName(i) + "=" + handle.getPid(i));
        }
        try {
            Path absolute = pidFile.toAbsolutePath();
            Files.createDirectories(absolute.getParent());
            Path temp = Files.createTempFile(absolute.getParent(), absolute.getFileName().toString(), ".tmp");
            Files.write(temp, lines);
            Files.move(temp, absolute, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write pid file " + pidFile, e);
        }
    }

    private void printBanner(DefaultLocalClusterHandle handle) {
        boolean https = Boolean.parseBoolean(args.settings().getOrDefault("xpack.security.http.ssl.enabled", "false"));
        String scheme = https ? "https://" : "http://";
        List<String> httpAddresses = new ArrayList<>();
        for (int i = 0; i < handle.getNumNodes(); i++) {
            httpAddresses.add(scheme + handle.getHttpAddress(i));
        }

        out.println("=".repeat(80));
        out.println("Cluster [" + args.name() + "] is running with " + handle.getNumNodes() + " node(s)");
        out.println("  HTTP endpoints:           " + String.join(", ", httpAddresses));
        out.println("  Transport endpoints:      " + handle.getTransportEndpoints());
        if (args.remoteClusterPort() != null) {
            out.println("  Remote cluster endpoints: " + handle.getRemoteClusterServerEndpoints());
        }
        boolean securityEnabled = args.distribution() == DistributionType.DEFAULT
            && "false".equalsIgnoreCase(args.settings().get("xpack.security.enabled")) == false;
        if (securityEnabled) {
            // The test-clusters library creates a default user when none are configured
            User user = args.users().isEmpty() ? User.DEFAULT_USER : args.users().get(0);
            out.println("  Credentials:              " + user.getUsername() + ":" + user.getPassword());
        }
        if (args.workingDir() != null) {
            out.println("  Working directory:        " + args.workingDir().toAbsolutePath());
        }
        out.println("=".repeat(80));
    }

    private int awaitTermination(DefaultLocalClusterHandle handle) {
        Optional<ProcessHandle> parent = ProcessHandle.current().parent();
        while (true) {
            for (int i = 0; i < handle.getNumNodes(); i++) {
                if (isAlive(handle.getPid(i)) == false) {
                    System.err.println("ERROR: node [" + handle.getName(i) + "] of cluster [" + args.name() + "] exited unexpectedly");
                    return 1;
                }
            }
            if (parent.isPresent() && parent.get().isAlive() == false) {
                out.println("Parent process exited, shutting down cluster [" + args.name() + "]");
                return 0;
            }
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return 0;
            }
        }
    }

    private static boolean isAlive(long pid) {
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    private void stopQuietly(DefaultLocalClusterHandle handle) {
        try {
            handle.stop(false);
        } catch (Exception e) {
            System.err.println("WARNING: failed to cleanly stop cluster [" + args.name() + "]: " + e.getMessage());
        }
    }
}
