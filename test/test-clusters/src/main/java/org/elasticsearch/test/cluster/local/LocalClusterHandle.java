/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.cluster.ClusterHandle;
import org.elasticsearch.test.cluster.local.LocalClusterFactory.Node;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.ExceptionUtils;
import org.elasticsearch.test.cluster.util.Version;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalClusterHandle implements ClusterHandle {
    private static final Logger LOGGER = LogManager.getLogger(LocalClusterHandle.class);
    private static final Duration CLUSTER_UP_TIMEOUT = Duration.ofSeconds(30);

    public final ForkJoinPool executor = new ForkJoinPool(
        Math.max(Runtime.getRuntime().availableProcessors(), 4),
        new ForkJoinPool.ForkJoinWorkerThreadFactory() {
            private final AtomicLong counter = new AtomicLong(0);

            @Override
            public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                thread.setName(name + "-node-executor-" + counter.getAndIncrement());
                return thread;
            }
        },
        null,
        false
    );
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final String name;
    private final List<Node> nodes;

    public LocalClusterHandle(String name, List<Node> nodes) {
        this.name = name;
        this.nodes = nodes;
    }

    @Override
    public void start() {
        if (started.getAndSet(true) == false) {
            LOGGER.info("Starting Elasticsearch test cluster '{}'", name);
            execute(() -> nodes.parallelStream().forEach(n -> n.start(null)));
        }
        waitUntilReady();
    }

    @Override
    public void stop(boolean forcibly) {
        if (started.getAndSet(false)) {
            LOGGER.info("Stopping Elasticsearch test cluster '{}', forcibly: {}", name, forcibly);
            execute(() -> nodes.parallelStream().forEach(n -> n.stop(forcibly)));
        } else {
            // Make sure the process is stopped, otherwise wait
            execute(() -> nodes.parallelStream().forEach(Node::waitForExit));
        }
    }

    @Override
    public void restart(boolean forcibly) {
        stop(forcibly);
        start();
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    @Override
    public void close() {
        stop(false);

        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getHttpAddresses() {
        start();
        return execute(() -> nodes.parallelStream().map(Node::getHttpAddress).collect(Collectors.joining(",")));
    }

    @Override
    public String getHttpAddress(int index) {
        return getHttpAddresses().split(",")[index];
    }

    @Override
    public String getTransportEndpoints() {
        start();
        return execute(() -> nodes.parallelStream().map(Node::getTransportEndpoint).collect(Collectors.joining(",")));
    }

    @Override
    public String getTransportEndpoint(int index) {
        return getTransportEndpoints().split(",")[index];
    }

    @Override
    public String getRemoteClusterServerEndpoint() {
        start();
        return execute(() -> nodes.parallelStream().map(Node::getRemoteClusterServerEndpoint).collect(Collectors.joining(",")));
    }

    @Override
    public String getRemoteClusterServerEndpoint(int index) {
        return getRemoteClusterServerEndpoint().split(",")[index];
    }

    @Override
    public void upgradeNodeToVersion(int index, Version version) {
        Node node = nodes.get(index);
        node.stop(false);
        LOGGER.info("Upgrading node '{}' to version {}", node.getName(), version);
        node.start(version);
        waitUntilReady();
    }

    @Override
    public void upgradeToVersion(Version version) {
        stop(false);
        if (started.getAndSet(true) == false) {
            LOGGER.info("Upgrading Elasticsearch test cluster '{}' to version {}", name, version);
            execute(() -> nodes.parallelStream().forEach(n -> n.start(version)));
        }
        waitUntilReady();
    }

    public String getName(int index) {
        return nodes.get(index).getName();
    }

    public void stopNode(int index) {
        nodes.get(index).stop(false);
    }

    protected void waitUntilReady() {
        writeUnicastHostsFile();
        try {
            WaitForHttpResource wait = configureWaitForReady();
            wait.waitFor(CLUSTER_UP_TIMEOUT.toMillis());
        } catch (Exception e) {
            throw new RuntimeException("An error occurred while checking cluster '" + name + "' status.", e);
        }
    }

    private WaitForHttpResource configureWaitForReady() throws MalformedURLException {
        Node node = nodes.get(0);
        boolean securityEnabled = Boolean.parseBoolean(node.getSpec().getSetting("xpack.security.enabled", "true"));
        boolean sslEnabled = Boolean.parseBoolean(node.getSpec().getSetting("xpack.security.http.ssl.enabled", "false"));
        boolean securityAutoConfigured = isSecurityAutoConfigured(node);
        String scheme = securityEnabled && (sslEnabled || securityAutoConfigured) ? "https" : "http";
        WaitForHttpResource wait = new WaitForHttpResource(scheme, node.getHttpAddress(), nodes.size());
        User credentials = node.getSpec().getUsers().get(0);
        wait.setUsername(credentials.getUsername());
        wait.setPassword(credentials.getPassword());
        if (sslEnabled) {
            configureWaitSecurity(wait, node);
        } else if (securityAutoConfigured) {
            wait.setCertificateAuthorities(node.getWorkingDir().resolve("config/certs/http_ca.crt").toFile());
        }

        return wait;
    }

    private void configureWaitSecurity(WaitForHttpResource wait, Node node) {
        String caFile = node.getSpec().getSetting("xpack.security.http.ssl.certificate_authorities", null);
        if (caFile != null) {
            wait.setCertificateAuthorities(node.getWorkingDir().resolve("config").resolve(caFile).toFile());
        }
        String sslCertFile = node.getSpec().getSetting("xpack.security.http.ssl.certificate", null);
        if (sslCertFile != null) {
            wait.setCertificateAuthorities(node.getWorkingDir().resolve("config").resolve(sslCertFile).toFile());
        }
        String sslKeystoreFile = node.getSpec().getSetting("xpack.security.http.ssl.keystore.path", null);
        if (sslKeystoreFile != null && caFile == null) { // Can not set both trust stores and CA
            wait.setTrustStoreFile(node.getWorkingDir().resolve("config").resolve(sslKeystoreFile).toFile());
        }
        String keystorePassword = node.getSpec().getSetting("xpack.security.http.ssl.keystore.secure_password", null);
        if (keystorePassword != null) {
            wait.setTrustStorePassword(keystorePassword);
        }
    }

    private boolean isSecurityAutoConfigured(Node node) {
        Path configFile = node.getWorkingDir().resolve("config").resolve("elasticsearch.yml");
        try (Stream<String> lines = Files.lines(configFile)) {
            return lines.anyMatch(l -> l.contains("BEGIN SECURITY AUTO CONFIGURATION"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeUnicastHostsFile() {
        String transportUris = execute(() -> nodes.parallelStream().map(Node::getTransportEndpoint).collect(Collectors.joining("\n")));
        execute(() -> nodes.parallelStream().forEach(node -> {
            try {
                Path hostsFile = node.getWorkingDir().resolve("config").resolve("unicast_hosts.txt");
                if (Files.notExists(hostsFile)) {
                    Files.writeString(hostsFile, transportUris);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to write unicast_hosts for: " + node, e);
            }
        }));
    }

    private <T> T execute(Callable<T> task) {
        try {
            return executor.submit(task).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException("An error occurred orchestrating test cluster.", ExceptionUtils.findRootCause(e));
        }
    }

    private void execute(Runnable task) {
        execute(() -> {
            task.run();
            return true;
        });
    }
}
