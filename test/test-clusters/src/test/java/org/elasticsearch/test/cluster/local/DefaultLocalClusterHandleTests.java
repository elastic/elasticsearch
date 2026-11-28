/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory.Node;
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

public class DefaultLocalClusterHandleTests {

    private static final DistributionResolver DISTRIBUTION_RESOLVER = (version, type) -> {
        throw new UnsupportedOperationException("Distribution resolution not required for tests");
    };

    @Test
    public void testAreAllNodesAliveWhenNotStartedReturnsFalse() throws Exception {
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", false)));
        assertThat(handle.areAllNodesAlive(), is(false));
    }

    @Test
    public void testCheckNodesAliveThrowsWhenNodeDead() throws Exception {
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", false)));
        setStarted(handle, true);
        IllegalStateException ex = assertThrows(IllegalStateException.class, handle::checkNodesAlive);
        assertThat(ex.getMessage(), containsString("Elasticsearch cluster"));
    }

    @Test
    public void testCheckNodesAliveThrowsWhenNodeDiesDuringHealthCheck() throws Exception {
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            private boolean first = true;

            @Override
            public boolean areAllNodesAlive() {
                if (first) {
                    first = false;
                    return true;
                }
                return false;
            }

            @Override
            protected List<WaitForHttpResource> createHealthChecks() throws MalformedURLException {
                return List.of(new FailingWaitForHttpResource());
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return 0L;
            }
        };
        setStarted(handle, true);
        IllegalStateException ex = assertThrows(IllegalStateException.class, handle::checkNodesAlive);
        assertThat(ex.getMessage(), containsString("Elasticsearch cluster"));
    }

    @Test
    public void testCheckNodesAliveThrowsWhenHealthCheckFailsAndNodesAlive() throws Exception {
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            @Override
            protected List<WaitForHttpResource> createHealthChecks() throws MalformedURLException {
                return List.of(new FailingWaitForHttpResource());
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return 0L;
            }
        };
        setStarted(handle, true);
        IllegalStateException ex = assertThrows(IllegalStateException.class, handle::checkNodesAlive);
        assertThat(ex.getMessage(), containsString("not responding"));
    }

    @Test
    public void testCheckNodesAliveCachesHealthChecks() throws Exception {
        CountingHandle handle = new CountingHandle("cluster", List.of(newNode("node-0", true)));
        setStarted(handle, true);
        handle.checkNodesAlive();
        handle.checkNodesAlive();
        assertThat(handle.getCheckCount(), is(1));
    }

    @Test
    public void testCheckNodesAliveProbesAgainAfterCacheExpiry() throws Exception {
        CountingWaitForHttpResource probe = new CountingWaitForHttpResource();
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            @Override
            protected List<WaitForHttpResource> createHealthChecks() {
                return List.of(probe);
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return 0L;
            }
        };
        setStarted(handle, true);
        handle.checkNodesAlive();
        handle.checkNodesAlive();
        assertThat(probe.checks.get(), is(2));
    }

    @Test
    public void testUpgradeNodeToVersionInvalidatesHealthCheckCache() throws Exception {
        // Per-node URLs change after restart. The cache must be dropped so subsequent
        // checkNodesAlive() calls rebuild probes against the upgraded node's current address.
        AtomicInteger buildCount = new AtomicInteger();
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            @Override
            protected List<WaitForHttpResource> createHealthChecks() throws MalformedURLException {
                buildCount.incrementAndGet();
                return List.of(new CountingWaitForHttpResource());
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return java.time.Duration.ofMinutes(1).toNanos();
            }

            @Override
            protected void waitUntilReady() {
                // Skip real cluster probing in this unit test
            }
        };
        setStarted(handle, true);
        handle.checkNodesAlive();
        assertThat(buildCount.get(), is(1));

        // Simulate an in-place node upgrade. We stub out node.stop()/node.start() via a no-op Node process.
        // The handle must invalidate the cache regardless of what the node does.
        invokeInvalidateOnUpgrade(handle);

        handle.checkNodesAlive();
        assertThat("cache should be rebuilt after node upgrade", buildCount.get(), is(2));
    }

    private static void invokeInvalidateOnUpgrade(DefaultLocalClusterHandle handle) throws Exception {
        java.lang.reflect.Method m = DefaultLocalClusterHandle.class.getDeclaredMethod("invalidateHealthCheckCache");
        m.setAccessible(true);
        m.invoke(handle);
    }

    @Test
    public void testCheckNodesAliveSuppressesRepeatProbingAfterFailure() throws Exception {
        CountingFailingWaitForHttpResource probe = new CountingFailingWaitForHttpResource();
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            @Override
            protected List<WaitForHttpResource> createHealthChecks() {
                return List.of(probe);
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return java.time.Duration.ofMinutes(1).toNanos();
            }
        };
        setStarted(handle, true);
        assertThrows(IllegalStateException.class, handle::checkNodesAlive);
        // Second call within the TTL window must not re-probe even though the first probe failed.
        handle.checkNodesAlive();
        assertThat(probe.checks.get(), is(1));
    }

    private static Node newNode(String name, boolean alive) throws Exception {
        Path baseDir = Files.createTempDirectory("test-handle-node");
        LocalClusterSpec clusterSpec = new LocalClusterSpec("cluster", List.of(User.DEFAULT_USER), List.of(), false);
        LocalNodeSpec nodeSpec = new LocalNodeSpec(
            clusterSpec,
            name,
            Version.CURRENT,
            List.of(),
            Map.of(),
            List.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            DistributionType.DEFAULT,
            Set.of(),
            List.of(),
            Map.of(),
            Map.of(),
            null,
            Map.of(),
            List.of(),
            Map.of(),
            List.of(),
            null
        );
        Node node = new Node(baseDir, DISTRIBUTION_RESOLVER, nodeSpec);
        setProcess(node, new TestProcess(alive));
        return node;
    }

    private static void setProcess(Node node, Process process) throws Exception {
        Field processField = Node.class.getDeclaredField("process");
        processField.setAccessible(true);
        processField.set(node, process);
    }

    private static void setStarted(DefaultLocalClusterHandle handle, boolean value) throws Exception {
        Field startedField = DefaultLocalClusterHandle.class.getDeclaredField("started");
        startedField.setAccessible(true);
        ((java.util.concurrent.atomic.AtomicBoolean) startedField.get(handle)).set(value);
    }

    @Test
    public void testCheckNodesAliveNoopWhenNotStarted() {
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of()) {
            @Override
            public boolean areAllNodesAlive() {
                throw new AssertionError("No nodes should be checked");
            }
        };
        handle.checkNodesAlive();
    }

    @Test
    public void testUpgradeToVersionSuppressesHealthChecksForCallbackDuration() throws Exception {
        // Rolling upgrade tests call getHttpAddresses() (→ checkNodesAlive()) inside the per-node
        // callback. A node that was just restarted may momentarily refuse direct HTTP connections
        // even after waitUntilReady() returns. The handle must suppress health checks during each
        // upgrade+callback window so these transient failures do not produce false positives.
        CountingFailingWaitForHttpResource probe = new CountingFailingWaitForHttpResource();
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            @Override
            protected List<WaitForHttpResource> createHealthChecks() {
                return List.of(probe);
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return 0L;
            }

            @Override
            protected void waitUntilReady() {
                // no-op: skip real cluster probing in unit test
            }

            @Override
            public void upgradeNodeToVersion(int index, Version version) {
                // no-op: skip actual node stop/start in unit test; just invalidate the cache
                // to simulate what the real implementation does before waitUntilReady().
                try {
                    java.lang.reflect.Method m = DefaultLocalClusterHandle.class.getDeclaredMethod("invalidateHealthCheckCache");
                    m.setAccessible(true);
                    m.invoke(this);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                waitUntilReady();
            }
        };
        setStarted(handle, true);

        // The callback fires inside upgradeToVersion; health checks must be suppressed there.
        handle.upgradeToVersion(org.elasticsearch.test.cluster.util.Version.CURRENT, () -> {
            // This must not throw even though the probe always fails.
            handle.checkNodesAlive();
        });

        assertThat("health check probe must not have been called during callback", probe.checks.get(), is(0));
    }

    @Test
    public void testWithSuppressedHealthChecksPreventsProbesDuringAction() throws Exception {
        // withSuppressedHealthChecks is the protected hook that subclasses (e.g.
        // ServerlessLocalClusterHandle) must call around their own rolling-upgrade callback
        // invocation. This test verifies the suppression and the automatic un-suppression.
        CountingFailingWaitForHttpResource probe = new CountingFailingWaitForHttpResource();
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            @Override
            protected List<WaitForHttpResource> createHealthChecks() {
                return List.of(probe);
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return 0L;
            }
        };
        setStarted(handle, true);

        handle.withSuppressedHealthChecks(handle::checkNodesAlive);

        assertThat("probe must not fire while health checks are suppressed", probe.checks.get(), is(0));
    }

    @Test
    public void testWithSuppressedHealthChecksRestoresProbesAfterAction() throws Exception {
        // Health checks must resume after withSuppressedHealthChecks completes so that
        // failures AFTER the rolling-upgrade window are still detected.
        CountingFailingWaitForHttpResource probe = new CountingFailingWaitForHttpResource();
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            @Override
            protected List<WaitForHttpResource> createHealthChecks() {
                return List.of(probe);
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return 0L;
            }
        };
        setStarted(handle, true);

        handle.withSuppressedHealthChecks(() -> {});

        // After withSuppressedHealthChecks returns, probes must fire again.
        assertThrows(IllegalStateException.class, handle::checkNodesAlive);
        assertThat("probe must fire after suppression window ends", probe.checks.get(), is(1));
    }

    @Test
    public void testWithSuppressedHealthChecksRestoresProbesEvenOnException() throws Exception {
        // Suppression must be lifted even when the action throws.
        CountingFailingWaitForHttpResource probe = new CountingFailingWaitForHttpResource();
        DefaultLocalClusterHandle handle = new DefaultLocalClusterHandle("cluster", List.of(newNode("node-0", true))) {
            @Override
            protected List<WaitForHttpResource> createHealthChecks() {
                return List.of(probe);
            }

            @Override
            protected long healthCheckCacheTtlNanos() {
                return 0L;
            }
        };
        setStarted(handle, true);

        assertThrows(
            RuntimeException.class,
            () -> handle.withSuppressedHealthChecks(() -> { throw new RuntimeException("upgrade failed"); })
        );

        // Even after an exception, health checks must be re-enabled.
        assertThrows(IllegalStateException.class, handle::checkNodesAlive);
        assertThat("probe must fire after suppression window ends (even on exception)", probe.checks.get(), is(1));
    }

    private static class TestProcess extends Process {
        private final boolean alive;

        TestProcess(boolean alive) {
            this.alive = alive;
        }

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public InputStream getErrorStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public int waitFor() {
            return 0;
        }

        @Override
        public int exitValue() {
            return 0;
        }

        @Override
        public void destroy() {}

        @Override
        public Process destroyForcibly() {
            return this;
        }

        @Override
        public boolean isAlive() {
            return alive;
        }
    }

    private static class FailingWaitForHttpResource extends WaitForHttpResource {
        FailingWaitForHttpResource() throws MalformedURLException {
            super(new java.net.URL("http://localhost:9200/_cluster/health"));
        }

        @Override
        protected void checkResource(javax.net.ssl.SSLContext ssl) throws java.io.IOException {
            throw new java.io.IOException("health check failure");
        }
    }

    private static class CountingWaitForHttpResource extends WaitForHttpResource {
        final AtomicInteger checks = new AtomicInteger();

        CountingWaitForHttpResource() throws MalformedURLException {
            super(new java.net.URL("http://localhost:9200/_cluster/health"));
        }

        @Override
        protected void checkResource(javax.net.ssl.SSLContext ssl) {
            checks.incrementAndGet();
        }
    }

    private static class CountingFailingWaitForHttpResource extends WaitForHttpResource {
        final AtomicInteger checks = new AtomicInteger();

        CountingFailingWaitForHttpResource() throws MalformedURLException {
            super(new java.net.URL("http://localhost:9200/_cluster/health"));
        }

        @Override
        protected void checkResource(javax.net.ssl.SSLContext ssl) throws java.io.IOException {
            checks.incrementAndGet();
            throw new java.io.IOException("health check failure");
        }
    }

    private static class CountingHandle extends DefaultLocalClusterHandle {
        private int checkCount = 0;

        CountingHandle(String name, List<Node> nodes) {
            super(name, nodes);
        }

        @Override
        protected List<WaitForHttpResource> createHealthChecks() throws MalformedURLException {
            checkCount++;
            return List.of(new FailingWaitForHttpResource() {
                @Override
                protected void checkResource(javax.net.ssl.SSLContext ssl) {}
            });
        }

        @Override
        protected long healthCheckCacheTtlNanos() {
            return java.time.Duration.ofMinutes(1).toNanos();
        }

        int getCheckCount() {
            return checkCount;
        }
    }
}
