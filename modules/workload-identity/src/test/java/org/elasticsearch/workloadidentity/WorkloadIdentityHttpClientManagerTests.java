/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for the lifecycle guards on {@link WorkloadIdentityHttpClientManager#getHttpClient()}
 * and the idempotency of {@link WorkloadIdentityHttpClientManager#start()} /
 * {@link WorkloadIdentityHttpClientManager#close()}.
 *
 * <p>The {@code IOReactorException} branch in {@code createConnectionManager} is not exercised
 * here: {@link org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor} only throws on
 * platform-level reactor setup failures that cannot be triggered deterministically from a unit
 * test without bytecode-level interception. Coverage of that path is left to integration
 * environments where the reactor genuinely fails to initialize.
 */
public class WorkloadIdentityHttpClientManagerTests extends ESTestCase {

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcher;
    private WorkloadIdentitySslConfig sslConfig;
    private Settings settings;

    @Before
    public void setupCollaborators() {
        // No SSL material is configured: SslConfigurationLoader falls back to JDK defaults, which
        // is enough to build the SSLIOSessionStrategy captured by the manager. These tests never
        // actually open a socket, so trust/key material is irrelevant.
        this.settings = Settings.builder().put("path.home", createTempDir()).build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        this.threadPool = new TestThreadPool(getTestName());
        // ENABLED=false suppresses background polling; reload-on-file-change is not exercised by
        // these manager-level tests, but the watcher is a required collaborator on the SSL config.
        this.resourceWatcher = new ResourceWatcherService(
            Settings.builder().put(ResourceWatcherService.ENABLED.getKey(), false).build(),
            threadPool
        );
        this.sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);
    }

    @After
    public void shutdownThreadPool() {
        resourceWatcher.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testGetHttpClientBeforeStartThrows() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
            assertThat(ex.getMessage(), containsString("has not been started"));
        }
    }

    public void testGetHttpClientAfterCloseThrows() {
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        manager.start();
        // Sanity-check the happy path before tearing the manager down so the "closed" assertion
        // below is unambiguously about the close() transition rather than a never-started manager.
        assertNotNull(manager.getHttpClient());
        manager.close();

        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        assertThat(ex.getMessage(), containsString("is closed"));
    }

    public void testGetHttpClientReturnsStableInstanceAfterStart() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            manager.start();
            final CloseableHttpAsyncClient first = manager.getHttpClient();
            assertNotNull(first);
            // The manager is documented to return a stable reference for its lifetime (see the
            // class Javadoc: "The reference is stable for the lifetime of the manager").
            assertThat(manager.getHttpClient(), sameInstance(first));
        }
    }

    public void testStartIsIdempotent() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            manager.start();
            final CloseableHttpAsyncClient first = manager.getHttpClient();
            // A second start() must be a no-op. If it were not, Apache HC's client.start() would
            // throw IllegalStateException ("Request execution failed" / reactor already running)
            // and this call would surface that.
            manager.start();
            assertThat(manager.getHttpClient(), sameInstance(first));
        }
    }

    public void testCloseIsIdempotent() {
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        manager.start();
        manager.close();
        // Calling close() again must not throw and must not change the closed-state contract for
        // subsequent getHttpClient() callers.
        manager.close();
        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        assertThat(ex.getMessage(), containsString("is closed"));
    }

    public void testCloseBeforeStartIsAllowed() {
        // Constructing then immediately closing (without start()) must not throw: this is the
        // path taken when plugin bootstrapping fails partway through createComponents and the
        // manager has to be released without ever having been started.
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        manager.close();
        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        // "closed" takes precedence over "not started" because the closed-check runs first in
        // getHttpClient(); pin that ordering so future refactors don't silently flip it.
        assertThat(ex.getMessage(), containsString("is closed"));
    }

    /**
     * Core invariant of the reload model: {@link WorkloadIdentityHttpClientManager#reload()
     * reload()} swaps the underlying {@link SSLIOSessionStrategy} on the registered
     * {@link ReloadableSchemeIoSessionStrategy} <em>without</em> replacing the Apache HC client.
     * Existing dispatched requests therefore complete on the same client; only the next TLS
     * handshake observes the new strategy.
     */
    public void testReloadSwapsSchemeDelegateWithoutReplacingHttpClient() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            manager.start();
            final CloseableHttpAsyncClient clientBefore = manager.getHttpClient();
            final SSLIOSessionStrategy delegateBefore = manager.getSslStrategy().getDelegate();
            final int epochBefore = manager.getSslStrategy().currentEpoch();
            assertNotNull(clientBefore);
            assertNotNull(delegateBefore);

            manager.reload();

            assertThat(
                "reload must NOT replace the HC client — in-flight requests must be preserved",
                manager.getHttpClient(),
                sameInstance(clientBefore)
            );
            assertThat(
                "reload must publish a new SSLIOSessionStrategy to the scheme wrapper",
                manager.getSslStrategy().getDelegate(),
                not(sameInstance(delegateBefore))
            );
            // The rotation epoch must advance so RotationAwareReuseStrategy can identify
            // connections established under the previous delegate as stale and close them on
            // their next response (the load-shape-independent drain pathway).
            assertThat(
                "reload must advance the rotation epoch so stale connections can be drained",
                manager.getSslStrategy().currentEpoch(),
                greaterThan(epochBefore)
            );
        }
    }

    public void testReloadBeforeStartIsNoOp() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            final SSLIOSessionStrategy delegateAtConstruction = manager.getSslStrategy().getDelegate();
            final int epochAtConstruction = manager.getSslStrategy().currentEpoch();
            manager.reload();
            assertThat(
                "reload before start must leave the construction-time delegate in place",
                manager.getSslStrategy().getDelegate(),
                sameInstance(delegateAtConstruction)
            );
            assertThat(
                "reload before start must not advance the epoch",
                manager.getSslStrategy().currentEpoch(),
                equalTo(epochAtConstruction)
            );
            manager.start();
            assertNotNull(manager.getHttpClient());
        }
    }

    public void testReloadAfterCloseIsNoOp() {
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        manager.start();
        final SSLIOSessionStrategy delegateBeforeClose = manager.getSslStrategy().getDelegate();
        final int epochBeforeClose = manager.getSslStrategy().currentEpoch();
        manager.close();
        manager.reload();
        assertThat(
            "reload after close must not publish a new delegate",
            manager.getSslStrategy().getDelegate(),
            sameInstance(delegateBeforeClose)
        );
        assertThat(
            "reload after close must not advance the epoch (no stale connections to drain)",
            manager.getSslStrategy().currentEpoch(),
            equalTo(epochBeforeClose)
        );
        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        assertThat(ex.getMessage(), containsString("is closed"));
    }
}
