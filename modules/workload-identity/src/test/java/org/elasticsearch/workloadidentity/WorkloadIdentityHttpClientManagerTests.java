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
 * Unit tests for the {@link WorkloadIdentityHttpClientManager} lifecycle state machine: the
 * legal {@code INIT → INIT_RELOADED → STARTED → CLOSED} transitions, the rejection of every
 * other source-state combination for {@code start()}, the idempotency of {@code close()}, and
 * the per-state error reported by {@code getHttpClient()}.
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
        // These tests construct the manager standalone (no listener wiring) and call
        // manager.reload() directly where a populated delegate is needed; sslConfig.start() here
        // just makes sslConfig.getStrategy() available to those reloads.
        this.sslConfig.start();
    }

    @After
    public void shutdownThreadPool() {
        resourceWatcher.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testGetHttpClientBeforeStartThrows() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
            assertThat(ex.getMessage(), containsString("[INIT]"));
        }
    }

    /**
     * Calling {@link WorkloadIdentityHttpClientManager#start()} before the initial SSL delegate
     * has been published must fail synchronously rather than deferring the failure to the first
     * TLS handshake on the IO reactor thread. The fix-up path (call {@code reload()}, then
     * {@code start()} again) must succeed.
     */
    public void testStartBeforeInitialDelegatePublishedFailsAndPermitsRetry() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::start);
            assertThat(ex.getMessage(), containsString("[INIT]"));

            manager.reload();
            manager.start();
            assertNotNull(manager.getHttpClient());
        }
    }

    public void testGetHttpClientAfterCloseThrows() {
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        manager.reload();
        manager.start();
        // Sanity-check the happy path before tearing the manager down so the "closed" assertion
        // below is unambiguously about the close() transition rather than a never-started manager.
        assertNotNull(manager.getHttpClient());
        manager.close();

        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        assertThat(ex.getMessage(), containsString("[CLOSED]"));
    }

    public void testGetHttpClientReturnsStableInstanceAfterStart() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            manager.reload();
            manager.start();
            final CloseableHttpAsyncClient first = manager.getHttpClient();
            assertNotNull(first);
            // The manager is documented to return a stable reference for its lifetime (see the
            // class Javadoc: "The reference is stable for the lifetime of the manager").
            assertThat(manager.getHttpClient(), sameInstance(first));
        }
    }

    /**
     * {@code start()} is not idempotent: a second call from {@code STARTED} surfaces the
     * programming error rather than silently no-op'ing. Only the one-shot
     * {@code INIT_RELOADED → STARTED} transition is legal.
     */
    public void testStartFromStartedThrows() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            manager.reload();
            manager.start();
            final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::start);
            assertThat(ex.getMessage(), containsString("[STARTED]"));
            // The error must not have torn anything down; the HC client is still serviceable.
            assertNotNull(manager.getHttpClient());
        }
    }

    public void testCloseIsIdempotent() {
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        manager.reload();
        manager.start();
        manager.close();
        // Calling close() again must not throw and must not change the closed-state contract for
        // subsequent getHttpClient() callers.
        manager.close();
        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        assertThat(ex.getMessage(), containsString("[CLOSED]"));
    }

    /**
     * {@code close()} from {@code INIT} or {@code INIT_RELOADED} must not throw and must not
     * flip the state: only the {@code STARTED → CLOSED} CAS in {@code close()} triggers cleanup.
     * Models the partial-init path where the never-started manager is dropped.
     */
    public void testCloseBeforeStartIsNoOp() {
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        manager.close();
        // State must still be INIT — close()'s CAS(STARTED, CLOSED) requires start() first.
        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        assertThat(ex.getMessage(), containsString("[INIT]"));
    }

    /**
     * {@code start()} from {@code CLOSED} must throw rather than silently flip the state or try
     * to restart a closed HC client.
     */
    public void testStartAfterCloseThrows() {
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        // Drive through INIT → INIT_RELOADED → STARTED so close() actually CASes to CLOSED.
        manager.reload();
        manager.start();
        manager.close();
        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::start);
        assertThat(ex.getMessage(), containsString("[CLOSED]"));
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
            // Stand-in for the plugin's initial-load reload so the delegate is non-null before
            // we exercise a rotation.
            manager.reload();
            manager.start();
            final CloseableHttpAsyncClient clientBefore = manager.getHttpClient();
            final SSLIOSessionStrategy delegateBefore = manager.getSslStrategy().getDelegate();
            final int epochBefore = manager.getSslStrategy().currentEpoch();
            assertNotNull(clientBefore);
            assertNotNull("initial reload must publish a non-null delegate", delegateBefore);

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

    /**
     * Pre-start {@link WorkloadIdentityHttpClientManager#reload()} must publish a delegate and
     * advance the rotation epoch: this is the edge that {@link WorkloadIdentitySslConfig#start()}
     * uses to populate the manager before {@code manager.start()}.
     */
    public void testReloadBeforeStartPublishesInitialDelegate() {
        try (WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool)) {
            assertNull("delegate must be unpublished at construction", manager.getSslStrategy().getDelegate());
            final int epochAtConstruction = manager.getSslStrategy().currentEpoch();

            manager.reload();

            assertNotNull(
                "pre-start reload must publish the initial delegate so it is in place by start()",
                manager.getSslStrategy().getDelegate()
            );
            assertThat(
                "pre-start reload must advance the rotation epoch",
                manager.getSslStrategy().currentEpoch(),
                greaterThan(epochAtConstruction)
            );

            manager.start();
            assertNotNull(manager.getHttpClient());
        }
    }

    public void testReloadAfterCloseIsNoOp() {
        final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(settings, sslConfig, threadPool);
        // Populate an initial delegate so the post-close assertion has a non-null baseline.
        manager.reload();
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
        assertThat(ex.getMessage(), containsString("[CLOSED]"));
    }
}
