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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
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
    private WorkloadIdentitySslConfig sslConfig;
    private Settings settings;

    @Before
    public void setupCollaborators() {
        // No SSL material is configured: SslConfigurationLoader falls back to JDK defaults, which
        // is enough to build the SSLIOSessionStrategy captured by the manager. These tests never
        // actually open a socket, so trust/key material is irrelevant.
        this.settings = Settings.builder().put("path.home", createTempDir()).build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        this.sslConfig = new WorkloadIdentitySslConfig(settings, environment);
        this.threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
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
}
