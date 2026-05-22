/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.workloadidentity.http.InactiveWorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.http.WorkloadIdentityHttpClientManager;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkloadIdentityPluginTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        this.threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * Guards the static-SetOnce slot used by {@link WorkloadIdentityPlugin} to publish the
     * node-wide {@link WorkloadIdentityIssuerClient}. The plugin is loaded into every node of a
     * multi-node JVM test through a single classloader (see {@code MockPluginsService}), which
     * means the static slot is shared. Constructing a second plugin instance must reset the slot
     * so the second node's {@code createComponents} can set the client without throwing
     * {@code SetOnce.AlreadySetException}.
     */
    public void testSecondPluginInstanceCanSetIssuerClient() {
        new WorkloadIdentityPlugin().setIssuerClient(InactiveWorkloadIdentityIssuerClient.INSTANCE);
        // A second plugin instance models a second node in the same JVM (or a subsequent test
        // class in the same Gradle fork). It must not see the first instance's already-set slot.
        new WorkloadIdentityPlugin().setIssuerClient(InactiveWorkloadIdentityIssuerClient.INSTANCE);
        assertSame(InactiveWorkloadIdentityIssuerClient.INSTANCE, WorkloadIdentityPlugin.getSharedIssuerClient());
    }

    /**
     * When workload-identity is enabled, the plugin owns a {@link WorkloadIdentityHttpClientManager}
     * that is not a {@code LifecycleComponent} and therefore not auto-closed by
     * {@code NodeConstruction#loadPluginComponents}. {@link WorkloadIdentityPlugin#close()} is
     * the only thing that releases the Apache HC connection pool / IO reactor / evictor on node
     * shutdown. Pin that contract: after close(), the manager must reject further use.
     */
    public void testCloseTearsDownHttpClientManagerWhenEnabled() throws Exception {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey(), "https://issuer.example:8443")
            .build();

        final WorkloadIdentityPlugin plugin = new WorkloadIdentityPlugin();
        plugin.createComponents(mockServices(settings));

        final WorkloadIdentityHttpClientManager manager = plugin.getHttpClientManagerForTesting();
        assertNotNull("plugin should retain the HTTP client manager when workload-identity is enabled", manager);

        // Sanity-check that the manager was actually started during createComponents, so the
        // post-close assertion below unambiguously witnesses the close() transition.
        assertNotNull(manager.getHttpClient());

        plugin.close();

        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        assertThat(ex.getMessage(), containsString("is closed"));
    }

    /**
     * Idempotency is part of the close contract: {@code Node#close} can race with other shutdown
     * paths (e.g. bootstrap failures) that also close the plugin. A second call must not throw
     * and must not perturb the closed state of the underlying manager.
     */
    public void testCloseIsIdempotentWhenEnabled() throws Exception {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey(), "https://issuer.example:8443")
            .build();

        final WorkloadIdentityPlugin plugin = new WorkloadIdentityPlugin();
        plugin.createComponents(mockServices(settings));

        plugin.close();
        // A second close() must be a safe no-op; relying on the manager's own idempotency guard.
        plugin.close();
    }

    /**
     * When the feature is disabled (no {@code workload_identity.issuer.url} configured) the
     * plugin owns no HTTP resources. {@code close()} must still be safe to call — Node always
     * invokes Plugin#close on shutdown regardless of whether the feature is active.
     */
    public void testCloseIsSafeWhenDisabled() throws Exception {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();

        final WorkloadIdentityPlugin plugin = new WorkloadIdentityPlugin();
        plugin.createComponents(mockServices(settings));

        plugin.close();
    }

    /**
     * Even without ever calling createComponents (e.g. plugin construction succeeded but node
     * bootstrap aborted before wiring), close() must not NPE on the unassigned manager field.
     */
    public void testCloseBeforeCreateComponentsIsSafe() throws Exception {
        new WorkloadIdentityPlugin().close();
    }

    /**
     * Build a {@link Plugin.PluginServices} stub that satisfies the dependencies
     * {@link WorkloadIdentityPlugin#createComponents} actually reads: the environment (for
     * settings + SSL config loading) and the thread pool (handed to the HTTP client manager
     * for the connection eviction task). Everything else on PluginServices is left as the
     * Mockito default; the plugin does not touch it.
     */
    private Plugin.PluginServices mockServices(Settings settings) {
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.environment()).thenReturn(environment);
        when(services.threadPool()).thenReturn(threadPool);
        return services;
    }
}
