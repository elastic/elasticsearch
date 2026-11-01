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
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkloadIdentityPluginTests extends ESTestCase {

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcher;

    @Before
    public void setupThreadPool() {
        this.threadPool = new TestThreadPool(getTestName());
        // ENABLED=false: the plugin tests don't exercise file-watch reload. The watcher is still
        // a required collaborator on the SSL config the plugin builds, so we hand it a disabled
        // one (no polling thread, no I/O) rather than letting Mockito return null.
        this.resourceWatcher = new ResourceWatcherService(
            Settings.builder().put(ResourceWatcherService.ENABLED.getKey(), false).build(),
            threadPool
        );
    }

    @After
    public void shutdownThreadPool() {
        resourceWatcher.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * Guards the {@link WorkloadIdentityRegistry} static slot used by
     * {@link WorkloadIdentityPlugin} to publish the node-wide issuer client. The plugin is
     * loaded into every node of a multi-node JVM test through a single classloader (see
     * {@code MockPluginsService}), which means the registry slot is shared. Constructing a
     * second plugin instance must reset the slot so the second node's {@code createComponents}
     * can publish its client without the previous instance's value lingering.
     */
    public void testSecondPluginInstanceCanSetIssuerClient() {
        new WorkloadIdentityPlugin();
        WorkloadIdentityRegistry.setIssuerClient(InactiveWorkloadIdentityIssuerClient.INSTANCE);
        // A second plugin instance models a second node in the same JVM (or a subsequent test
        // class in the same Gradle fork). The constructor must reset the slot so this second
        // setIssuerClient publishes successfully.
        new WorkloadIdentityPlugin();
        WorkloadIdentityRegistry.setIssuerClient(InactiveWorkloadIdentityIssuerClient.INSTANCE);
        assertSame(InactiveWorkloadIdentityIssuerClient.INSTANCE, WorkloadIdentityRegistry.getSharedIssuerClient());
    }

    /**
     * Before any plugin instance has published, the registry must throw rather than silently
     * return null. This is the contract extender plugins rely on to distinguish "issuer client
     * not yet wired" from "issuer client wired but disabled".
     */
    public void testGetSharedIssuerClientThrowsBeforeFirstSet() {
        WorkloadIdentityRegistry.reset();
        final IllegalStateException ex = expectThrows(IllegalStateException.class, WorkloadIdentityRegistry::getSharedIssuerClient);
        assertThat(ex.getMessage(), containsString("not constructed"));
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

        final WorkloadIdentityHttpClientManager manager = plugin.getHttpClientManager();
        assertNotNull("plugin should retain the HTTP client manager when workload-identity is enabled", manager);

        // Sanity-check that the manager was actually started during createComponents, so the
        // post-close assertion below unambiguously witnesses the close() transition.
        assertNotNull(manager.getHttpClient());

        plugin.close();

        final IllegalStateException ex = expectThrows(IllegalStateException.class, manager::getHttpClient);
        assertThat(ex.getMessage(), containsString("[CLOSED]"));
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
     * settings + SSL config loading), the thread pool (handed to the HTTP client manager for
     * the connection eviction task), and the {@link ResourceWatcherService} (passed through to
     * {@link WorkloadIdentitySslConfig} for file-watch SSL reload). Everything else on
     * PluginServices is left as the Mockito default; the plugin does not touch it.
     */
    private Plugin.PluginServices mockServices(Settings settings) {
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.environment()).thenReturn(environment);
        when(services.threadPool()).thenReturn(threadPool);
        when(services.resourceWatcherService()).thenReturn(resourceWatcher);
        return services;
    }
}
