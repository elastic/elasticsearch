/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Module entry point. Registers the {@code workload_identity.*} settings and constructs a
 * singleton {@link WorkloadIdentityIssuerClient}, exposed both as an injectable component via
 * {@link PluginComponentBinding} and through {@link WorkloadIdentityRegistry#getSharedIssuerClient()}
 * for consumers that are not built by the DI container.
 *
 * <p>The module is loaded into every distribution but only activates the network transport when
 * {@link WorkloadIdentityIssuerSettings#isEnabled(Settings)} reports {@code true}. When the
 * feature is disabled, the registered client is the inactive null-object stub
 * ({@link InactiveWorkloadIdentityIssuerClient}); consumers should gate workload-identity-backed
 * code paths on {@link WorkloadIdentityIssuerClient#isEnabled()}.
 *
 * <p>Implements {@link ExtensiblePlugin} as an opt-in marker so other plugins can name
 * {@code workload-identity} in their {@code extendedPlugins}. The default
 * {@link ExtensiblePlugin#loadExtensions} no-op is sufficient; this plugin does not consume
 * extender-contributed extensions.
 */
public class WorkloadIdentityPlugin extends Plugin implements ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(WorkloadIdentityPlugin.class);

    // Captured in createComponents when workload-identity is enabled on this node so that
    // close() can tear it down on node shutdown. Null when the feature is disabled (in which
    // case the client is the InactiveWorkloadIdentityIssuerClient stub and owns no resources).
    // Plugin#close runs from Node#close on the node-shutdown thread after createComponents has
    // returned; volatile is sufficient to publish the reference across those phases.
    private volatile WorkloadIdentityHttpClientManager httpClientManager;

    public WorkloadIdentityPlugin() {
        // Clear any prior instance's slot; see WorkloadIdentityRegistry#reset.
        WorkloadIdentityRegistry.reset();
    }

    // Test-only access to the manager wired in createComponents; the manager is intentionally not
    // exported through createComponents' DI return list (only the issuer client interface is).
    WorkloadIdentityHttpClientManager getHttpClientManagerForTesting() {
        return httpClientManager;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        final Settings settings = services.environment().settings();
        final List<Object> components = new ArrayList<>();
        final WorkloadIdentityIssuerClient client;

        if (WorkloadIdentityIssuerSettings.isEnabled(settings)) {
            final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(settings, services.environment());
            final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(
                settings,
                sslConfig,
                services.threadPool()
            );
            // Construct the issuer client (which validates the configured URL) before starting the
            // manager, so a malformed workload_identity.issuer.url throws without booting the IO
            // reactor thread and the periodic eviction task. Once that synchronous validation has
            // passed, publish the manager to the field before start() so a partial start (e.g.
            // httpClient.start() succeeds but connectionEvictor.start() throws) is still cleaned
            // up by close() on the normal node-shutdown path.
            client = new HttpsWorkloadIdentityIssuerClient(settings, manager, services.threadPool());
            this.httpClientManager = manager;
            manager.start();
        } else {
            logger.debug(
                "workload-identity is not enabled on this node; set [{}] to enable",
                WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey()
            );
            client = InactiveWorkloadIdentityIssuerClient.INSTANCE;
        }

        WorkloadIdentityRegistry.setIssuerClient(client);
        components.add(new PluginComponentBinding<>(WorkloadIdentityIssuerClient.class, client));
        return components;
    }

    /**
     * Tears down the {@link WorkloadIdentityHttpClientManager} captured in {@link #createComponents}.
     * The manager implements {@link java.io.Closeable} but not {@code LifecycleComponent}, so
     * {@code NodeConstruction#loadPluginComponents} does not wire it into the node-shutdown
     * {@code resourcesToClose} list; closing it here is what actually releases the Apache HC
     * connection pool, I/O reactor threads, and the periodic eviction task on {@code Node#close}.
     */
    @Override
    public void close() {
        final WorkloadIdentityHttpClientManager manager = this.httpClientManager;
        if (manager != null) {
            // Idempotent (guarded inside the manager); also safe if createComponents threw between
            // assigning the field and finishing setup, because manager.close() tolerates being
            // called on a never-started instance.
            manager.close();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        final List<Setting<?>> settings = new ArrayList<>();
        settings.addAll(WorkloadIdentityIssuerSettings.getSettings());
        settings.addAll(WorkloadIdentitySslConfig.getSettings());
        settings.addAll(WorkloadIdentityHttpSettings.getSettings());
        return settings;
    }
}
