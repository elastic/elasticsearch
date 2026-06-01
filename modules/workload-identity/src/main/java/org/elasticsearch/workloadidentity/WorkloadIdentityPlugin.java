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

    // Captured alongside httpClientManager so close() can unregister its file watchers on node
    // shutdown. The volatile write also publishes the started instance (and its watcher handles)
    // across the createComponents -> close thread boundary; see WorkloadIdentitySslConfig.
    private volatile WorkloadIdentitySslConfig sslConfig;

    public WorkloadIdentityPlugin() {
        // Clear any prior instance's slot; see WorkloadIdentityRegistry#reset.
        WorkloadIdentityRegistry.reset();
    }

    // Package-private accessor for tests; the manager is intentionally not exported through
    // createComponents' DI return list (only the issuer client interface is).
    WorkloadIdentityHttpClientManager getHttpClientManager() {
        return httpClientManager;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        final Settings settings = services.environment().settings();
        final List<Object> components = new ArrayList<>();
        final WorkloadIdentityIssuerClient client;

        if (WorkloadIdentityIssuerSettings.isEnabled(settings)) {
            // Register the manager as a reload listener before sslConfig.start() so the initial
            // SSLContext publish reaches the manager; sslConfig.start() must precede
            // manager.start() so the SSL delegate is in place before the HC client comes up.
            final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(
                settings,
                services.environment(),
                services.resourceWatcherService()
            );
            final WorkloadIdentityHttpClientManager manager = new WorkloadIdentityHttpClientManager(
                settings,
                sslConfig,
                services.threadPool()
            );
            sslConfig.addReloadListener(manager::reload);
            // Store before start() so a partial start (watchers registered, initial load failed)
            // is still released by close().
            this.sslConfig = sslConfig;
            sslConfig.start();
            // Build the issuer client (which validates the URL) before storing the manager: a
            // malformed URL throws synchronously without leaking the unstarted manager. Store
            // before manager.start() so a partial start is still released by close().
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
     * Tears down the components captured in {@link #createComponents}. Neither implements
     * {@code LifecycleComponent}, so {@code NodeConstruction#loadPluginComponents} does not wire
     * them into the node-shutdown {@code resourcesToClose} list; closing them here is what unregisters
     * the SSL file watchers (sslConfig) and releases the Apache HC connection pool, I/O reactor threads,
     * and the periodic eviction task (manager) on {@code Node#close}. Watchers are stopped before the
     * HTTP client so no reload callback runs during manager shutdown. Both closes are idempotent and
     * tolerate a never-started instance, so a {@code createComponents} that threw partway through is
     * still released.
     */
    @Override
    public void close() {
        final WorkloadIdentitySslConfig sslConfig = this.sslConfig;
        if (sslConfig != null) {
            sslConfig.close();
        }
        final WorkloadIdentityHttpClientManager manager = this.httpClientManager;
        if (manager != null) {
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
