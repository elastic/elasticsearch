/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * An extension point allowing to plug in custom functionality. This class has a number of extension points that are available to all
 * plugins, in addition you can implement any of the following interfaces to further customize Elasticsearch:
 * <ul>
 * <li>{@link ActionPlugin}
 * <li>{@link AnalysisPlugin}
 * <li>{@link ClusterPlugin}
 * <li>{@link DiscoveryPlugin}
 * <li>{@link HealthPlugin}
 * <li>{@link IngestPlugin}
 * <li>{@link MapperPlugin}
 * <li>{@link NetworkPlugin}
 * <li>{@link RepositoryPlugin}
 * <li>{@link ScriptPlugin}
 * <li>{@link SearchPlugin}
 * <li>{@link ReloadablePlugin}
 * </ul>
 */
public abstract class Plugin implements Closeable {

    /**
     * Provides access to various Elasticsearch services.
     */
    public interface PluginServices {
        /**
         * A client to make requests to the system
         */
        Client client();

        /**
         * A service to allow watching and updating cluster state
         */
        ClusterService clusterService();

        /**
         * A service to reroute shards to other nodes
         */
        RerouteService rerouteService();

        /**
         * A service to allow retrieving an executor to run an async action
         */
        ThreadPool threadPool();

        /**
         * A service to watch for changes to node local files
         */
        ResourceWatcherService resourceWatcherService();

        /**
         * A service to allow running scripts on the local node
         */
        ScriptService scriptService();

        /**
         * The registry for extensible xContent parsing
         */
        NamedXContentRegistry xContentRegistry();

        /**
         * The environment for path and setting configurations
         */
        Environment environment();

        /**
         * The node environment used coordinate access to the data paths
         */
        NodeEnvironment nodeEnvironment();

        /**
         * The registry for {@link NamedWriteable} object parsing
         */
        NamedWriteableRegistry namedWriteableRegistry();

        /**
         * A service that resolves expression to index and alias names
         */
        IndexNameExpressionResolver indexNameExpressionResolver();

        /**
         * A service that manages snapshot repositories.
         */
        RepositoriesService repositoriesService();

        /**
         * An interface for distributed tracing
         */
        TelemetryProvider telemetryProvider();

        /**
         * A service to manage shard allocation in the cluster
         */
        AllocationService allocationService();

        /**
         * A service to manage indices in the cluster
         */
        IndicesService indicesService();

        /**
         * A service to access features supported by nodes in the cluster
         */
        FeatureService featureService();

        /**
         * The system indices for the cluster
         */
        SystemIndices systemIndices();

        /**
         * A service that holds the data stream global retention settings that applies to
         * data streams managed by the data stream lifecycle.
         */
        DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings();

        /**
         * A provider of utilities to observe and report parsing of documents
         */
        DocumentParsingProvider documentParsingProvider();

        /**
         * The task manager for the node. This should only be used by plugins
         * to track task removal by registering a RemovedTaskListener.
         */
        TaskManager taskManager();

        /**
         * The project resolver for the cluster. This should be used to determine the active project against which a request should execute
         */
        ProjectResolver projectResolver();

        /**
         * Provider for additional SlowLog fields
         */
        SlowLogFieldProvider slowLogFieldProvider();

        /**
         * Provider for indexing pressure
         */
        IndexingPressure indexingPressure();
    }

    /**
     * Returns components added by this plugin.
     * <p>
     * Any components returned that implement {@link LifecycleComponent} will have their lifecycle managed.
     * Note: To aid in the migration away from guice, all objects returned as components will be bound in guice
     * to themselves.
     *
     * @param services      Provides access to various Elasticsearch services
     */
    public Collection<?> createComponents(PluginServices services) {
        return Collections.emptyList();
    }

    /**
     * Additional node settings loaded by the plugin. Note that settings that are explicit in the nodes settings can't be
     * overwritten with the additional settings. These settings added if they don't exist.
     */
    public Settings additionalSettings() {
        return Settings.EMPTY;
    }

    /**
     * Returns parsers for {@link NamedWriteable} this plugin will use over the transport protocol.
     * @see NamedWriteableRegistry
     */
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.emptyList();
    }

    /**
     * Returns parsers for named objects this plugin will parse from {@link XContentParser#namedObject(Class, String, Object)}.
     * @see NamedWriteableRegistry
     */
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.emptyList();
    }

    /**
     * Called before a new index is created on a node. The given module can be used to register index-level
     * extensions.
     */
    public void onIndexModule(IndexModule indexModule) {}

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    public List<Setting<?>> getSettings() {
        return Collections.emptyList();
    }

    /**
     * Returns a list of additional settings filter for this plugin
     */
    public List<String> getSettingsFilter() {
        return Collections.emptyList();
    }

    /**
     * Provides a function to modify index template meta data on startup.
     * <p>
     * Plugins should return the input template map via {@link UnaryOperator#identity()} if no upgrade is required.
     * <p>
     * The order of the template upgrader calls is undefined and can change between runs so, it is expected that
     * plugins will modify only templates owned by them to avoid conflicts.
     *
     * @return Never {@code null}. The same or upgraded {@code IndexTemplateMetadata} map.
     * @throws IllegalStateException if the node should not start because at least one {@code IndexTemplateMetadata}
     *                               cannot be upgraded
     */
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return UnaryOperator.identity();
    }

    /**
     * Returns operators to modify custom metadata in the cluster state on startup.
     *
     * <p>Each key of the map returned gives the type of custom to be modified. Each value is an operator to be applied to that custom
     * metadata. The operator will be invoked with the result of calling
     * {@link org.elasticsearch.cluster.metadata.ProjectMetadata#custom(String)} with the map key as its argument,
     * and should downcast the value accordingly.
     *
     * <p>Plugins should return an empty map if no upgrade is required.
     *
     * <p>The order of the upgrade calls is undefined and can change between runs. It is expected that plugins will modify only templates
     * owned by them to avoid conflicts.
     */
    public Map<String, UnaryOperator<Metadata.ProjectCustom>> getProjectCustomMetadataUpgraders() {
        return Map.of();
    }

    /**
     * Provides the list of this plugin's custom thread pools, empty if
     * none.
     *
     * @param settings the current settings
     * @return executors builders for this plugin's custom thread pools
     */
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.emptyList();
    }

    /**
     * Returns a list of checks that are enforced when a node starts up once a node has the transport protocol bound to a non-loopback
     * interface. In this case we assume the node is running in production and all bootstrap checks must pass. This allows plugins
     * to provide a better out of the box experience by pre-configuring otherwise (in production) mandatory settings or to enforce certain
     * configurations like OS settings or 3rd party resources.
     */
    public List<BootstrapCheck> getBootstrapChecks() {
        return Collections.emptyList();
    }

    /**
     * Close the resources opened by this plugin.
     *
     * @throws IOException if the plugin failed to close its resources
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * An {@link IndexSettingProvider} allows hooking in to parts of an index
     * lifecycle to provide explicit default settings for newly created indices. Rather than changing
     * the default values for an index-level setting, these act as though the setting has been set
     * explicitly, but still allow the setting to be overridden by a template or creation request body.
     */
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        return Collections.emptyList();
    }
}
