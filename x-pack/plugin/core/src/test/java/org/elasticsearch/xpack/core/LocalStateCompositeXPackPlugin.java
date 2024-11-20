/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse.ResetFeatureStateStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.plugins.interceptor.RestServerActionPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class LocalStateCompositeXPackPlugin extends XPackPlugin
    implements
        ScriptPlugin,
        ActionPlugin,
        IngestPlugin,
        NetworkPlugin,
        ClusterPlugin,
        DiscoveryPlugin,
        ClusterCoordinationPlugin,
        MapperPlugin,
        AnalysisPlugin,
        PersistentTaskPlugin,
        EnginePlugin,
        IndexStorePlugin,
        SystemIndexPlugin,
        SearchPlugin,
        ShutdownAwarePlugin,
        RestServerActionPlugin {

    private XPackLicenseState licenseState;
    private SSLService sslService;
    private LicenseService licenseService;
    private LongSupplier epochMillisSupplier;
    protected List<Plugin> plugins = new ArrayList<>();

    public LocalStateCompositeXPackPlugin(final Settings settings, final Path configPath) {
        super(settings);
    }

    // Get around all the setOnce nonsense in the plugin
    @Override
    protected SSLService getSslService() {
        return sslService;
    }

    @Override
    protected void setSslService(SSLService sslService) {
        this.sslService = sslService;
    }

    @Override
    protected LicenseService getLicenseService() {
        return licenseService;
    }

    @Override
    protected void setLicenseService(LicenseService licenseService) {
        this.licenseService = licenseService;
    }

    @Override
    protected XPackLicenseState getLicenseState() {
        return licenseState;
    }

    @Override
    protected void setLicenseState(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    @Override
    protected LongSupplier getEpochMillisSupplier() {
        return epochMillisSupplier;
    }

    @Override
    protected void setEpochMillisSupplier(LongSupplier epochMillisSupplier) {
        this.epochMillisSupplier = epochMillisSupplier;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        List<Object> components = new ArrayList<>(super.createComponents(services));

        filterPlugins(Plugin.class).forEach(p -> components.addAll(p.createComponents(services)));
        return components;
    }

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        List<RestHeaderDefinition> headers = new ArrayList<>(super.getRestHeaders());
        filterPlugins(ActionPlugin.class).forEach(p -> headers.addAll(p.getRestHeaders()));
        return headers;
    }

    @Override
    public List<Setting<?>> getSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>(super.getSettings());
        filterPlugins(Plugin.class).forEach(p -> settings.addAll(p.getSettings()));
        return settings;
    }

    @Override
    public List<String> getSettingsFilter() {
        List<String> filters = new ArrayList<>(super.getSettingsFilter());
        filterPlugins(Plugin.class).forEach(p -> filters.addAll(p.getSettingsFilter()));
        return filters;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>(super.getActions());
        filterPlugins(ActionPlugin.class).forEach(p -> actions.addAll(p.getActions()));
        return actions;
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        List<ActionFilter> filters = new ArrayList<>(super.getActionFilters());
        filterPlugins(ActionPlugin.class).forEach(p -> filters.addAll(p.getActionFilters()));
        return filters;
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        List<RestHandler> handlers = new ArrayList<>(
            super.getRestHandlers(
                settings,
                namedWriteableRegistry,
                restController,
                clusterSettings,
                indexScopedSettings,
                settingsFilter,
                indexNameExpressionResolver,
                nodesInCluster,
                clusterSupportsFeature
            )
        );
        filterPlugins(ActionPlugin.class).forEach(
            p -> handlers.addAll(
                p.getRestHandlers(
                    settings,
                    namedWriteableRegistry,
                    restController,
                    clusterSettings,
                    indexScopedSettings,
                    settingsFilter,
                    indexNameExpressionResolver,
                    nodesInCluster,
                    clusterSupportsFeature
                )
            )
        );
        return handlers;
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(super.getNamedWriteables());
        for (Plugin p : plugins) {
            entries.addAll(p.getNamedWriteables());
        }
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(super.getNamedXContent());
        for (Plugin p : plugins) {
            entries.addAll(p.getNamedXContent());
        }
        return entries;
    }

    // End of the XPackPlugin overrides

    @Override
    public Settings additionalSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(super.additionalSettings());
        filterPlugins(Plugin.class).forEach(p -> builder.put(p.additionalSettings()));
        return builder.build();
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        List<ScriptContext<?>> contexts = new ArrayList<>();
        filterPlugins(ScriptPlugin.class).forEach(p -> contexts.addAll(p.getContexts()));
        return contexts;
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        Map<String, Processor.Factory> processors = new HashMap<>();
        filterPlugins(IngestPlugin.class).forEach(p -> processors.putAll(p.getProcessors(parameters)));
        return processors;
    }

    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        List<TransportInterceptor> interceptors = new ArrayList<>();
        filterPlugins(NetworkPlugin.class).forEach(
            p -> interceptors.addAll(p.getTransportInterceptors(namedWriteableRegistry, threadContext))
        );
        return interceptors;
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService
    ) {
        Map<String, Supplier<Transport>> transports = new HashMap<>(
            super.getTransports(settings, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService)
        );
        filterPlugins(NetworkPlugin.class).forEach(
            p -> transports.putAll(
                p.getTransports(settings, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService)
            )
        );
        return transports;

    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        Map<String, Supplier<HttpServerTransport>> transports = new HashMap<>();
        filterPlugins(NetworkPlugin.class).forEach(
            p -> transports.putAll(
                p.getHttpTransports(
                    settings,
                    threadPool,
                    bigArrays,
                    pageCacheRecycler,
                    circuitBreakerService,
                    xContentRegistry,
                    networkService,
                    dispatcher,
                    perRequestThreadContext,
                    clusterSettings,
                    tracer
                )
            )
        );
        return transports;
    }

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
        List<BootstrapCheck> checks = new ArrayList<>();
        filterPlugins(Plugin.class).forEach(p -> checks.addAll(p.getBootstrapChecks()));
        return Collections.unmodifiableList(checks);
    }

    @Override
    public RestInterceptor getRestHandlerInterceptor(ThreadContext threadContext) {
        // There can be only one.
        List<RestInterceptor> items = filterPlugins(ActionPlugin.class).stream()
            .filter(RestServerActionPlugin.class::isInstance)
            .map(RestServerActionPlugin.class::cast)
            .map(p -> p.getRestHandlerInterceptor(threadContext))
            .filter(Objects::nonNull)
            .toList();

        if (items.size() > 1) {
            throw new UnsupportedOperationException("Only the security ActionPlugin should override this");
        } else if (items.size() == 1) {
            return items.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        List<ExecutorBuilder<?>> builders = new ArrayList<>();
        filterPlugins(Plugin.class).forEach(p -> builders.addAll(p.getExecutorBuilders(settings)));
        return builders;
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return templates -> {
            for (Plugin p : plugins) {
                templates = p.getIndexTemplateMetadataUpgrader().apply(templates);
            }
            return templates;
        };
    }

    @Override
    public Map<String, ElectionStrategy> getElectionStrategies() {
        Map<String, ElectionStrategy> electionStrategies = new HashMap<>();
        filterPlugins(ClusterCoordinationPlugin.class).forEach(p -> electionStrategies.putAll(p.getElectionStrategies()));
        return electionStrategies;
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        Set<IndexSettingProvider> providers = new HashSet<>();
        filterPlugins(Plugin.class).forEach(p -> providers.addAll(p.getAdditionalIndexSettingProviders(parameters)));
        providers.addAll(super.getAdditionalIndexSettingProviders(parameters));
        return providers;

    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
        Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> tokenizers = new HashMap<>();
        filterPlugins(AnalysisPlugin.class).forEach(p -> tokenizers.putAll(p.getTokenizers()));
        return tokenizers;
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        super.onIndexModule(indexModule);
        filterPlugins(Plugin.class).forEach(p -> p.onIndexModule(indexModule));
    }

    @Override
    public Function<String, FieldPredicate> getFieldFilter() {
        List<Function<String, FieldPredicate>> items = filterPlugins(MapperPlugin.class).stream()
            .map(p -> p.getFieldFilter())
            .filter(p -> p.equals(NOOP_FIELD_FILTER) == false)
            .toList();
        if (items.size() > 1) {
            throw new UnsupportedOperationException("Only the security MapperPlugin should override this");
        } else if (items.size() == 1) {
            return items.get(0);
        } else {
            // return the same default from MapperPlugin
            return MapperPlugin.NOOP_FIELD_FILTER;
        }
    }

    @Override
    public BiConsumer<DiscoveryNode, ClusterState> getJoinValidator() {
        // There can be only one.
        List<BiConsumer<DiscoveryNode, ClusterState>> items = filterPlugins(ClusterCoordinationPlugin.class).stream()
            .map(p -> p.getJoinValidator())
            .toList();
        if (items.size() > 1) {
            throw new UnsupportedOperationException("Only the security DiscoveryPlugin should override this");
        } else if (items.size() == 1) {
            return items.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return filterPlugins(PersistentTaskPlugin.class).stream()
            .map(p -> p.getPersistentTasksExecutor(clusterService, threadPool, client, settingsModule, expressionResolver))
            .flatMap(List::stream)
            .collect(toList());
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        RepositoriesMetrics repositoriesMetrics
    ) {
        HashMap<String, Repository.Factory> repositories = new HashMap<>(
            super.getRepositories(env, namedXContentRegistry, clusterService, bigArrays, recoverySettings, repositoriesMetrics)
        );
        filterPlugins(RepositoryPlugin.class).forEach(
            r -> repositories.putAll(
                r.getRepositories(env, namedXContentRegistry, clusterService, bigArrays, recoverySettings, RepositoriesMetrics.NOOP)
            )
        );
        return repositories;
    }

    @Override
    public Map<String, Repository.Factory> getInternalRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        HashMap<String, Repository.Factory> internalRepositories = new HashMap<>(
            super.getInternalRepositories(env, namedXContentRegistry, clusterService, recoverySettings)
        );
        filterPlugins(RepositoryPlugin.class).forEach(
            r -> internalRepositories.putAll(r.getInternalRepositories(env, namedXContentRegistry, clusterService, recoverySettings))
        );
        return internalRepositories;
    }

    @Override
    public BiConsumer<Snapshot, IndexVersion> addPreRestoreVersionCheck() {
        List<BiConsumer<Snapshot, IndexVersion>> checks = filterPlugins(RepositoryPlugin.class).stream()
            .map(RepositoryPlugin::addPreRestoreVersionCheck)
            .filter(Objects::nonNull)
            .toList();
        return checks.isEmpty() ? null : (s, v) -> checks.forEach(c -> c.accept(s, v));
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(plugins);
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        List<Optional<EngineFactory>> enginePlugins = filterPlugins(EnginePlugin.class).stream()
            .map(p -> p.getEngineFactory(indexSettings))
            .toList();
        if (enginePlugins.size() == 0) {
            return Optional.empty();
        } else if (enginePlugins.size() == 1) {
            return enginePlugins.stream().findFirst().get();
        } else {
            throw new IllegalStateException("Only one EngineFactory plugin allowed");
        }
    }

    @Override
    public Collection<RequestValidators.RequestValidator<PutMappingRequest>> mappingRequestValidators() {
        return filterPlugins(ActionPlugin.class).stream().flatMap(p -> p.mappingRequestValidators().stream()).collect(Collectors.toList());
    }

    @Override
    public Collection<RequestValidators.RequestValidator<IndicesAliasesRequest>> indicesAliasesRequestValidators() {
        return filterPlugins(ActionPlugin.class).stream()
            .flatMap(p -> p.indicesAliasesRequestValidators().stream())
            .collect(Collectors.toList());
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        Set<AllocationDecider> deciders = new HashSet<>();
        deciders.addAll(
            filterPlugins(ClusterPlugin.class).stream()
                .flatMap(p -> p.createAllocationDeciders(settings, clusterSettings).stream())
                .toList()
        );
        deciders.addAll(super.createAllocationDeciders(settings, clusterSettings));
        return deciders;
    }

    @Override
    public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        final Map<String, ExistingShardsAllocator> allocators = new HashMap<>();
        filterPlugins(ClusterPlugin.class).forEach(p -> allocators.putAll(p.getExistingShardsAllocators()));
        return allocators;
    }

    @Override
    public Map<String, IndexStorePlugin.DirectoryFactory> getDirectoryFactories() {
        final Map<String, IndexStorePlugin.DirectoryFactory> factories = new HashMap<>();
        filterPlugins(IndexStorePlugin.class).forEach(p -> factories.putAll(p.getDirectoryFactories()));
        return factories;
    }

    @Override
    public Map<String, RecoveryStateFactory> getRecoveryStateFactories() {
        final Map<String, RecoveryStateFactory> factories = new HashMap<>();
        filterPlugins(IndexStorePlugin.class).forEach(p -> factories.putAll(p.getRecoveryStateFactories()));
        return factories;
    }

    @Override
    public List<IndexFoldersDeletionListener> getIndexFoldersDeletionListeners() {
        final List<IndexFoldersDeletionListener> listeners = new ArrayList<>();
        filterPlugins(IndexStorePlugin.class).forEach(p -> listeners.addAll(p.getIndexFoldersDeletionListeners()));
        return Collections.unmodifiableList(listeners);
    }

    @Override
    public Map<String, IndexStorePlugin.SnapshotCommitSupplier> getSnapshotCommitSuppliers() {
        final Map<String, IndexStorePlugin.SnapshotCommitSupplier> suppliers = new HashMap<>();
        filterPlugins(IndexStorePlugin.class).forEach(p -> suppliers.putAll(p.getSnapshotCommitSuppliers()));
        return suppliers;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> filterPlugins(Class<T> type) {
        return plugins.stream().filter(x -> type.isAssignableFrom(x.getClass())).map(p -> ((T) p)).collect(Collectors.toList());
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return this.filterPlugins(SystemIndexPlugin.class)
            .stream()
            .flatMap(p -> p.getSystemIndexDescriptors(this.settings).stream())
            .collect(Collectors.toList());
    }

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        Map<String, MetadataFieldMapper.TypeParser> pluginsMetadataMappers = filterPlugins(MapperPlugin.class).stream()
            .map(MapperPlugin::getMetadataMappers)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // the xpack plugin itself exposes a metadata mapper so let's include it as well
        Map<String, MetadataFieldMapper.TypeParser> metadataMappersIncludingXPackPlugin = new HashMap<>(pluginsMetadataMappers);
        metadataMappersIncludingXPackPlugin.putAll(super.getMetadataMappers());
        return metadataMappersIncludingXPackPlugin;
    }

    @Override
    public String getFeatureName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getFeatureDescription() {
        return this.getClass().getCanonicalName();
    }

    @Override
    public CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> getRequestCacheKeyDifferentiator() {
        final List<CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException>> differentiators = filterPlugins(SearchPlugin.class)
            .stream()
            .map(SearchPlugin::getRequestCacheKeyDifferentiator)
            .filter(Objects::nonNull)
            .toList();

        if (differentiators.size() > 1) {
            throw new UnsupportedOperationException("Only the security SearchPlugin should provide the request cache key differentiator");
        } else if (differentiators.size() == 1) {
            return differentiators.get(0);
        } else {
            return null;
        }

    }

    @Override
    public void cleanUpFeature(
        ClusterService clusterService,
        Client client,
        ActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> finalListener
    ) {
        List<SystemIndexPlugin> systemPlugins = filterPlugins(SystemIndexPlugin.class);

        GroupedActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> allListeners = new GroupedActionListener<>(
            systemPlugins.size(),
            finalListener.delegateFailureAndWrap((delegate, listenerResults) -> {
                // If the clean-up produced only one result, use that to pass along. In most
                // cases it should be 1-1 mapping of feature to response. Passing back success
                // prevents us from writing validation tests on this API.
                if (listenerResults != null && listenerResults.size() == 1) {
                    delegate.onResponse(listenerResults.stream().findFirst().get());
                } else {
                    delegate.onResponse(ResetFeatureStateStatus.success(getFeatureName()));
                }
            })
        );
        systemPlugins.forEach(plugin -> plugin.cleanUpFeature(clusterService, client, allListeners));
    }

    @Override
    public boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
        return filterPlugins(ShutdownAwarePlugin.class).stream().allMatch(plugin -> plugin.safeToShutdown(nodeId, shutdownType));
    }

    @Override
    public void signalShutdown(Collection<String> shutdownNodeIds) {
        filterPlugins(ShutdownAwarePlugin.class).forEach(plugin -> plugin.signalShutdown(shutdownNodeIds));
    }
}
