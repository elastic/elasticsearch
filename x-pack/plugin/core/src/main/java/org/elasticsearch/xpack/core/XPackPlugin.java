/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.Licensing;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.MutableLicenseService;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.snapshots.sourceonly.SourceOnlySnapshotRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.cluster.routing.allocation.mapper.DataTierFieldMapper;
import org.elasticsearch.xpack.core.action.DataStreamInfoTransportAction;
import org.elasticsearch.xpack.core.action.DataStreamLifecycleUsageTransportAction;
import org.elasticsearch.xpack.core.action.DataStreamUsageTransportAction;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.rest.action.RestXPackInfoAction;
import org.elasticsearch.xpack.core.rest.action.RestXPackUsageAction;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationReloader;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TransportTermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.rest.RestTermsEnumAction;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings("HiddenField")
public class XPackPlugin extends XPackClientPlugin
    implements
        ExtensiblePlugin,
        RepositoryPlugin,
        EnginePlugin,
        ClusterPlugin,
        MapperPlugin {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(XPackPlugin.class);

    public static final String ASYNC_RESULTS_INDEX = ".async-search";
    public static final String XPACK_INSTALLED_NODE_ATTR = "xpack.installed";
    private static final Logger logger = LogManager.getLogger(XPackPlugin.class);

    // TODO: clean up this library to not ask for write access to all system properties!
    static {
        // invoke this clinit in unbound with permissions to access all system properties
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    try {
                        Class.forName("com.unboundid.util.Debug");
                        Class.forName("com.unboundid.ldap.sdk.LDAPConnectionOptions");
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }
            });
            // TODO: fix gradle to add all security resources (plugin metadata) to test classpath
            // of watcher plugin, which depends on it directly. This prevents these plugins
            // from being initialized correctly by the test framework, and means we have to
            // have this leniency.
        } catch (ExceptionInInitializerError bogus) {
            if (bogus.getCause() instanceof SecurityException == false) {
                throw bogus; // some other bug
            }
        }
    }

    protected final Settings settings;
    // private final Environment env;
    protected final Licensing licensing;
    // These should not be directly accessed as they cannot be overridden in tests. Please use the getters so they can be overridden.
    private static final SetOnce<SSLService> sslService = new SetOnce<>();
    // non-final to allow for testing
    private static SetOnce<LongSupplier> epochMillisSupplier = new SetOnce<>();
    private static SetOnce<XPackLicenseState> licenseState = new SetOnce<>();
    private static SetOnce<LicenseService> licenseService = new SetOnce<>();

    public XPackPlugin(final Settings settings) {
        super();
        // FIXME: The settings might be changed after this (e.g. from "additionalSettings" method in other plugins)
        // We should only depend on the settings from the Environment object passed to createComponents
        this.settings = settings;
        licenseState = new SetOnce<>();
        licenseService = new SetOnce<>();
        epochMillisSupplier = new SetOnce<>();

        this.licensing = new Licensing(settings);
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    protected SSLService getSslService() {
        return getSharedSslService();
    }

    protected LicenseService getLicenseService() {
        return getSharedLicenseService();
    }

    protected XPackLicenseState getLicenseState() {
        return getSharedLicenseState();
    }

    protected LongSupplier getEpochMillisSupplier() {
        return getSharedEpochMillisSupplier();
    }

    protected void setSslService(SSLService sslService) {
        XPackPlugin.sslService.set(sslService);
    }

    protected void setLicenseService(LicenseService licenseService) {
        XPackPlugin.licenseService.set(licenseService);
    }

    protected void setLicenseState(XPackLicenseState licenseState) {
        XPackPlugin.licenseState.set(licenseState);
    }

    protected void setEpochMillisSupplier(LongSupplier epochMillisSupplier) {
        XPackPlugin.epochMillisSupplier.set(epochMillisSupplier);
    }

    public static SSLService getSharedSslService() {
        final SSLService ssl = XPackPlugin.sslService.get();
        if (ssl == null) {
            throw new IllegalStateException("SSL Service is not constructed yet");
        }
        return ssl;
    }

    public static LicenseService getSharedLicenseService() {
        return licenseService.get();
    }

    public static XPackLicenseState getSharedLicenseState() {
        return licenseState.get();
    }

    public static LongSupplier getSharedEpochMillisSupplier() {
        return epochMillisSupplier.get();
    }

    /**
     * Checks if the cluster state allows this node to add x-pack metadata to the cluster state,
     * and throws an exception otherwise.
     * This check should be called before installing any x-pack metadata to the cluster state,
     * to ensure that the other nodes that are part of the cluster will be able to deserialize
     * that metadata. Note that if the cluster state already contains x-pack metadata, this
     * check assumes that the nodes are already ready to receive additional x-pack metadata.
     * Having this check properly in place everywhere allows to install x-pack into a cluster
     * using a rolling restart.
     */
    public static void checkReadyForXPackCustomMetadata(ClusterState clusterState) {
        if (alreadyContainsXPackCustomMetadata(clusterState)) {
            return;
        }
        List<DiscoveryNode> notReadyNodes = nodesNotReadyForXPackCustomMetadata(clusterState);
        if (notReadyNodes.isEmpty() == false) {
            throw new IllegalStateException("The following nodes are not ready yet for enabling x-pack custom metadata: " + notReadyNodes);
        }
    }

    /**
     * Checks if the cluster state allows this node to add x-pack metadata to the cluster state.
     * See {@link #checkReadyForXPackCustomMetadata} for more details.
     */
    public static boolean isReadyForXPackCustomMetadata(ClusterState clusterState) {
        return alreadyContainsXPackCustomMetadata(clusterState) || nodesNotReadyForXPackCustomMetadata(clusterState).isEmpty();
    }

    /**
     * Returns the list of nodes that won't allow this node from adding x-pack metadata to the cluster state.
     * See {@link #checkReadyForXPackCustomMetadata} for more details.
     */
    public static List<DiscoveryNode> nodesNotReadyForXPackCustomMetadata(ClusterState clusterState) {
        // check that all nodes would be capable of deserializing newly added x-pack metadata
        final List<DiscoveryNode> notReadyNodes = clusterState.nodes().stream().filter(node -> {
            final String xpackInstalledAttr = node.getAttributes().getOrDefault(XPACK_INSTALLED_NODE_ATTR, "false");
            return Booleans.parseBoolean(xpackInstalledAttr) == false;
        }).collect(Collectors.toList());

        return notReadyNodes;
    }

    private static boolean alreadyContainsXPackCustomMetadata(ClusterState clusterState) {
        final Metadata metadata = clusterState.metadata();
        return metadata.custom(LicensesMetadata.TYPE) != null
            || metadata.custom(MlMetadata.TYPE) != null
            || metadata.custom(WatcherMetadata.TYPE) != null
            || clusterState.custom(TokenMetadata.TYPE) != null
            || metadata.custom(TransformMetadata.TYPE) != null;
    }

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Map.of(DataTierFieldMapper.NAME, DataTierFieldMapper.PARSER);
    }

    @Override
    public Settings additionalSettings() {
        final String xpackInstalledNodeAttrSetting = "node.attr." + XPACK_INSTALLED_NODE_ATTR;

        if (settings.get(xpackInstalledNodeAttrSetting) != null) {
            throw new IllegalArgumentException("Directly setting [" + xpackInstalledNodeAttrSetting + "] is not permitted");
        }
        return Settings.builder().put(super.additionalSettings()).put(xpackInstalledNodeAttrSetting, "true").build();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        List<Object> components = new ArrayList<>();

        final SSLService sslService = createSSLService(environment, resourceWatcherService);

        LicenseService licenseService = getLicenseService();
        if (licenseService == null) {
            licenseService = new ClusterStateLicenseService(settings, threadPool, clusterService, getClock(), getLicenseState());
            setLicenseService(licenseService);
        }

        setEpochMillisSupplier(threadPool::absoluteTimeInMillis);

        // It is useful to override these as they are what guice is injecting into actions
        components.add(sslService);
        components.add(new PluginComponentBinding<>(MutableLicenseService.class, licenseService));
        components.add(new PluginComponentBinding<>(LicenseService.class, licenseService));
        components.add(getLicenseState());

        return components;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(XPackInfoAction.INSTANCE, getInfoAction()));
        actions.add(new ActionHandler<>(XPackUsageAction.INSTANCE, getUsageAction()));
        actions.addAll(licensing.getActions());
        actions.add(new ActionHandler<>(TermsEnumAction.INSTANCE, TransportTermsEnumAction.class));
        actions.add(new ActionHandler<>(DeleteAsyncResultAction.INSTANCE, TransportDeleteAsyncResultAction.class));
        actions.add(new ActionHandler<>(XPackInfoFeatureAction.DATA_TIERS, DataTiersInfoTransportAction.class));
        actions.add(new ActionHandler<>(XPackUsageFeatureAction.DATA_TIERS, DataTiersUsageTransportAction.class));
        actions.add(new ActionHandler<>(XPackUsageFeatureAction.DATA_STREAMS, DataStreamUsageTransportAction.class));
        actions.add(new ActionHandler<>(XPackInfoFeatureAction.DATA_STREAMS, DataStreamInfoTransportAction.class));
        actions.add(new ActionHandler<>(XPackUsageFeatureAction.DATA_STREAM_LIFECYCLE, DataStreamLifecycleUsageTransportAction.class));
        actions.add(new ActionHandler<>(XPackUsageFeatureAction.HEALTH, HealthApiUsageTransportAction.class));
        actions.add(new ActionHandler<>(XPackUsageFeatureAction.REMOTE_CLUSTERS, RemoteClusterUsageTransportAction.class));
        return actions;
    }

    // overridable for tests
    protected Class<? extends TransportAction<XPackUsageRequest, XPackUsageResponse>> getUsageAction() {
        return TransportXPackUsageAction.class;
    }

    // overridable for tests
    protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
        return TransportXPackInfoAction.class;
    }

    @Override
    public List<ActionType<? extends ActionResponse>> getClientActions() {
        List<ActionType<? extends ActionResponse>> actions = new ArrayList<>();
        actions.addAll(licensing.getClientActions());
        actions.addAll(super.getClientActions());
        return actions;
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        List<ActionFilter> filters = new ArrayList<>();
        filters.addAll(licensing.getActionFilters());
        return filters;
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new RestXPackInfoAction());
        handlers.add(new RestXPackUsageAction());
        handlers.add(new RestTermsEnumAction());
        handlers.addAll(
            licensing.getRestHandlers(
                settings,
                restController,
                clusterSettings,
                indexScopedSettings,
                settingsFilter,
                indexNameExpressionResolver,
                nodesInCluster
            )
        );
        return handlers;
    }

    public static Path resolveConfigFile(Environment env, String name) {
        Path config = env.configFile().resolve(name);
        if (Files.exists(config) == false) {
            Path legacyConfig = env.configFile().resolve("x-pack").resolve(name);
            if (Files.exists(legacyConfig)) {
                deprecationLogger.warn(
                    DeprecationCategory.OTHER,
                    "config_file_path",
                    "Config file ["
                        + name
                        + "] is in a deprecated location. Move from "
                        + legacyConfig.toString()
                        + " to "
                        + config.toString()
                );
                return legacyConfig;
            }
        }
        return config;
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap("source", SourceOnlySnapshotRepository.newRepositoryFactory());
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(SourceOnlySnapshotRepository.SOURCE_ONLY)
            && indexSettings.getIndexMetadata().isSearchableSnapshot() == false) {
            return Optional.of(SourceOnlySnapshotRepository.getEngineFactory());
        }

        return Optional.empty();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = super.getSettings();
        settings.add(SourceOnlySnapshotRepository.SOURCE_ONLY);

        // Don't register the license setting if there is an alternate implementation loaded as an extension.
        // this relies on the order in which methods are called - loadExtensions, (this method) getSettings, then createComponents
        if (getSharedLicenseService() == null) {
            settings.add(LicenseSettings.SELF_GENERATED_LICENSE_TYPE);
            settings.add(LicenseSettings.ALLOWED_LICENSE_TYPES_SETTING);
        }

        return settings;
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        if (DiscoveryNode.isStateless(settings)) {
            return List.of();
        }
        return Collections.singleton(DataTierAllocationDecider.INSTANCE);
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        if (DiscoveryNode.isStateless(settings)) {
            return List.of();
        }
        return Collections.singleton(new DataTier.DefaultHotAllocationSettingProvider());
    }

    /**
     * Handles the creation of the SSLService along with the necessary actions to enable reloading
     * of SSLContexts when configuration files change on disk.
     */
    private SSLService createSSLService(Environment environment, ResourceWatcherService resourceWatcherService) {
        final Map<String, SslConfiguration> sslConfigurations = SSLService.getSSLConfigurations(environment);
        // Must construct the reloader before the SSL service so that we don't miss any config changes, see #54867
        final SSLConfigurationReloader reloader = new SSLConfigurationReloader(resourceWatcherService, sslConfigurations.values());
        final SSLService sslService = new SSLService(environment, sslConfigurations);
        reloader.setSSLService(sslService);
        setSslService(sslService);
        return sslService;
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        List<MutableLicenseService> licenseServices = loader.loadExtensions(MutableLicenseService.class);
        if (licenseServices.size() > 1) {
            throw new IllegalStateException(MutableLicenseService.class + " may not have multiple implementations");
        } else if (licenseServices.size() == 1) {
            MutableLicenseService licenseService = licenseServices.get(0);
            logger.debug("Loaded implementation [{}] for interface MutableLicenseService", licenseService.getClass().getCanonicalName());
            setLicenseService(licenseService);
            setLicenseState(
                new XPackLicenseState(
                    () -> getEpochMillisSupplier().getAsLong(),
                    LicenseUtils.getXPackLicenseStatus(licenseService.getLicense(), getClock())
                )
            );
        } else {
            setLicenseState(
                new XPackLicenseState(
                    () -> getEpochMillisSupplier().getAsLong(),
                    new XPackLicenseStatus(License.OperationMode.TRIAL, true, null)
                )
            );
        }
    }
}
