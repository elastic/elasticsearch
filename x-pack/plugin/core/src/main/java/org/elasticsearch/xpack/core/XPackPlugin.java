/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensesMetaData;
import org.elasticsearch.license.Licensing;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.snapshots.SourceOnlySnapshotRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.action.ReloadAnalyzerAction;
import org.elasticsearch.xpack.core.action.TransportReloadAnalyzersAction;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.rest.action.RestReloadAnalyzersAction;
import org.elasticsearch.xpack.core.rest.action.RestXPackInfoAction;
import org.elasticsearch.xpack.core.rest.action.RestXPackUsageAction;
import org.elasticsearch.xpack.core.security.authc.TokenMetaData;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationReloader;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;

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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class XPackPlugin extends XPackClientPlugin implements ExtensiblePlugin, RepositoryPlugin, EnginePlugin {

    private static Logger logger = LogManager.getLogger(XPackPlugin.class);
    private static DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public static final String XPACK_INSTALLED_NODE_ATTR = "xpack.installed";

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
    //private final Environment env;
    protected final Licensing licensing;
    // These should not be directly accessed as they cannot be overridden in tests. Please use the getters so they can be overridden.
    private static final SetOnce<XPackLicenseState> licenseState = new SetOnce<>();
    private static final SetOnce<SSLService> sslService = new SetOnce<>();
    private static final SetOnce<LicenseService> licenseService = new SetOnce<>();

    public XPackPlugin(
            final Settings settings,
            final Path configPath) {
        super(settings);
        this.settings = settings;
        Environment env = new Environment(settings, configPath);

        setSslService(new SSLService(settings, env));
        setLicenseState(new XPackLicenseState(settings));

        this.licensing = new Licensing(settings);
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    protected SSLService getSslService() { return getSharedSslService(); }
    protected LicenseService getLicenseService() { return getSharedLicenseService(); }
    protected XPackLicenseState getLicenseState() { return getSharedLicenseState(); }
    protected void setSslService(SSLService sslService) { XPackPlugin.sslService.set(sslService); }
    protected void setLicenseService(LicenseService licenseService) { XPackPlugin.licenseService.set(licenseService); }
    protected void setLicenseState(XPackLicenseState licenseState) { XPackPlugin.licenseState.set(licenseState); }
    public static SSLService getSharedSslService() { return sslService.get(); }
    public static LicenseService getSharedLicenseService() { return licenseService.get(); }
    public static XPackLicenseState getSharedLicenseState() { return licenseState.get(); }

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
        final List<DiscoveryNode> notReadyNodes = StreamSupport.stream(clusterState.nodes().spliterator(), false).filter(node -> {
            final String xpackInstalledAttr = node.getAttributes().getOrDefault(XPACK_INSTALLED_NODE_ATTR, "false");
            return Booleans.parseBoolean(xpackInstalledAttr) == false;
        }).collect(Collectors.toList());

        return notReadyNodes;
    }

    private static boolean alreadyContainsXPackCustomMetadata(ClusterState clusterState) {
        final MetaData metaData = clusterState.metaData();
        return metaData.custom(LicensesMetaData.TYPE) != null ||
            metaData.custom(MlMetadata.TYPE) != null ||
            metaData.custom(WatcherMetaData.TYPE) != null ||
            clusterState.custom(TokenMetaData.TYPE) != null;
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
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        List<Object> components = new ArrayList<>();

        // just create the reloader as it will pull all of the loaded ssl configurations and start watching them
        new SSLConfigurationReloader(environment, getSslService(), resourceWatcherService);

        setLicenseService(new LicenseService(settings, clusterService, getClock(),
                environment, resourceWatcherService, getLicenseState()));

        // It is useful to override these as they are what guice is injecting into actions
        components.add(getSslService());
        components.add(getLicenseService());
        components.add(getLicenseState());

        return components;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(XPackInfoAction.INSTANCE, getInfoAction()));
        actions.add(new ActionHandler<>(XPackUsageAction.INSTANCE, getUsageAction()));
        actions.addAll(licensing.getActions());
        actions.add(new ActionHandler<>(ReloadAnalyzerAction.INSTANCE, TransportReloadAnalyzersAction.class));
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
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new RestXPackInfoAction(restController));
        handlers.add(new RestXPackUsageAction(restController));
        handlers.add(new RestReloadAnalyzersAction(restController));
        handlers.addAll(licensing.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings, settingsFilter,
                indexNameExpressionResolver, nodesInCluster));
        return handlers;
    }

    public static void bindFeatureSet(Binder binder, Class<? extends XPackFeatureSet> featureSet) {
        Multibinder<XPackFeatureSet> featureSetBinder = createFeatureSetMultiBinder(binder, featureSet);
        featureSetBinder.addBinding().to(featureSet);
    }

    public static Multibinder<XPackFeatureSet> createFeatureSetMultiBinder(Binder binder, Class<? extends XPackFeatureSet> featureSet) {
        binder.bind(featureSet).asEagerSingleton();
        return Multibinder.newSetBinder(binder, XPackFeatureSet.class);
    }

    public static Path resolveConfigFile(Environment env, String name) {
        Path config =  env.configFile().resolve(name);
        if (Files.exists(config) == false) {
            Path legacyConfig = env.configFile().resolve("x-pack").resolve(name);
            if (Files.exists(legacyConfig)) {
                deprecationLogger.deprecated("Config file [" + name + "] is in a deprecated location. Move from " +
                    legacyConfig.toString() + " to " + config.toString());
                return legacyConfig;
            }
        }
        return config;
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ThreadPool threadPool) {
        return Collections.singletonMap("source", SourceOnlySnapshotRepository.newRepositoryFactory());
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(SourceOnlySnapshotRepository.SOURCE_ONLY)) {
            return Optional.of(SourceOnlySnapshotRepository.getEngineFactory());
        }

        return Optional.empty();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = super.getSettings();
        settings.add(SourceOnlySnapshotRepository.SOURCE_ONLY);
        return settings;
    }
}
