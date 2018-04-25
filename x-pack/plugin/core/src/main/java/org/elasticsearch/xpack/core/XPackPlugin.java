/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.bouncycastle.operator.OperatorCreationException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.Licensing;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.rest.action.RestXPackInfoAction;
import org.elasticsearch.xpack.core.rest.action.RestXPackUsageAction;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationReloader;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.security.auth.DestroyFailedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class XPackPlugin extends XPackClientPlugin implements ScriptPlugin, ExtensiblePlugin {

    private static Logger logger = ESLoggerFactory.getLogger(XPackPlugin.class);
    private static DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

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
    protected boolean transportClientMode;
    protected final Licensing licensing;
    // These should not be directly accessed as they cannot be overriden in tests. Please use the getters so they can be overridden.
    private static final SetOnce<XPackLicenseState> licenseState = new SetOnce<>();
    private static final SetOnce<SSLService> sslService = new SetOnce<>();
    private static final SetOnce<LicenseService> licenseService = new SetOnce<>();

    public XPackPlugin(
            final Settings settings,
            final Path configPath) throws IOException, DestroyFailedException, OperatorCreationException, GeneralSecurityException {
        super(settings);
        this.settings = settings;
        this.transportClientMode = transportClientMode(settings);
        Environment env = transportClientMode ? null : new Environment(settings, configPath);

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

    @Override
    public Collection<Module> createGuiceModules() {
        ArrayList<Module> modules = new ArrayList<>();
        //modules.add(b -> b.bind(Clock.class).toInstance(getClock()));
        // used to get core up and running, we do not bind the actual feature set here
        modules.add(b -> XPackPlugin.createFeatureSetMultiBinder(b, EmptyXPackFeatureSet.class));

        if (transportClientMode) {
            modules.add(b -> b.bind(XPackLicenseState.class).toProvider(Providers.of(null)));
        }
        return modules;
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        List<Object> components = new ArrayList<>();

        // just create the reloader as it will pull all of the loaded ssl configurations and start watching them
        new SSLConfigurationReloader(settings, environment, getSslService(), resourceWatcherService);

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
        actions.add(new ActionHandler<>(XPackInfoAction.INSTANCE, TransportXPackInfoAction.class));
        actions.add(new ActionHandler<>(XPackUsageAction.INSTANCE, TransportXPackUsageAction.class));
        actions.addAll(licensing.getActions());
        return actions;
    }

    @Override
    public List<GenericAction> getClientActions() {
        List<GenericAction> actions = new ArrayList<>();
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
        handlers.add(new RestXPackInfoAction(settings, restController));
        handlers.add(new RestXPackUsageAction(settings, restController));
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

    public static boolean transportClientMode(Settings settings) {
        return TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
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
}
