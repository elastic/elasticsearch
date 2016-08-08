/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.Licensing;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.action.XPackInfoAction;
import org.elasticsearch.xpack.action.XPackUsageAction;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.common.http.auth.HttpAuthFactory;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuth;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuthFactory;
import org.elasticsearch.xpack.common.text.TextTemplateModule;
import org.elasticsearch.xpack.extensions.XPackExtension;
import org.elasticsearch.xpack.extensions.XPackExtensionsService;
import org.elasticsearch.xpack.graph.Graph;
import org.elasticsearch.xpack.graph.GraphFeatureSet;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.monitoring.MonitoringFeatureSet;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.notification.Notification;
import org.elasticsearch.xpack.notification.email.Account;
import org.elasticsearch.xpack.notification.email.support.BodyPartSource;
import org.elasticsearch.xpack.rest.action.RestXPackInfoAction;
import org.elasticsearch.xpack.rest.action.RestXPackUsageAction;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.SecurityFeatureSet;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.support.clock.Clock;
import org.elasticsearch.xpack.support.clock.SystemClock;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.WatcherFeatureSet;
import org.elasticsearch.xpack.watcher.support.WatcherScript;

public class XPackPlugin extends Plugin implements ScriptPlugin, ActionPlugin, IngestPlugin {

    public static final String NAME = "x-pack";

    /** Name constant for the security feature. */
    public static final String SECURITY = "security";

    /** Name constant for the monitoring feature. */
    public static final String MONITORING = "monitoring";

    /** Name constant for the watcher feature. */
    public static final String WATCHER = "watcher";

    /** Name constant for the graph feature. */
    public static final String GRAPH = "graph";

    // inside of YAML settings we still use xpack do not having handle issues with dashes
    private static final String SETTINGS_NAME = "xpack";

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
        // some classes need to have their own clinit blocks
        BodyPartSource.init();
        Account.init();
    }

    protected final Settings settings;
    private final Environment env;
    protected boolean transportClientMode;
    protected final XPackExtensionsService extensionsService;

    protected XPackLicenseState licenseState;
    protected Licensing licensing;
    protected Security security;
    protected Monitoring monitoring;
    protected Watcher watcher;
    protected Graph graph;
    protected Notification notification;

    public XPackPlugin(Settings settings) throws IOException {
        this.settings = settings;
        this.transportClientMode = transportClientMode(settings);
        this.env = transportClientMode ? null : new Environment(settings);
        this.licenseState = new XPackLicenseState();

        this.licensing = new Licensing(settings);
        this.security = new Security(settings, env, licenseState);
        this.monitoring = new Monitoring(settings, env, licenseState);
        this.watcher = new Watcher(settings);
        this.graph = new Graph(settings);
        this.notification = new Notification(settings);
        // Check if the node is a transport client.
        if (transportClientMode == false) {
            this.extensionsService = new XPackExtensionsService(settings, resolveXPackExtensionsFile(env), getExtensions());
        } else {
            this.extensionsService = null;
        }
    }

    // For tests only
    public Collection<Class<? extends XPackExtension>> getExtensions() {
        return Collections.emptyList();
    }

    // overridable by tests
    protected Clock getClock() {
        return SystemClock.INSTANCE;
    }

    @Override
    public Collection<Module> createGuiceModules() {
        ArrayList<Module> modules = new ArrayList<>();
        modules.add(b -> b.bind(Clock.class).toInstance(getClock()));
        modules.addAll(notification.nodeModules());
        modules.addAll(security.nodeModules());
        modules.addAll(watcher.nodeModules());
        modules.addAll(monitoring.nodeModules());
        modules.addAll(graph.createGuiceModules());

        if (transportClientMode == false) {
            modules.add(new TextTemplateModule());
        } else {
            modules.add(b -> b.bind(XPackLicenseState.class).toProvider(Providers.of(null)));
        }
        return modules;
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService) {
        List<Object> components = new ArrayList<>();
        final InternalClient internalClient = new InternalClient(settings, threadPool, client, security.getCryptoService());
        components.add(internalClient);

        LicenseService licenseService = new LicenseService(settings, clusterService, getClock(),
            env, resourceWatcherService, licenseState);
        components.add(licenseService);
        components.add(licenseState);

        components.addAll(security.createComponents(internalClient, threadPool, clusterService, resourceWatcherService,
                                                    extensionsService.getExtensions()));
        components.addAll(monitoring.createComponents(internalClient, threadPool, clusterService, licenseService));

        // watcher http stuff
        Map<String, HttpAuthFactory> httpAuthFactories = new HashMap<>();
        httpAuthFactories.put(BasicAuth.TYPE, new BasicAuthFactory(security.getCryptoService()));
        // TODO: add more auth types, or remove this indirection
        HttpAuthRegistry httpAuthRegistry = new HttpAuthRegistry(httpAuthFactories);
        components.add(new HttpRequestTemplate.Parser(httpAuthRegistry));
        components.add(new HttpClient(settings, httpAuthRegistry, env));

        return components;
    }

    @Override
    public Settings additionalSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(security.additionalSettings());
        builder.put(watcher.additionalSettings());
        return builder.build();
    }

    @Override
    public Collection<String> getRestHeaders() {
        if (transportClientMode) {
            return Collections.emptyList();
        }
        Set<String> headers = new HashSet<>();
        headers.add(UsernamePasswordToken.BASIC_AUTH_HEADER);
        if (AuthenticationService.RUN_AS_ENABLED.get(settings)) {
            headers.add(AuthenticationService.RUN_AS_USER_HEADER);
        }
        headers.addAll(extensionsService.getExtensions().stream()
            .flatMap(e -> e.getRestHeaders().stream()).collect(Collectors.toList()));
        return headers;
    }

    @Override
    public ScriptContext.Plugin getCustomScriptContexts() {
        return WatcherScript.CTX_PLUGIN;
    }

    @Override
    public List<Setting<?>> getSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.addAll(notification.getSettings());
        settings.addAll(security.getSettings());
        settings.addAll(MonitoringSettings.getSettings());
        settings.addAll(watcher.getSettings());
        settings.addAll(licensing.getSettings());

        settings.addAll(XPackSettings.getAllSettings());

        // we add the `xpack.version` setting to all internal indices
        settings.add(Setting.simpleString("index.xpack.version", Setting.Property.IndexScope));

        // http settings
        settings.add(Setting.simpleString("xpack.http.default_read_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.http.default_connection_timeout", Setting.Property.NodeScope));
        settings.add(Setting.groupSetting("xpack.http.ssl.", Setting.Property.NodeScope));
        settings.add(Setting.groupSetting("xpack.http.proxy.", Setting.Property.NodeScope));
        return settings;
    }

    @Override
    public List<String> getSettingsFilter() {
        List<String> filters = new ArrayList<>();
        filters.addAll(notification.getSettingsFilter());
        filters.addAll(security.getSettingsFilter());
        filters.addAll(MonitoringSettings.getSettingsFilter());
        return filters;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        return watcher.getExecutorBuilders(settings);
    }

    public void onModule(NetworkModule module) {
        security.onModule(module);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(XPackInfoAction.INSTANCE, TransportXPackInfoAction.class));
        actions.add(new ActionHandler<>(XPackUsageAction.INSTANCE, TransportXPackUsageAction.class));
        actions.addAll(licensing.getActions());
        actions.addAll(monitoring.getActions());
        actions.addAll(security.getActions());
        actions.addAll(watcher.getActions());
        actions.addAll(graph.getActions());
        return actions;
    }

    @Override
    public List<Class<? extends ActionFilter>> getActionFilters() {
        List<Class<? extends ActionFilter>> filters = new ArrayList<>();
        filters.addAll(licensing.getActionFilters());
        filters.addAll(monitoring.getActionFilters());
        filters.addAll(security.getActionFilters());
        filters.addAll(watcher.getActionFilters());
        return filters;
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        List<Class<? extends RestHandler>> handlers = new ArrayList<>();
        handlers.add(RestXPackInfoAction.class);
        handlers.add(RestXPackUsageAction.class);
        handlers.addAll(licensing.getRestHandlers());
        handlers.addAll(monitoring.getRestHandlers());
        handlers.addAll(security.getRestHandlers());
        handlers.addAll(watcher.getRestHandlers());
        handlers.addAll(graph.getRestHandlers());
        return handlers;
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return security.getProcessors(parameters);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, SECURITY, SecurityFeatureSet.Usage::new),
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, WATCHER, WatcherFeatureSet.Usage::new),
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, MONITORING, MonitoringFeatureSet.Usage::new),
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, GRAPH, GraphFeatureSet.Usage::new)
        );
    }

    public void onIndexModule(IndexModule module) {
        security.onIndexModule(module);
    }

    public static void bindFeatureSet(Binder binder, Class<? extends XPackFeatureSet> featureSet) {
        binder.bind(featureSet).asEagerSingleton();
        Multibinder<XPackFeatureSet> featureSetBinder = Multibinder.newSetBinder(binder, XPackFeatureSet.class);
        featureSetBinder.addBinding().to(featureSet);
    }

    public static boolean transportClientMode(Settings settings) {
        return TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
    }

    public static boolean isTribeNode(Settings settings) {
        return settings.getGroups("tribe", true).isEmpty() == false;
    }
    public static boolean isTribeClientNode(Settings settings) {
        return settings.get("tribe.name") != null;
    }

    public static Path resolveConfigFile(Environment env, String name) {
        return env.configFile().resolve(NAME).resolve(name);
    }

    public static String featureSettingPrefix(String featureName) {
        return SETTINGS_NAME + "." + featureName;
    }

    public static Path resolveXPackExtensionsFile(Environment env) {
        return env.pluginsFile().resolve(XPackPlugin.NAME).resolve("extensions");
    }
}
