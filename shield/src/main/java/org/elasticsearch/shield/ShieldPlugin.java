/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerModule;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.shield.action.ShieldActionFilter;
import org.elasticsearch.shield.action.ShieldActionModule;
import org.elasticsearch.shield.action.authc.cache.ClearRealmCacheAction;
import org.elasticsearch.shield.action.authc.cache.TransportClearRealmCacheAction;
import org.elasticsearch.shield.audit.AuditTrailModule;
import org.elasticsearch.shield.audit.index.IndexAuditUserHolder;
import org.elasticsearch.shield.authc.AuthenticationModule;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.authz.AuthorizationModule;
import org.elasticsearch.shield.authz.accesscontrol.OptOutQueryCache;
import org.elasticsearch.shield.authz.accesscontrol.ShieldIndexSearcherWrapper;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.crypto.CryptoModule;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.shield.license.LicenseModule;
import org.elasticsearch.shield.license.ShieldLicenseState;
import org.elasticsearch.shield.license.ShieldLicensee;
import org.elasticsearch.shield.rest.ShieldRestModule;
import org.elasticsearch.shield.rest.action.RestShieldInfoAction;
import org.elasticsearch.shield.rest.action.authc.cache.RestClearRealmCacheAction;
import org.elasticsearch.shield.ssl.SSLModule;
import org.elasticsearch.shield.transport.ShieldClientTransportService;
import org.elasticsearch.shield.transport.ShieldServerTransportService;
import org.elasticsearch.shield.transport.ShieldTransportModule;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.shield.transport.netty.ShieldNettyHttpServerTransport;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.transport.TransportModule;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.*;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 *
 */
public class ShieldPlugin extends Plugin {

    public static final String NAME = "shield";
    public static final String ENABLED_SETTING_NAME = NAME + ".enabled";
    public static final String OPT_OUT_QUERY_CACHE = "opt_out_cache";
    public static final String DLS_FLS_ENABLED_SETTING = "shield.dls_fls.enabled";

    private static final boolean DEFAULT_ENABLED_SETTING = true;

    private final Settings settings;
    private final boolean enabled;
    private final boolean clientMode;
    private ShieldLicenseState shieldLicenseState;

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
        // TODO: fix gradle to add all shield resources (plugin metadata) to test classpath
        // of watcher plugin, which depends on it directly. This prevents these plugins
        // from being initialized correctly by the test framework, and means we have to
        // have this leniency.
        } catch (ExceptionInInitializerError bogus) {
            if (bogus.getCause() instanceof SecurityException == false) {
                throw bogus; // some other bug
            }
        }
    }

    public ShieldPlugin(Settings settings) {
        this.settings = settings;
        this.enabled = shieldEnabled(settings);
        this.clientMode = clientMode(settings);
        if (enabled && clientMode == false) {
            failIfShieldQueryCacheIsNotActive(settings, true);
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch Shield (security)";
    }

    @Override
    public Collection<Module> nodeModules() {
        if (enabled == false) {
            return Collections.<Module>singletonList(new ShieldDisabledModule(settings));
        } else if (clientMode) {
            return Arrays.<Module>asList(
                    new ShieldTransportModule(settings),
                    new SSLModule(settings));
        } else {
            // we can't load that at construction time since the license plugin might not have been loaded at that point
            // which might not be the case during Plugin class instantiation. Once nodeModules are pulled
            // everything should have been loaded
            shieldLicenseState = new ShieldLicenseState();
            return Arrays.<Module>asList(
                    new ShieldModule(settings),
                    new LicenseModule(settings, shieldLicenseState),
                    new CryptoModule(settings),
                    new AuthenticationModule(settings),
                    new AuthorizationModule(settings),
                    new AuditTrailModule(settings),
                    new ShieldRestModule(settings),
                    new ShieldActionModule(settings),
                    new ShieldTransportModule(settings),
                    new SSLModule(settings));
        }
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (enabled && clientMode == false) {
            return Arrays.<Class<? extends LifecycleComponent>>asList(ShieldLicensee.class, InternalCryptoService.class, FileRolesStore.class, Realms.class, IPFilter.class);
        }
        return Collections.emptyList();
    }

    @Override
    public Settings additionalSettings() {
        if (enabled == false) {
            return Settings.EMPTY;
        }

        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        addUserSettings(settingsBuilder);
        addTribeSettings(settingsBuilder);
        addQueryCacheSettings(settingsBuilder);
        return settingsBuilder.build();
    }

    public void onModule(ClusterModule clusterDynamicSettingsModule) {
        clusterDynamicSettingsModule.registerClusterDynamicSetting("shield.transport.filter.*", Validator.EMPTY);
        clusterDynamicSettingsModule.registerClusterDynamicSetting("shield.http.filter.*", Validator.EMPTY);
        clusterDynamicSettingsModule.registerClusterDynamicSetting("transport.profiles.*", Validator.EMPTY);
        clusterDynamicSettingsModule.registerClusterDynamicSetting(IPFilter.IP_FILTER_ENABLED_SETTING, Validator.EMPTY);
        clusterDynamicSettingsModule.registerClusterDynamicSetting(IPFilter.IP_FILTER_ENABLED_HTTP_SETTING, Validator.EMPTY);
    }

    @Override
    public void onIndexService(IndexService indexService) {
        if (enabled && clientMode == false) {
            failIfShieldQueryCacheIsNotActive(indexService.getIndexSettings().getSettings(), false);
        }
    }

    @Override
    public void onIndexModule(IndexModule module) {
        if (enabled == false) {
            return;
        }
        assert shieldLicenseState != null;
        if (flsDlsEnabled(settings)) {
            module.setSearcherWrapper((indexService) -> new ShieldIndexSearcherWrapper(indexService.getIndexSettings(),
                    indexService.getQueryShardContext(), indexService.mapperService(),
                    indexService.cache().bitsetFilterCache(), shieldLicenseState));
        }
        if (clientMode == false) {
            module.registerQueryCache(ShieldPlugin.OPT_OUT_QUERY_CACHE, OptOutQueryCache::new);
        }
    }

    public void onModule(ActionModule module) {
        if (enabled == false) {
            return;
        }
        // registering the security filter only for nodes
        if (clientMode == false) {
            module.registerFilter(ShieldActionFilter.class);
        }

        // registering all shield actions
        module.registerAction(ClearRealmCacheAction.INSTANCE, TransportClearRealmCacheAction.class);
    }

    public void onModule(TransportModule module) {
        if (enabled == false) {
            return;
        }
        module.setTransport(ShieldNettyTransport.class, ShieldPlugin.NAME);
        if (clientMode) {
            module.setTransportService(ShieldClientTransportService.class, ShieldPlugin.NAME);
        } else {
            module.setTransportService(ShieldServerTransportService.class, ShieldPlugin.NAME);
        }
    }

    public void onModule(HttpServerModule module) {
        if (enabled && clientMode == false) {
            module.setHttpServerTransport(ShieldNettyHttpServerTransport.class, ShieldPlugin.NAME);
        }
    }

    public void onModule(RestModule module) {
        if (enabled && clientMode == false) {
            module.addRestAction(RestClearRealmCacheAction.class);
        }
        // we want to expose the shield rest action even when the plugin is disabled
        module.addRestAction(RestShieldInfoAction.class);
    }

    public void onModule(AuthorizationModule module) {
        if (enabled && AuditTrailModule.auditingEnabled(settings)) {
            module.registerReservedRole(IndexAuditUserHolder.ROLE);
        }
    }

    private void addUserSettings(Settings.Builder settingsBuilder) {
        String authHeaderSettingName = Headers.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER;
        if (settings.get(authHeaderSettingName) != null) {
            return;
        }
        String userSetting = settings.get("shield.user");
        if (userSetting == null) {
            return;
        }
        int i = userSetting.indexOf(":");
        if (i < 0 || i == userSetting.length() - 1) {
            throw new IllegalArgumentException("invalid [shield.user] setting. must be in the form of \"<username>:<password>\"");
        }
        String username = userSetting.substring(0, i);
        String password = userSetting.substring(i + 1);
        settingsBuilder.put(authHeaderSettingName, UsernamePasswordToken.basicAuthHeaderValue(username, new SecuredString(password.toCharArray())));
    }

    /*
     We inject additional settings on each tribe client if the current node is a tribe node, to make sure that every tribe has shield installed and enabled too:
     - if shield is loaded on the tribe node we make sure it is also loaded on every tribe, by making it mandatory there
     (this means that the tribe node will fail at startup if shield is not loaded on any tribe due to missing mandatory plugin)
     - if shield is loaded and enabled on the tribe node, we make sure it is also enabled on every tribe, by forcibly enabling it
       (that means it's not possible to disable shield on the tribe clients)
     */
    private void addTribeSettings(Settings.Builder settingsBuilder) {
        Map<String, Settings> tribesSettings = settings.getGroups("tribe", true);
        if (tribesSettings.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Settings> tribeSettings : tribesSettings.entrySet()) {
            String tribePrefix = "tribe." + tribeSettings.getKey() + ".";

            //we copy over existing mandatory plugins under additional settings, as they would get overridden otherwise (arrays don't get merged)
            String[] existingMandatoryPlugins = tribeSettings.getValue().getAsArray("plugin.mandatory", null);
            if (existingMandatoryPlugins == null) {
                //shield is mandatory on every tribe if installed and enabled on the tribe node
                settingsBuilder.putArray(tribePrefix + "plugin.mandatory", NAME);
            } else {
                if (!isShieldMandatory(existingMandatoryPlugins)) {
                    throw new IllegalStateException("when [plugin.mandatory] is explicitly configured, [" + NAME + "] must be included in this list");
                }
            }

            final String tribeEnabledSetting = tribePrefix + ENABLED_SETTING_NAME;
            if (settings.get(tribeEnabledSetting) != null) {
                boolean enabled = shieldEnabled(tribeSettings.getValue());
                if (!enabled) {
                    throw new IllegalStateException("tribe setting [" + tribeEnabledSetting + "] must be set to true but the value is [" + settings.get(tribeEnabledSetting) + "]");
                }
            } else {
                //shield must be enabled on every tribe if it's enabled on the tribe node
                settingsBuilder.put(tribeEnabledSetting, true);
            }
        }
    }

    /*
        We need to forcefully overwrite the query cache implementation to use Shield's opt out query cache implementation.
        This impl. disabled the query cache if field level security is used for a particular request. If we wouldn't do
        forcefully overwrite the query cache implementation then we leave the system vulnerable to leakages of data to
        unauthorized users.
     */
    private void addQueryCacheSettings(Settings.Builder settingsBuilder) {
        settingsBuilder.put(IndexModule.QUERY_CACHE_TYPE, OPT_OUT_QUERY_CACHE);
    }

    private static boolean isShieldMandatory(String[] existingMandatoryPlugins) {
        for (String existingMandatoryPlugin : existingMandatoryPlugins) {
            if (NAME.equals(existingMandatoryPlugin)) {
                return true;
            }
        }
        return false;
    }

    public static Path configDir(Environment env) {
        return env.configFile().resolve(NAME);
    }

    public static Path resolveConfigFile(Environment env, String name) {
        return configDir(env).resolve(name);
    }

    public static boolean clientMode(Settings settings) {
        return !"node".equals(settings.get(Client.CLIENT_TYPE_SETTING));
    }

    public static boolean shieldEnabled(Settings settings) {
        return settings.getAsBoolean(ENABLED_SETTING_NAME, DEFAULT_ENABLED_SETTING);
    }

    public static boolean flsDlsEnabled(Settings settings) {
        return settings.getAsBoolean(DLS_FLS_ENABLED_SETTING, false);
    }

    private void failIfShieldQueryCacheIsNotActive(Settings settings, boolean nodeSettings) {
        String queryCacheImplementation;
        if (nodeSettings) {
            // in case this are node settings then the plugin additional settings have not been applied yet,
            // so we use 'opt_out_cache' as default. So in that case we only fail if the node settings contain
            // another cache impl than 'opt_out_cache'.
            queryCacheImplementation = settings.get(IndexModule.QUERY_CACHE_TYPE, OPT_OUT_QUERY_CACHE);
        } else {
            queryCacheImplementation = settings.get(IndexModule.QUERY_CACHE_TYPE);
        }
        if (OPT_OUT_QUERY_CACHE.equals(queryCacheImplementation) == false) {
            throw new IllegalStateException("shield does not support a user specified query cache. remove the setting [" + IndexModule.QUERY_CACHE_TYPE + "] with value [" + queryCacheImplementation + "]");
        }
    }
}
