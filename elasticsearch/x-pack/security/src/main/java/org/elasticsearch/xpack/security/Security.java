/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.action.SecurityActionModule;
import org.elasticsearch.xpack.security.action.filter.SecurityActionFilter;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.realm.TransportClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.security.action.role.TransportClearRolesCacheAction;
import org.elasticsearch.xpack.security.action.role.TransportDeleteRoleAction;
import org.elasticsearch.xpack.security.action.role.TransportGetRolesAction;
import org.elasticsearch.xpack.security.action.role.TransportPutRoleAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.security.action.user.PutUserAction;
import org.elasticsearch.xpack.security.action.user.TransportAuthenticateAction;
import org.elasticsearch.xpack.security.action.user.TransportChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.TransportDeleteUserAction;
import org.elasticsearch.xpack.security.action.user.TransportGetUsersAction;
import org.elasticsearch.xpack.security.action.user.TransportPutUserAction;
import org.elasticsearch.xpack.security.audit.AuditTrailModule;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.audit.index.IndexNameResolver;
import org.elasticsearch.xpack.security.authc.AuthenticationModule;
import org.elasticsearch.xpack.security.authc.InternalAuthenticationService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authz.AuthorizationModule;
import org.elasticsearch.xpack.security.authz.InternalAuthorizationService;
import org.elasticsearch.xpack.security.authz.accesscontrol.OptOutQueryCache;
import org.elasticsearch.xpack.security.authz.accesscontrol.SecurityIndexSearcherWrapper;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.rest.SecurityRestModule;
import org.elasticsearch.xpack.security.rest.action.RestAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.realm.RestClearRealmCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestClearRolesCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestDeleteRoleAction;
import org.elasticsearch.xpack.security.rest.action.role.RestGetRolesAction;
import org.elasticsearch.xpack.security.rest.action.role.RestPutRoleAction;
import org.elasticsearch.xpack.security.rest.action.user.RestChangePasswordAction;
import org.elasticsearch.xpack.security.rest.action.user.RestDeleteUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestGetUsersAction;
import org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration;
import org.elasticsearch.xpack.security.ssl.SSLModule;
import org.elasticsearch.xpack.security.support.OptionalSettings;
import org.elasticsearch.xpack.security.transport.SecurityClientTransportService;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportService;
import org.elasticsearch.xpack.security.transport.SecurityTransportModule;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3HttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 *
 */
public class Security implements ActionPlugin {

    private static final ESLogger logger = Loggers.getLogger(XPackPlugin.class);

    public static final String NAME = "security";
    public static final String DLS_FLS_FEATURE = "security.dls_fls";
    public static final Setting<Optional<String>> USER_SETTING = OptionalSettings.createString(setting("user"), Property.NodeScope);

    private final Settings settings;
    private final boolean enabled;
    private final boolean transportClientMode;
    private final SecurityLicenseState securityLicenseState;
    private final CryptoService cryptoService;

    public Security(Settings settings, Environment env) throws IOException {
        this.settings = settings;
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        this.enabled = XPackPlugin.featureEnabled(settings, NAME, true);
        if (enabled && transportClientMode == false) {
            validateAutoCreateIndex(settings);
            cryptoService = new CryptoService(settings, env);
        } else {
            cryptoService = null;
        }
        securityLicenseState = new SecurityLicenseState();
    }

    public CryptoService getCryptoService() {
        return cryptoService;
    }

    public SecurityLicenseState getSecurityLicenseState() {
        return securityLicenseState;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            if (enabled == false) {
                return modules;
            }
            modules.add(new SecurityModule(settings));
            modules.add(new SecurityTransportModule(settings));
            modules.add(new SSLModule(settings));
            return modules;
        }

        modules.add(new AuthenticationModule(settings));
        modules.add(new AuthorizationModule(settings));
        if (enabled == false) {
            modules.add(b -> b.bind(CryptoService.class).toProvider(Providers.of(null)));
            modules.add(new SecurityModule(settings));
            modules.add(new AuditTrailModule(settings));
            modules.add(new SecurityTransportModule(settings));
            return modules;
        }

        // we can't load that at construction time since the license plugin might not have been loaded at that point
        // which might not be the case during Plugin class instantiation. Once nodeModules are pulled
        // everything should have been loaded

        modules.add(b -> b.bind(CryptoService.class).toInstance(cryptoService));
        modules.add(new SecurityModule(settings));
        modules.add(new AuditTrailModule(settings));
        modules.add(new SecurityRestModule(settings));
        modules.add(new SecurityActionModule(settings));
        modules.add(new SecurityTransportModule(settings));
        modules.add(new SSLModule(settings));
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (enabled == false || transportClientMode == true) {
            return Collections.emptyList();
        }
        List<Class<? extends LifecycleComponent>> list = new ArrayList<>();
        list.add(FileRolesStore.class);
        list.add(Realms.class);
        return list;
    }

    public Settings additionalSettings() {
        if (enabled == false) {
            return Settings.EMPTY;
        }

        return additionalSettings(settings);
    }

    // pkg private for testing
    static Settings additionalSettings(Settings settings) {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME);
        settingsBuilder.put(NetworkModule.TRANSPORT_SERVICE_TYPE_KEY, Security.NAME);
        settingsBuilder.put(NetworkModule.HTTP_TYPE_SETTING.getKey(), Security.NAME);
        SecurityNetty3HttpServerTransport.overrideSettings(settingsBuilder, settings);
        addUserSettings(settings, settingsBuilder);
        addTribeSettings(settings, settingsBuilder);
        return settingsBuilder.build();
    }

    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingsList = new ArrayList<>();
        // always register for both client and node modes
        XPackPlugin.addFeatureEnabledSettings(settingsList, NAME, true);
        settingsList.add(USER_SETTING);

        // SSL settings
        SSLConfiguration.Global.addSettings(settingsList);

        // transport settings
        SecurityNetty3Transport.addSettings(settingsList);

        if (transportClientMode) {
            return settingsList;
        }

        // The following just apply in node mode
        XPackPlugin.addFeatureEnabledSettings(settingsList, DLS_FLS_FEATURE, true);

        // IP Filter settings
        IPFilter.addSettings(settingsList);

        // audit settings
        AuditTrailModule.addSettings(settingsList);

        // authentication settings
        FileRolesStore.addSettings(settingsList);
        AnonymousUser.addSettings(settingsList);
        Realms.addSettings(settingsList);
        NativeUsersStore.addSettings(settingsList);
        NativeRolesStore.addSettings(settingsList);
        InternalAuthenticationService.addSettings(settingsList);
        InternalAuthorizationService.addSettings(settingsList);

        // HTTP settings
        SecurityNetty3HttpServerTransport.addSettings(settingsList);

        // encryption settings
        CryptoService.addSettings(settingsList);

        // hide settings
        settingsList.add(Setting.listSetting(setting("hide_settings"), Collections.emptyList(), Function.identity(),
                Property.NodeScope, Property.Filtered));
        return settingsList;
    }

    
    public List<String> getSettingsFilter() {
        ArrayList<String> settingsFilter = new ArrayList<>();
        String[] asArray = settings.getAsArray(setting("hide_settings"));
        for (String pattern : asArray) {
            settingsFilter.add(pattern);
        }

        settingsFilter.add(setting("authc.realms.*.bind_dn"));
        settingsFilter.add(setting("authc.realms.*.bind_password"));
        settingsFilter.add(setting("authc.realms.*." + SessionFactory.HOSTNAME_VERIFICATION_SETTING));
        settingsFilter.add(setting("authc.realms.*.truststore.password"));
        settingsFilter.add(setting("authc.realms.*.truststore.path"));
        settingsFilter.add(setting("authc.realms.*.truststore.algorithm"));

        // hide settings where we don't define them - they are part of a group...
        settingsFilter.add("transport.profiles.*." + setting("*"));
        return settingsFilter;
    }

    public void onIndexModule(IndexModule module) {
        if (enabled == false) {
            return;
        }

        assert securityLicenseState != null;
        if (flsDlsEnabled(settings)) {
            module.setSearcherWrapper((indexService) -> new SecurityIndexSearcherWrapper(indexService.getIndexSettings(),
                    indexService.newQueryShardContext(), indexService.mapperService(),
                    indexService.cache().bitsetFilterCache(), indexService.getIndexServices().getThreadPool().getThreadContext(),
                    securityLicenseState));
        }
        if (transportClientMode == false) {
            /*  We need to forcefully overwrite the query cache implementation to use security's opt out query cache implementation.
             *  This impl. disabled the query cache if field level security is used for a particular request. If we wouldn't do
             *  forcefully overwrite the query cache implementation then we leave the system vulnerable to leakages of data to
             *  unauthorized users. */
            module.forceQueryCacheProvider(OptOutQueryCache::new);
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return emptyList();
        }
        return Arrays.asList(new ActionHandler<>(ClearRealmCacheAction.INSTANCE, TransportClearRealmCacheAction.class),
                new ActionHandler<>(ClearRolesCacheAction.INSTANCE, TransportClearRolesCacheAction.class),
                new ActionHandler<>(GetUsersAction.INSTANCE, TransportGetUsersAction.class),
                new ActionHandler<>(PutUserAction.INSTANCE, TransportPutUserAction.class),
                new ActionHandler<>(DeleteUserAction.INSTANCE, TransportDeleteUserAction.class),
                new ActionHandler<>(GetRolesAction.INSTANCE, TransportGetRolesAction.class),
                new ActionHandler<>(PutRoleAction.INSTANCE, TransportPutRoleAction.class),
                new ActionHandler<>(DeleteRoleAction.INSTANCE, TransportDeleteRoleAction.class),
                new ActionHandler<>(ChangePasswordAction.INSTANCE, TransportChangePasswordAction.class),
                new ActionHandler<>(AuthenticateAction.INSTANCE, TransportAuthenticateAction.class));
    }

    @Override
    public List<Class<? extends ActionFilter>> getActionFilters() {
        if (enabled == false) {
            return emptyList();
        }
        // registering the security filter only for nodes
        if (transportClientMode == false) {
            return singletonList(SecurityActionFilter.class);
        }
        return emptyList();
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        if (enabled == false) {
            return emptyList();
        }
        return Arrays.asList(RestAuthenticateAction.class,
                RestClearRealmCacheAction.class,
                RestClearRolesCacheAction.class,
                RestGetUsersAction.class,
                RestPutUserAction.class,
                RestDeleteUserAction.class,
                RestGetRolesAction.class,
                RestPutRoleAction.class,
                RestDeleteRoleAction.class,
                RestChangePasswordAction.class);
    }

    public void onModule(NetworkModule module) {

        if (transportClientMode) {
            if (enabled) {
                module.registerTransport(Security.NAME, SecurityNetty3Transport.class);
                module.registerTransportService(Security.NAME, SecurityClientTransportService.class);
            }
            return;
        }

        if (enabled) {
            module.registerTransport(Security.NAME, SecurityNetty3Transport.class);
            module.registerTransportService(Security.NAME, SecurityServerTransportService.class);
            module.registerHttpTransport(Security.NAME, SecurityNetty3HttpServerTransport.class);
        }
    }

    private static void addUserSettings(Settings settings, Settings.Builder settingsBuilder) {
        String authHeaderSettingName = ThreadContext.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER;
        if (settings.get(authHeaderSettingName) != null) {
            return;
        }
        Optional<String> userOptional = USER_SETTING.get(settings);
        userOptional.ifPresent(userSetting -> {
            final int i = userSetting.indexOf(":");
            if (i < 0 || i == userSetting.length() - 1) {
                throw new IllegalArgumentException("invalid [" + USER_SETTING.getKey() + "] setting. must be in the form of " +
                        "\"<username>:<password>\"");
            }
            String username = userSetting.substring(0, i);
            String password = userSetting.substring(i + 1);
            settingsBuilder.put(authHeaderSettingName, UsernamePasswordToken.basicAuthHeaderValue(username, new SecuredString(password
                    .toCharArray())));
        });
    }

    /**
     * If the current node is a tribe node, we inject additional settings on each tribe client. We do this to make sure
     * that every tribe cluster has x-pack installed and security is enabled. We do that by:
     *
     *    - making it mandatory on the tribe client (this means that the tribe node will fail at startup if x-pack is
     *      not loaded on any tribe due to missing mandatory plugin)
     *
     *    - forcibly enabling it (that means it's not possible to disable security on the tribe clients)
     */
    private static void addTribeSettings(Settings settings, Settings.Builder settingsBuilder) {
        Map<String, Settings> tribesSettings = settings.getGroups("tribe", true);
        if (tribesSettings.isEmpty()) {
            // it's not a tribe node
            return;
        }

        final Map<String, String> settingsMap = settings.getAsMap();
        for (Map.Entry<String, Settings> tribeSettings : tribesSettings.entrySet()) {
            String tribePrefix = "tribe." + tribeSettings.getKey() + ".";

            // we copy over existing mandatory plugins under additional settings, as they would get overridden
            // otherwise (arrays don't get merged)
            String[] existingMandatoryPlugins = tribeSettings.getValue().getAsArray("plugin.mandatory", null);
            if (existingMandatoryPlugins == null) {
                //x-pack is mandatory on every tribe if installed and enabled on the tribe node
                settingsBuilder.putArray(tribePrefix + "plugin.mandatory", XPackPlugin.NAME);
            } else {
                if (Arrays.binarySearch(existingMandatoryPlugins, XPackPlugin.NAME) < 0) {
                    throw new IllegalStateException("when [plugin.mandatory] is explicitly configured, [" +
                            XPackPlugin.NAME + "] must be included in this list");
                }
            }

            final String tribeEnabledSetting = tribePrefix + XPackPlugin.featureEnabledSetting(NAME);
            if (settings.get(tribeEnabledSetting) != null) {
                boolean enabled = enabled(tribeSettings.getValue());
                if (!enabled) {
                    throw new IllegalStateException("tribe setting [" + tribeEnabledSetting + "] must be set to true but the value is ["
                            + settings.get(tribeEnabledSetting) + "]");
                }
            } else {
                //x-pack security must be enabled on every tribe if it's enabled on the tribe node
                settingsBuilder.put(tribeEnabledSetting, true);
            }

            // we passed all the checks now we need to copy in all of the x-pack security settings
            for (Map.Entry<String, String> entry : settingsMap.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("xpack.security.")) {
                    settingsBuilder.put(tribePrefix + key, entry.getValue());
                }
            }
        }
    }

    public static boolean enabled(Settings settings) {
        return XPackPlugin.featureEnabled(settings, NAME, true);
    }

    public static boolean flsDlsEnabled(Settings settings) {
        return XPackPlugin.featureEnabled(settings, DLS_FLS_FEATURE, true);
    }

    public static String enabledSetting() {
        return XPackPlugin.featureEnabledSetting(NAME);
    }

    public static String settingPrefix() {
        return XPackPlugin.featureSettingPrefix(NAME) + ".";
    }

    public static String setting(String setting) {
        assert setting != null && setting.startsWith(".") == false;
        return settingPrefix() + setting;
    }

    public static String featureEnabledSetting(String feature) {
        assert feature != null && feature.startsWith(".") == false;
        return XPackPlugin.featureEnabledSetting("security." + feature);
    }

    static void validateAutoCreateIndex(Settings settings) {
        String value = settings.get("action.auto_create_index");
        if (value == null) {
            return;
        }

        final boolean indexAuditingEnabled = AuditTrailModule.indexAuditLoggingEnabled(settings);
        final String auditIndex = indexAuditingEnabled ? "," + IndexAuditTrail.INDEX_NAME_PREFIX + "*" : "";
        String errorMessage = LoggerMessageFormat.format("the [action.auto_create_index] setting value [{}] is too" +
                " restrictive. disable [action.auto_create_index] or set it to " +
                "[{}{}]", (Object) value, SecurityTemplateService.SECURITY_INDEX_NAME, auditIndex);
        if (Booleans.isExplicitFalse(value)) {
            throw new IllegalArgumentException(errorMessage);
        }

        if (Booleans.isExplicitTrue(value)) {
            return;
        }

        String[] matches = Strings.commaDelimitedListToStringArray(value);
        List<String> indices = new ArrayList<>();
        indices.add(SecurityTemplateService.SECURITY_INDEX_NAME);
        if (indexAuditingEnabled) {
            DateTime now = new DateTime(DateTimeZone.UTC);
            // just use daily rollover
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now, IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusDays(1), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(1), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(2), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(3), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(4), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(5), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(6), IndexNameResolver.Rollover.DAILY));
        }

        for (String index : indices) {
            boolean matched = false;
            for (String match : matches) {
                char c = match.charAt(0);
                if (c == '-') {
                    if (Regex.simpleMatch(match.substring(1), index)) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                } else if (c == '+') {
                    if (Regex.simpleMatch(match.substring(1), index)) {
                        matched = true;
                        break;
                    }
                } else {
                    if (Regex.simpleMatch(match, index)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (!matched) {
                throw new IllegalArgumentException(errorMessage);
            }
        }

        if (indexAuditingEnabled) {
            logger.warn("the [action.auto_create_index] setting is configured to be restrictive [{}]. " +
                    " for the next 6 months audit indices are allowed to be created, but please make sure" +
                    " that any future history indices after 6 months with the pattern " +
                    "[.security_audit_log*] are allowed to be created", value);
        }
    }
}
