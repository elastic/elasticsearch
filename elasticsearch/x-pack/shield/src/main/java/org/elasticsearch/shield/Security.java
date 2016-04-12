/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.shield.action.ShieldActionModule;
import org.elasticsearch.shield.action.filter.ShieldActionFilter;
import org.elasticsearch.shield.action.realm.ClearRealmCacheAction;
import org.elasticsearch.shield.action.realm.TransportClearRealmCacheAction;
import org.elasticsearch.shield.action.role.ClearRolesCacheAction;
import org.elasticsearch.shield.action.role.DeleteRoleAction;
import org.elasticsearch.shield.action.role.GetRolesAction;
import org.elasticsearch.shield.action.role.PutRoleAction;
import org.elasticsearch.shield.action.role.TransportClearRolesCacheAction;
import org.elasticsearch.shield.action.role.TransportDeleteRoleAction;
import org.elasticsearch.shield.action.role.TransportGetRolesAction;
import org.elasticsearch.shield.action.role.TransportPutRoleAction;
import org.elasticsearch.shield.action.user.AuthenticateAction;
import org.elasticsearch.shield.action.user.ChangePasswordAction;
import org.elasticsearch.shield.action.user.DeleteUserAction;
import org.elasticsearch.shield.action.user.GetUsersAction;
import org.elasticsearch.shield.action.user.PutUserAction;
import org.elasticsearch.shield.action.user.TransportAuthenticateAction;
import org.elasticsearch.shield.action.user.TransportChangePasswordAction;
import org.elasticsearch.shield.action.user.TransportDeleteUserAction;
import org.elasticsearch.shield.action.user.TransportGetUsersAction;
import org.elasticsearch.shield.action.user.TransportPutUserAction;
import org.elasticsearch.shield.audit.AuditTrailModule;
import org.elasticsearch.shield.audit.index.IndexAuditTrail;
import org.elasticsearch.shield.audit.index.IndexNameResolver;
import org.elasticsearch.shield.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.shield.authc.AuthenticationModule;
import org.elasticsearch.shield.authc.InternalAuthenticationService;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.shield.authc.esnative.NativeUsersStore;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.authz.AuthorizationModule;
import org.elasticsearch.shield.authz.InternalAuthorizationService;
import org.elasticsearch.shield.authz.accesscontrol.OptOutQueryCache;
import org.elasticsearch.shield.authz.accesscontrol.ShieldIndexSearcherWrapper;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.authz.store.NativeRolesStore;
import org.elasticsearch.shield.crypto.CryptoModule;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.shield.rest.ShieldRestModule;
import org.elasticsearch.shield.rest.action.RestAuthenticateAction;
import org.elasticsearch.shield.rest.action.realm.RestClearRealmCacheAction;
import org.elasticsearch.shield.rest.action.role.RestClearRolesCacheAction;
import org.elasticsearch.shield.rest.action.role.RestDeleteRoleAction;
import org.elasticsearch.shield.rest.action.role.RestGetRolesAction;
import org.elasticsearch.shield.rest.action.role.RestPutRoleAction;
import org.elasticsearch.shield.rest.action.user.RestChangePasswordAction;
import org.elasticsearch.shield.rest.action.user.RestDeleteUserAction;
import org.elasticsearch.shield.rest.action.user.RestGetUsersAction;
import org.elasticsearch.shield.rest.action.user.RestPutUserAction;
import org.elasticsearch.shield.ssl.SSLModule;
import org.elasticsearch.shield.ssl.SSLConfiguration;
import org.elasticsearch.shield.support.OptionalSettings;
import org.elasticsearch.shield.transport.ShieldClientTransportService;
import org.elasticsearch.shield.transport.ShieldServerTransportService;
import org.elasticsearch.shield.transport.ShieldTransportModule;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.shield.transport.netty.ShieldNettyHttpServerTransport;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.shield.user.AnonymousUser;
import org.elasticsearch.xpack.XPackPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 *
 */
public class Security {

    private static final ESLogger logger = Loggers.getLogger(XPackPlugin.class);

    public static final String NAME = "security";
    public static final String DLS_FLS_FEATURE = "security.dls_fls";
    public static final String OPT_OUT_QUERY_CACHE = "opt_out_cache";
    public static final Setting<Optional<String>> USER_SETTING = OptionalSettings.createString(setting("user"), Property.NodeScope);

    private final Settings settings;
    private final boolean enabled;
    private final boolean transportClientMode;
    private SecurityLicenseState securityLicenseState;

    public Security(Settings settings) {
        this.settings = settings;
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        this.enabled = XPackPlugin.featureEnabled(settings, NAME, true);
        if (enabled && !transportClientMode) {
            validateAutoCreateIndex(settings);
        }
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();

        // we can't load that at construction time since the license plugin might not have been loaded at that point
        // which might not be the case during Plugin class instantiation. Once nodeModules are pulled
        // everything should have been loaded
        if (enabled && transportClientMode == false) {
            securityLicenseState = new SecurityLicenseState();
        }

        modules.add(new SecurityModule(settings, securityLicenseState));

        if (enabled == false) {
            return modules;
        }

        if (transportClientMode == true) {
            modules.add(new ShieldTransportModule(settings));
            modules.add(new SSLModule(settings));
            return modules;
        }

        modules.add(new CryptoModule(settings));
        modules.add(new AuthenticationModule(settings));
        modules.add(new AuthorizationModule(settings));
        modules.add(new AuditTrailModule(settings));
        modules.add(new ShieldRestModule(settings));
        modules.add(new ShieldActionModule(settings));
        modules.add(new ShieldTransportModule(settings));
        modules.add(new SSLModule(settings));
        return modules;
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (enabled == false || transportClientMode == true) {
            return Collections.emptyList();
        }
        List<Class<? extends LifecycleComponent>> list = new ArrayList<>();

        //TODO why only focus on file audit logs? shouldn't we just check if audit trail is enabled in general?
        if (AuditTrailModule.fileAuditLoggingEnabled(settings) == true) {
            list.add(LoggingAuditTrail.class);
        }
        list.add(SecurityLicensee.class);
        list.add(InternalCryptoService.class);
        list.add(FileRolesStore.class);
        list.add(Realms.class);
        return list;
    }

    public Settings additionalSettings() {
        if (enabled == false) {
            return Settings.EMPTY;
        }

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME);
        settingsBuilder.put(NetworkModule.TRANSPORT_SERVICE_TYPE_KEY, Security.NAME);
        settingsBuilder.put(NetworkModule.HTTP_TYPE_SETTING.getKey(), Security.NAME);
        addUserSettings(settingsBuilder);
        addTribeSettings(settingsBuilder);
        return settingsBuilder.build();
    }

    public void onModule(SettingsModule settingsModule) {
        // always register for both client and node modes
        XPackPlugin.registerFeatureEnabledSettings(settingsModule, NAME, true);
        settingsModule.registerSetting(USER_SETTING);

        // SSL settings
        SSLConfiguration.Global.registerSettings(settingsModule);

        // transport settings
        ShieldNettyTransport.registerSettings(settingsModule);

        if (transportClientMode) {
            return;
        }

        // The following just apply in node mode
        XPackPlugin.registerFeatureEnabledSettings(settingsModule, DLS_FLS_FEATURE, true);

        // IP Filter settings
        IPFilter.registerSettings(settingsModule);

        // audit settings
        AuditTrailModule.registerSettings(settingsModule);

        // authentication settings
        FileRolesStore.registerSettings(settingsModule);
        AnonymousUser.registerSettings(settingsModule);
        Realms.registerSettings(settingsModule);
        NativeUsersStore.registerSettings(settingsModule);
        NativeRolesStore.registerSettings(settingsModule);
        InternalAuthenticationService.registerSettings(settingsModule);
        InternalAuthorizationService.registerSettings(settingsModule);

        // HTTP settings
        ShieldNettyHttpServerTransport.registerSettings(settingsModule);

        // encryption settings
        InternalCryptoService.registerSettings(settingsModule);

        // hide settings
        settingsModule.registerSetting(Setting.listSetting(setting("hide_settings"), Collections.emptyList(), Function.identity(),
                Property.NodeScope, Property.Filtered));
        String[] asArray = settings.getAsArray(setting("hide_settings"));
        for (String pattern : asArray) {
            settingsModule.registerSettingsFilter(pattern);
        }

        settingsModule.registerSettingsFilter(setting("authc.realms.*.bind_dn"));
        settingsModule.registerSettingsFilter(setting("authc.realms.*.bind_password"));
        settingsModule.registerSettingsFilter(setting("authc.realms.*." + SessionFactory.HOSTNAME_VERIFICATION_SETTING));
        settingsModule.registerSettingsFilter(setting("authc.realms.*.truststore.password"));
        settingsModule.registerSettingsFilter(setting("authc.realms.*.truststore.path"));
        settingsModule.registerSettingsFilter(setting("authc.realms.*.truststore.algorithm"));

        // hide settings where we don't define them - they are part of a group...
        settingsModule.registerSettingsFilter("transport.profiles.*." + setting("*"));
    }

    public void onIndexModule(IndexModule module) {
        if (enabled == false) {
            return;
        }

        assert securityLicenseState != null;
        if (flsDlsEnabled(settings)) {
            module.setSearcherWrapper((indexService) -> new ShieldIndexSearcherWrapper(indexService.getIndexSettings(),
                    indexService.newQueryShardContext(), indexService.mapperService(),
                    indexService.cache().bitsetFilterCache(), indexService.getIndexServices().getThreadPool().getThreadContext(),
                    securityLicenseState));
        }
        if (transportClientMode == false) {
            /*  We need to forcefully overwrite the query cache implementation to use Shield's opt out query cache implementation.
             *  This impl. disabled the query cache if field level security is used for a particular request. If we wouldn't do
             *  forcefully overwrite the query cache implementation then we leave the system vulnerable to leakages of data to
             *  unauthorized users. */
            module.forceQueryCacheProvider(OptOutQueryCache::new);
        }
    }

    public void onModule(ActionModule module) {
        if (enabled == false) {
            return;
        }
        // registering the security filter only for nodes
        if (transportClientMode == false) {
            module.registerFilter(ShieldActionFilter.class);
        }

        // registering all shield actions
        module.registerAction(ClearRealmCacheAction.INSTANCE, TransportClearRealmCacheAction.class);
        module.registerAction(ClearRolesCacheAction.INSTANCE, TransportClearRolesCacheAction.class);
        module.registerAction(GetUsersAction.INSTANCE, TransportGetUsersAction.class);
        module.registerAction(PutUserAction.INSTANCE, TransportPutUserAction.class);
        module.registerAction(DeleteUserAction.INSTANCE, TransportDeleteUserAction.class);
        module.registerAction(GetRolesAction.INSTANCE, TransportGetRolesAction.class);
        module.registerAction(PutRoleAction.INSTANCE, TransportPutRoleAction.class);
        module.registerAction(DeleteRoleAction.INSTANCE, TransportDeleteRoleAction.class);
        module.registerAction(ChangePasswordAction.INSTANCE, TransportChangePasswordAction.class);
        module.registerAction(AuthenticateAction.INSTANCE, TransportAuthenticateAction.class);
    }

    public void onModule(NetworkModule module) {

        if (transportClientMode) {
            if (enabled) {
                module.registerTransport(Security.NAME, ShieldNettyTransport.class);
                module.registerTransportService(Security.NAME, ShieldClientTransportService.class);
            }
            return;
        }

        if (enabled) {
            module.registerTransport(Security.NAME, ShieldNettyTransport.class);
            module.registerTransportService(Security.NAME, ShieldServerTransportService.class);
            module.registerRestHandler(RestAuthenticateAction.class);
            module.registerRestHandler(RestClearRealmCacheAction.class);
            module.registerRestHandler(RestClearRolesCacheAction.class);
            module.registerRestHandler(RestGetUsersAction.class);
            module.registerRestHandler(RestPutUserAction.class);
            module.registerRestHandler(RestDeleteUserAction.class);
            module.registerRestHandler(RestGetRolesAction.class);
            module.registerRestHandler(RestPutRoleAction.class);
            module.registerRestHandler(RestDeleteRoleAction.class);
            module.registerRestHandler(RestChangePasswordAction.class);
            module.registerHttpTransport(Security.NAME, ShieldNettyHttpServerTransport.class);
        }
    }

    private void addUserSettings(Settings.Builder settingsBuilder) {
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
     * that every tribe cluster has shield installed and is enabled. We do that by:
     *
     *    - making it mandatory on the tribe client (this means that the tribe node will fail at startup if shield is
     *      not loaded on any tribe due to missing mandatory plugin)
     *
     *    - forcibly enabling it (that means it's not possible to disable shield on the tribe clients)
     */
    private void addTribeSettings(Settings.Builder settingsBuilder) {
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
                //shield is mandatory on every tribe if installed and enabled on the tribe node
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
                //shield must be enabled on every tribe if it's enabled on the tribe node
                settingsBuilder.put(tribeEnabledSetting, true);
            }

            // we passed all the checks now we need to copy in all of the shield settings
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
                "[{}{}]", (Object) value, ShieldTemplateService.SECURITY_INDEX_NAME, auditIndex);
        if (Booleans.isExplicitFalse(value)) {
            throw new IllegalArgumentException(errorMessage);
        }

        if (Booleans.isExplicitTrue(value)) {
            return;
        }

        String[] matches = Strings.commaDelimitedListToStringArray(value);
        List<String> indices = new ArrayList<>();
        indices.add(ShieldTemplateService.SECURITY_INDEX_NAME);
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
                    "[.shield_audit_log*] are allowed to be created", value);
        }
    }
}
