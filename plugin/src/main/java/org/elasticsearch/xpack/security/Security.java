/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.XpackField;
import org.elasticsearch.xpack.extensions.XPackExtension;
import org.elasticsearch.xpack.extensions.XPackExtensionsService;
import org.elasticsearch.xpack.security.action.filter.SecurityActionFilter;
import org.elasticsearch.xpack.security.action.interceptor.BulkShardRequestInterceptor;
import org.elasticsearch.xpack.security.action.interceptor.IndicesAliasesRequestInterceptor;
import org.elasticsearch.xpack.security.action.interceptor.RequestInterceptor;
import org.elasticsearch.xpack.security.action.interceptor.ResizeRequestInterceptor;
import org.elasticsearch.xpack.security.action.interceptor.SearchRequestInterceptor;
import org.elasticsearch.xpack.security.action.interceptor.UpdateRequestInterceptor;
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
import org.elasticsearch.xpack.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportDeleteRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportGetRoleMappingsAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportPutRoleMappingAction;
import org.elasticsearch.xpack.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.security.action.token.TransportCreateTokenAction;
import org.elasticsearch.xpack.security.action.token.TransportInvalidateTokenAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.PutUserAction;
import org.elasticsearch.xpack.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.security.action.user.TransportAuthenticateAction;
import org.elasticsearch.xpack.security.action.user.TransportChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.TransportDeleteUserAction;
import org.elasticsearch.xpack.security.action.user.TransportGetUsersAction;
import org.elasticsearch.xpack.security.action.user.TransportHasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.TransportPutUserAction;
import org.elasticsearch.xpack.security.action.user.TransportSetEnabledAction;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.audit.index.IndexNameResolver;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenMetaData;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.ExpressionParser;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.SecuritySearchOperationListener;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.accesscontrol.OptOutQueryCache;
import org.elasticsearch.xpack.security.authz.accesscontrol.SecurityIndexSearcherWrapper;
import org.elasticsearch.xpack.security.authz.accesscontrol.SetSecurityUserProcessor;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.rest.SecurityRestFilter;
import org.elasticsearch.xpack.security.rest.action.RestAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction;
import org.elasticsearch.xpack.security.rest.action.realm.RestClearRealmCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestClearRolesCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestDeleteRoleAction;
import org.elasticsearch.xpack.security.rest.action.role.RestGetRolesAction;
import org.elasticsearch.xpack.security.rest.action.role.RestPutRoleAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestDeleteRoleMappingAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestGetRoleMappingsAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestPutRoleMappingAction;
import org.elasticsearch.xpack.security.rest.action.user.RestChangePasswordAction;
import org.elasticsearch.xpack.security.rest.action.user.RestDeleteUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestGetUsersAction;
import org.elasticsearch.xpack.security.rest.action.user.RestHasPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestSetEnabledAction;
import org.elasticsearch.xpack.security.support.IndexLifecycleManager;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4HttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.TLSLicenseBootstrapCheck;
import org.elasticsearch.xpack.ssl.action.GetCertificateInfoAction;
import org.elasticsearch.xpack.ssl.action.TransportGetCertificateInfoAction;
import org.elasticsearch.xpack.ssl.rest.RestGetCertificateInfoAction;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_FORMAT_SETTING;
import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_TEMPLATE_NAME;
import static org.elasticsearch.xpack.security.support.IndexLifecycleManager.INTERNAL_INDEX_FORMAT;

public class Security implements ActionPlugin, IngestPlugin, NetworkPlugin, ClusterPlugin, DiscoveryPlugin, MapperPlugin {

    private static final Logger logger = Loggers.getLogger(XPackPlugin.class);

    public static final String NAME4 = XpackField.SECURITY + "4";
    public static final Setting<Optional<String>> USER_SETTING =
            new Setting<>(SecurityField.setting("user"), (String) null, Optional::ofNullable, Property.NodeScope);

    static final Setting<List<String>> AUDIT_OUTPUTS_SETTING =
        Setting.listSetting(SecurityField.setting("audit.outputs"),
            s -> s.keySet().contains(SecurityField.setting("audit.outputs")) ?
                Collections.emptyList() : Collections.singletonList(LoggingAuditTrail.NAME),
            Function.identity(), Property.NodeScope);

    private final Settings settings;
    private final Environment env;
    private final boolean enabled;
    private final boolean transportClientMode;
    private final XPackLicenseState licenseState;
    private final SSLService sslService;
    /* what a PITA that we need an extra indirection to initialize this. Yet, once we got rid of guice we can thing about how
     * to fix this or make it simpler. Today we need several service that are created in createComponents but we need to register
     * an instance of TransportInterceptor way earlier before createComponents is called. */
    private final SetOnce<TransportInterceptor> securityInterceptor = new SetOnce<>();
    private final SetOnce<IPFilter> ipFilter = new SetOnce<>();
    private final SetOnce<AuthenticationService> authcService = new SetOnce<>();
    private final SetOnce<AuditTrailService> auditTrailService = new SetOnce<>();
    private final SetOnce<SecurityContext> securityContext = new SetOnce<>();
    private final SetOnce<ThreadContext> threadContext = new SetOnce<>();
    private final SetOnce<TokenService> tokenService = new SetOnce<>();
    private final SetOnce<SecurityActionFilter> securityActionFilter = new SetOnce<>();
    private final List<BootstrapCheck> bootstrapChecks;

    public Security(Settings settings, Environment env, XPackLicenseState licenseState, SSLService sslService) {
        this.settings = settings;
        this.env = env;
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        this.enabled = XPackSettings.SECURITY_ENABLED.get(settings);
        if (enabled && transportClientMode == false) {
            validateAutoCreateIndex(settings);
        }
        this.licenseState = licenseState;
        this.sslService = sslService;
        if (enabled) {
            // we load them all here otherwise we can't access secure settings since they are closed once the checks are
            // fetched
            final List<BootstrapCheck> checks = new ArrayList<>();
            checks.addAll(Arrays.asList(
                    new TokenSSLBootstrapCheck(),
                    new PkiRealmBootstrapCheck(sslService),
                    new TLSLicenseBootstrapCheck()));
            checks.addAll(InternalRealms.getBootstrapChecks(settings, env));
            this.bootstrapChecks = Collections.unmodifiableList(checks);
        } else {
            this.bootstrapChecks = Collections.emptyList();
        }
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        if (enabled == false || transportClientMode) {
            modules.add(b -> b.bind(IPFilter.class).toProvider(Providers.of(null)));
        }

        if (transportClientMode) {
            if (enabled == false) {
                return modules;
            }
            modules.add(b -> {
                // for transport client we still must inject these ssl classes with guice
                b.bind(SSLService.class).toInstance(sslService);
            });

            return modules;
        }
        modules.add(b -> XPackPlugin.bindFeatureSet(b, SecurityFeatureSet.class));

        
        if (enabled == false) {
            modules.add(b -> {
                b.bind(Realms.class).toProvider(Providers.of(null)); // for SecurityFeatureSet
                b.bind(CompositeRolesStore.class).toProvider(Providers.of(null)); // for SecurityFeatureSet
                b.bind(NativeRoleMappingStore.class).toProvider(Providers.of(null)); // for SecurityFeatureSet
                b.bind(AuditTrailService.class)
                    .toInstance(new AuditTrailService(settings, Collections.emptyList(), licenseState));
            });
            return modules;
        }

        // we can't load that at construction time since the license plugin might not have been loaded at that point
        // which might not be the case during Plugin class instantiation. Once nodeModules are pulled
        // everything should have been loaded
        modules.add(b -> {
            if (XPackSettings.AUDIT_ENABLED.get(settings)) {
                b.bind(AuditTrail.class).to(AuditTrailService.class); // interface used by some actions...
            }
        });
        return modules;
    }

    public Collection<Object> createComponents(Client client, ThreadPool threadPool, ClusterService clusterService,
                                               ResourceWatcherService resourceWatcherService,
                                               List<XPackExtension> extensions) throws Exception {
        if (enabled == false) {
            return Collections.emptyList();
        }
        threadContext.set(threadPool.getThreadContext());
        List<Object> components = new ArrayList<>();
        securityContext.set(new SecurityContext(settings, threadPool.getThreadContext()));
        components.add(securityContext.get());

        // audit trails construction
        IndexAuditTrail indexAuditTrail = null;
        Set<AuditTrail> auditTrails = new LinkedHashSet<>();
        if (XPackSettings.AUDIT_ENABLED.get(settings)) {
            List<String> outputs = AUDIT_OUTPUTS_SETTING.get(settings);
            if (outputs.isEmpty()) {
                throw new IllegalArgumentException("Audit logging is enabled but there are zero output types in "
                    + XPackSettings.AUDIT_ENABLED.getKey());
            }

            for (String output : outputs) {
                switch (output) {
                    case LoggingAuditTrail.NAME:
                        auditTrails.add(new LoggingAuditTrail(settings, clusterService, threadPool));
                        break;
                    case IndexAuditTrail.NAME:
                        indexAuditTrail = new IndexAuditTrail(settings, client, threadPool, clusterService);
                        auditTrails.add(indexAuditTrail);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown audit trail output [" + output + "]");
                }
            }
        }
        final AuditTrailService auditTrailService =
                new AuditTrailService(settings, new ArrayList<>(auditTrails), licenseState);
        components.add(auditTrailService);
        this.auditTrailService.set(auditTrailService);

        final SecurityLifecycleService securityLifecycleService =
                new SecurityLifecycleService(settings, clusterService, threadPool, client, indexAuditTrail);
        final TokenService tokenService = new TokenService(settings, Clock.systemUTC(), client, securityLifecycleService, clusterService);
        this.tokenService.set(tokenService);
        components.add(tokenService);

        // realms construction
        final NativeUsersStore nativeUsersStore = new NativeUsersStore(settings, client, securityLifecycleService);
        final NativeRoleMappingStore nativeRoleMappingStore = new NativeRoleMappingStore(settings, client, securityLifecycleService);
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        final ReservedRealm reservedRealm = new ReservedRealm(env, settings, nativeUsersStore,
                anonymousUser, securityLifecycleService, threadPool.getThreadContext());
        Map<String, Realm.Factory> realmFactories = new HashMap<>(InternalRealms.getFactories(threadPool, resourceWatcherService,
                sslService, nativeUsersStore, nativeRoleMappingStore, securityLifecycleService));
        for (XPackExtension extension : extensions) {
            Map<String, Realm.Factory> newRealms = extension.getRealms(resourceWatcherService);
            for (Map.Entry<String, Realm.Factory> entry : newRealms.entrySet()) {
                if (realmFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Realm type [" + entry.getKey() + "] is already registered");
                }
            }
        }
        final Realms realms = new Realms(settings, env, realmFactories, licenseState, threadPool.getThreadContext(), reservedRealm);
        components.add(nativeUsersStore);
        components.add(nativeRoleMappingStore);
        components.add(realms);
        components.add(reservedRealm);

        AuthenticationFailureHandler failureHandler = null;
        String extensionName = null;
        for (XPackExtension extension : extensions) {
            AuthenticationFailureHandler extensionFailureHandler = extension.getAuthenticationFailureHandler();
            if (extensionFailureHandler != null && failureHandler != null) {
                throw new IllegalStateException("Extensions [" + extensionName + "] and [" + extension.name() + "] " +
                    "both set an authentication failure handler");
            }
            failureHandler = extensionFailureHandler;
            extensionName = extension.name();
        }
        if (failureHandler == null) {
            logger.debug("Using default authentication failure handler");
            failureHandler = new DefaultAuthenticationFailureHandler();
        } else {
            logger.debug("Using authentication failure handler from extension [" + extensionName + "]");
        }

        authcService.set(new AuthenticationService(settings, realms, auditTrailService, failureHandler, threadPool, anonymousUser, tokenService));
        components.add(authcService.get());

        final FileRolesStore fileRolesStore = new FileRolesStore(settings, env, resourceWatcherService, licenseState);
        final NativeRolesStore nativeRolesStore = new NativeRolesStore(settings, client, licenseState, securityLifecycleService);
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();
        List<BiConsumer<Set<String>, ActionListener<Set<RoleDescriptor>>>> rolesProviders = new ArrayList<>();
        for (XPackExtension extension : extensions) {
            rolesProviders.addAll(extension.getRolesProviders(settings, resourceWatcherService));
        }
        final CompositeRolesStore allRolesStore = new CompositeRolesStore(settings, fileRolesStore, nativeRolesStore,
            reservedRolesStore, rolesProviders, threadPool.getThreadContext(), licenseState);
        securityLifecycleService.addSecurityIndexHealthChangeListener(allRolesStore::onSecurityIndexHealthChange);
        securityLifecycleService.addSecurityIndexOutOfDateListener(allRolesStore::onSecurityIndexOutOfDateChange);
        // to keep things simple, just invalidate all cached entries on license change. this happens so rarely that the impact should be
        // minimal
        licenseState.addListener(allRolesStore::invalidateAll);
        final AuthorizationService authzService = new AuthorizationService(settings, allRolesStore, clusterService,
            auditTrailService, failureHandler, threadPool, anonymousUser);
        components.add(nativeRolesStore); // used by roles actions
        components.add(reservedRolesStore); // used by roles actions
        components.add(allRolesStore); // for SecurityFeatureSet and clear roles cache
        components.add(authzService);

        components.add(securityLifecycleService);

        ipFilter.set(new IPFilter(settings, auditTrailService, clusterService.getClusterSettings(), licenseState));
        components.add(ipFilter.get());
        DestructiveOperations destructiveOperations = new DestructiveOperations(settings, clusterService.getClusterSettings());
        securityInterceptor.set(new SecurityServerTransportInterceptor(settings, threadPool, authcService.get(), authzService, licenseState,
                sslService, securityContext.get(), destructiveOperations));

        final Set<RequestInterceptor> requestInterceptors;
        if (XPackSettings.DLS_FLS_ENABLED.get(settings)) {
            requestInterceptors = Collections.unmodifiableSet(Sets.newHashSet(
                    new SearchRequestInterceptor(settings, threadPool, licenseState),
                    new UpdateRequestInterceptor(settings, threadPool, licenseState),
                    new BulkShardRequestInterceptor(settings, threadPool, licenseState),
                    new ResizeRequestInterceptor(settings, threadPool, licenseState, auditTrailService),
                    new IndicesAliasesRequestInterceptor(threadPool.getThreadContext(), licenseState, auditTrailService)));
        } else {
            requestInterceptors = Collections.emptySet();
        }

        securityActionFilter.set(new SecurityActionFilter(settings, authcService.get(), authzService, licenseState,
                requestInterceptors, threadPool, securityContext.get(), destructiveOperations));

        return components;
    }

    public Settings additionalSettings() {
        if (enabled == false) {
            return Settings.EMPTY;
        }

        return additionalSettings(settings, transportClientMode);
    }

    // visible for tests
    static Settings additionalSettings(Settings settings, boolean transportClientMode) {
        final Settings.Builder settingsBuilder = Settings.builder();

        if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings)) {
            final String transportType = NetworkModule.TRANSPORT_TYPE_SETTING.get(settings);
            if (NAME4.equals(transportType) == false) {
                throw new IllegalArgumentException("transport type setting [" + NetworkModule.TRANSPORT_TYPE_KEY + "] must be [" + NAME4
                        + "] but is [" + transportType + "]");
            }
        } else {
            // default to security4
            settingsBuilder.put(NetworkModule.TRANSPORT_TYPE_KEY, NAME4);
        }

        if (NetworkModule.HTTP_TYPE_SETTING.exists(settings)) {
            final String httpType = NetworkModule.HTTP_TYPE_SETTING.get(settings);
            if (httpType.equals(NAME4)) {
                SecurityNetty4HttpServerTransport.overrideSettings(settingsBuilder, settings);
            } else {
                throw new IllegalArgumentException("http type setting [" + NetworkModule.HTTP_TYPE_KEY + "] must be [" + NAME4
                        + "] but is [" + httpType + "]");
            }
        } else {
            // default to security4
            settingsBuilder.put(NetworkModule.HTTP_TYPE_KEY, NAME4);
            SecurityNetty4HttpServerTransport.overrideSettings(settingsBuilder, settings);
        }

        addUserSettings(settings, settingsBuilder);
        addTribeSettings(settings, settingsBuilder);
        return settingsBuilder.build();
    }

    /**
     * Get the {@link Setting setting configuration} for all security components, including those defined in extensions.
     */
    public static List<Setting<?>> getSettings(boolean transportClientMode, @Nullable XPackExtensionsService extensionsService) {
        List<Setting<?>> settingsList = new ArrayList<>();
        // always register for both client and node modes
        settingsList.add(USER_SETTING);

        if (transportClientMode) {
            return settingsList;
        }

        // The following just apply in node mode

        // IP Filter settings
        IPFilter.addSettings(settingsList);

        // audit settings
        settingsList.add(AUDIT_OUTPUTS_SETTING);
        LoggingAuditTrail.registerSettings(settingsList);
        IndexAuditTrail.registerSettings(settingsList);

        // authentication settings
        AnonymousUser.addSettings(settingsList);
        RealmSettings.addSettings(settingsList, extensionsService == null ? null : extensionsService.getExtensions());
        NativeRolesStore.addSettings(settingsList);
        ReservedRealm.addSettings(settingsList);
        AuthenticationService.addSettings(settingsList);
        AuthorizationService.addSettings(settingsList);
        settingsList.add(CompositeRolesStore.CACHE_SIZE_SETTING);
        settingsList.add(FieldPermissionsCache.CACHE_SIZE_SETTING);
        settingsList.add(TokenService.TOKEN_EXPIRATION);
        settingsList.add(TokenService.DELETE_INTERVAL);
        settingsList.add(TokenService.DELETE_TIMEOUT);
        settingsList.add(SecurityServerTransportInterceptor.TRANSPORT_TYPE_PROFILE_SETTING);
        settingsList.addAll(SSLConfigurationSettings.getProfileSettings());

        // hide settings
        settingsList.add(Setting.listSetting(SecurityField.setting("hide_settings"), Collections.emptyList(), Function.identity(),
                Property.NodeScope, Property.Filtered));
        return settingsList;
    }

    
    public List<String> getSettingsFilter(@Nullable XPackExtensionsService extensionsService) {
        List<String> asArray = settings.getAsList(SecurityField.setting("hide_settings"));
        ArrayList<String> settingsFilter = new ArrayList<>(asArray);

        final List<XPackExtension> extensions = extensionsService == null ? Collections.emptyList() : extensionsService.getExtensions();
        settingsFilter.addAll(RealmSettings.getSettingsFilter(extensions));

        // hide settings where we don't define them - they are part of a group...
        settingsFilter.add("transport.profiles.*." + SecurityField.setting("*"));
        return settingsFilter;
    }

    public List<BootstrapCheck> getBootstrapChecks() {
       return bootstrapChecks;
    }

    public void onIndexModule(IndexModule module) {
        if (enabled) {
            assert licenseState != null;
            if (XPackSettings.DLS_FLS_ENABLED.get(settings)) {
                module.setSearcherWrapper(indexService ->
                        new SecurityIndexSearcherWrapper(indexService.getIndexSettings(),
                                shardId -> indexService.newQueryShardContext(shardId.id(),
                                // we pass a null index reader, which is legal and will disable rewrite optimizations
                                // based on index statistics, which is probably safer...
                                null,
                                () -> {
                                    throw new IllegalArgumentException("permission filters are not allowed to use the current timestamp");

                                }, null),
                                indexService.cache().bitsetFilterCache(),
                                indexService.getThreadPool().getThreadContext(), licenseState,
                                indexService.getScriptService()));
                /*  We need to forcefully overwrite the query cache implementation to use security's opt out query cache implementation.
                *  This impl. disabled the query cache if field level security is used for a particular request. If we wouldn't do
                *  forcefully overwrite the query cache implementation then we leave the system vulnerable to leakages of data to
                *  unauthorized users. */
                module.forceQueryCacheProvider((settings, cache) -> new OptOutQueryCache(settings, cache, threadContext.get()));
            }

            // in order to prevent scroll ids from being maliciously crafted and/or guessed, a listener is added that
            // attaches information to the scroll context so that we can validate the user that created the scroll against
            // the user that is executing a scroll operation
            module.addSearchOperationListener(
                    new SecuritySearchOperationListener(threadContext.get(), licenseState, auditTrailService.get()));
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return emptyList();
        }
        return Arrays.asList(
                new ActionHandler<>(ClearRealmCacheAction.INSTANCE, TransportClearRealmCacheAction.class),
                new ActionHandler<>(ClearRolesCacheAction.INSTANCE, TransportClearRolesCacheAction.class),
                new ActionHandler<>(GetUsersAction.INSTANCE, TransportGetUsersAction.class),
                new ActionHandler<>(PutUserAction.INSTANCE, TransportPutUserAction.class),
                new ActionHandler<>(DeleteUserAction.INSTANCE, TransportDeleteUserAction.class),
                new ActionHandler<>(GetRolesAction.INSTANCE, TransportGetRolesAction.class),
                new ActionHandler<>(PutRoleAction.INSTANCE, TransportPutRoleAction.class),
                new ActionHandler<>(DeleteRoleAction.INSTANCE, TransportDeleteRoleAction.class),
                new ActionHandler<>(ChangePasswordAction.INSTANCE, TransportChangePasswordAction.class),
                new ActionHandler<>(AuthenticateAction.INSTANCE, TransportAuthenticateAction.class),
                new ActionHandler<>(SetEnabledAction.INSTANCE, TransportSetEnabledAction.class),
                new ActionHandler<>(HasPrivilegesAction.INSTANCE, TransportHasPrivilegesAction.class),
                new ActionHandler<>(GetRoleMappingsAction.INSTANCE, TransportGetRoleMappingsAction.class),
                new ActionHandler<>(PutRoleMappingAction.INSTANCE, TransportPutRoleMappingAction.class),
                new ActionHandler<>(DeleteRoleMappingAction.INSTANCE, TransportDeleteRoleMappingAction.class),
                new ActionHandler<>(CreateTokenAction.INSTANCE, TransportCreateTokenAction.class),
                new ActionHandler<>(InvalidateTokenAction.INSTANCE, TransportInvalidateTokenAction.class),
                new ActionHandler<>(GetCertificateInfoAction.INSTANCE, TransportGetCertificateInfoAction.class)
        );
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        if (enabled == false) {
            return emptyList();
        }
        // registering the security filter only for nodes
        if (transportClientMode == false) {
            return singletonList(securityActionFilter.get());
        }
        return emptyList();
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return emptyList();
        }
        return Arrays.asList(
                new RestAuthenticateAction(settings, restController, securityContext.get(), licenseState),
                new RestClearRealmCacheAction(settings, restController, licenseState),
                new RestClearRolesCacheAction(settings, restController, licenseState),
                new RestGetUsersAction(settings, restController, licenseState),
                new RestPutUserAction(settings, restController, licenseState),
                new RestDeleteUserAction(settings, restController, licenseState),
                new RestGetRolesAction(settings, restController, licenseState),
                new RestPutRoleAction(settings, restController, licenseState),
                new RestDeleteRoleAction(settings, restController, licenseState),
                new RestChangePasswordAction(settings, restController, securityContext.get(), licenseState),
                new RestSetEnabledAction(settings, restController, licenseState),
                new RestHasPrivilegesAction(settings, restController, securityContext.get(), licenseState),
                new RestGetRoleMappingsAction(settings, restController, licenseState),
                new RestPutRoleMappingAction(settings, restController, licenseState),
                new RestDeleteRoleMappingAction(settings, restController, licenseState),
                new RestGetTokenAction(settings, restController, licenseState),
                new RestInvalidateTokenAction(settings, restController, licenseState),
                new RestGetCertificateInfoAction(settings, restController)
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(SetSecurityUserProcessor.TYPE, new SetSecurityUserProcessor.Factory(parameters.threadContext));
    }

    private static void addUserSettings(Settings settings, Settings.Builder settingsBuilder) {
        String authHeaderSettingName = ThreadContext.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER;
        if (settings.get(authHeaderSettingName) != null) {
            return;
        }
        Optional<String> userOptional = USER_SETTING.get(settings); // TODO migrate to securesetting!
        userOptional.ifPresent(userSetting -> {
            final int i = userSetting.indexOf(":");
            if (i < 0 || i == userSetting.length() - 1) {
                throw new IllegalArgumentException("invalid [" + USER_SETTING.getKey() + "] setting. must be in the form of " +
                        "\"<username>:<password>\"");
            }
            String username = userSetting.substring(0, i);
            String password = userSetting.substring(i + 1);
            settingsBuilder.put(authHeaderSettingName, UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString(password)));
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

        for (Map.Entry<String, Settings> tribeSettings : tribesSettings.entrySet()) {
            final String tribeName = tribeSettings.getKey();
            final String tribePrefix = "tribe." + tribeName + ".";

            if ("blocks".equals(tribeName) || "on_conflict".equals(tribeName) || "name".equals(tribeName)) {
                continue;
            }

            final String tribeEnabledSetting = tribePrefix + XPackSettings.SECURITY_ENABLED.getKey();
            if (settings.get(tribeEnabledSetting) != null) {
                boolean enabled = XPackSettings.SECURITY_ENABLED.get(tribeSettings.getValue());
                if (!enabled) {
                    throw new IllegalStateException("tribe setting [" + tribeEnabledSetting + "] must be set to true but the value is ["
                            + settings.get(tribeEnabledSetting) + "]");
                }
            } else {
                //x-pack security must be enabled on every tribe if it's enabled on the tribe node
                settingsBuilder.put(tribeEnabledSetting, true);
            }

            // we passed all the checks now we need to copy in all of the x-pack security settings
            settings.keySet().forEach(k -> {
                if (k.startsWith("xpack.security.")) {
                    settingsBuilder.copy(tribePrefix + k, k, settings);
                }
            });
        }

        Map<String, Settings> realmsSettings = settings.getGroups(SecurityField.setting("authc.realms"), true);
        final boolean hasNativeRealm = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings) ||
                realmsSettings.isEmpty() ||
                realmsSettings.entrySet().stream()
                        .anyMatch((e) -> NativeRealm.TYPE.equals(e.getValue().get("type")) && e.getValue().getAsBoolean("enabled", true));
        if (hasNativeRealm) {
            if (settings.get("tribe.on_conflict", "").startsWith("prefer_") == false) {
                throw new IllegalArgumentException("use of security on tribe nodes requires setting [tribe.on_conflict] to specify the " +
                        "name of the tribe to prefer such as [prefer_t1] as the security index can exist in multiple tribes but only one" +
                        " can be used by the tribe node");
            }
        }
    }

    static boolean indexAuditLoggingEnabled(Settings settings) {
        if (XPackSettings.AUDIT_ENABLED.get(settings)) {
            List<String> outputs = AUDIT_OUTPUTS_SETTING.get(settings);
            for (String output : outputs) {
                if (output.equals(IndexAuditTrail.NAME)) {
                    return true;
                }
            }
        }
        return false;
    }

    static void validateAutoCreateIndex(Settings settings) {
        String value = settings.get("action.auto_create_index");
        if (value == null) {
            return;
        }

        final boolean indexAuditingEnabled = Security.indexAuditLoggingEnabled(settings);
        final String auditIndex;
        if (indexAuditingEnabled) {
            auditIndex = "," + IndexAuditTrail.INDEX_NAME_PREFIX + "*";
        } else {
            auditIndex = "";
        }
        String securityIndices = SecurityLifecycleService.indexNames().stream()
                .collect(Collectors.joining(","));
        String errorMessage = LoggerMessageFormat.format(
                "the [action.auto_create_index] setting value [{}] is too" +
                        " restrictive. disable [action.auto_create_index] or set it to " +
                        "[{}{}]", (Object) value, securityIndices, auditIndex);
        if (Booleans.isFalse(value)) {
            throw new IllegalArgumentException(errorMessage);
        }

        if (Booleans.isTrue(value)) {
            return;
        }

        String[] matches = Strings.commaDelimitedListToStringArray(value);
        List<String> indices = new ArrayList<>(SecurityLifecycleService.indexNames());
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

    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        if (transportClientMode || enabled == false) { // don't register anything if we are not enabled
            // interceptors are not installed if we are running on the transport client
            return Collections.emptyList();
        }
       return Collections.singletonList(new TransportInterceptor() {
            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                                                                                            boolean forceExecution,
                                                                                            TransportRequestHandler<T> actualHandler) {
                assert securityInterceptor.get() != null;
                return securityInterceptor.get().interceptHandler(action, executor, forceExecution, actualHandler);
            }

            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                assert securityInterceptor.get() != null;
                return securityInterceptor.get().interceptSender(sender);
            }
        });
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                          PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry,
                                                          NetworkService networkService) {
        if (enabled == false) { // don't register anything if we are not enabled
            return Collections.emptyMap();
        }
        return Collections.singletonMap(Security.NAME4, () -> new SecurityNetty4Transport(settings, threadPool, networkService, bigArrays,
                namedWriteableRegistry, circuitBreakerService, ipFilter.get(), sslService));
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                        CircuitBreakerService circuitBreakerService,
                                                                        NamedWriteableRegistry namedWriteableRegistry,
                                                                        NamedXContentRegistry xContentRegistry,
                                                                        NetworkService networkService,
                                                                        HttpServerTransport.Dispatcher dispatcher) {
        if (enabled == false) { // don't register anything if we are not enabled
            return Collections.emptyMap();
        }
        return Collections.singletonMap(Security.NAME4, () -> new SecurityNetty4HttpServerTransport(settings, networkService, bigArrays,
                ipFilter.get(), sslService, threadPool, xContentRegistry, dispatcher));
    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        if (enabled == false || transportClientMode) {
            return null;
        }
        final boolean ssl = HTTP_SSL_ENABLED.get(settings);
        Settings httpSSLSettings = SSLService.getHttpTransportSSLSettings(settings);
        boolean extractClientCertificate = ssl && sslService.isSSLClientAuthEnabled(httpSSLSettings);
        return handler -> new SecurityRestFilter(licenseState, threadContext, authcService.get(), handler,
                extractClientCertificate);
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetaData.TYPE, TokenMetaData::new));
        entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, TokenMetaData.TYPE, TokenMetaData::readDiffFrom));
        entries.add(new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XpackField.SECURITY, SecurityFeatureSet.Usage::new));
        entries.addAll(Arrays.asList(ExpressionParser.NAMED_WRITEABLES));
        return entries;
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        if (enabled && transportClientMode == false) {
            return Collections.singletonList(
                    new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool"));
        }
        return Collections.emptyList();
    }

    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return templates -> {
            final byte[] securityTemplate = TemplateUtils.loadTemplate("/" + SECURITY_TEMPLATE_NAME + ".json",
                    Version.CURRENT.toString(), IndexLifecycleManager.TEMPLATE_VERSION_PATTERN).getBytes(StandardCharsets.UTF_8);
            final XContent xContent = XContentFactory.xContent(XContentType.JSON);

            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, securityTemplate)) {
                templates.put(SECURITY_TEMPLATE_NAME, IndexTemplateMetaData.Builder.fromXContent(parser, SECURITY_TEMPLATE_NAME));
            } catch (IOException e) {
                // TODO: should we handle this with a thrown exception?
                logger.error("Error loading template [{}] as part of metadata upgrading", SECURITY_TEMPLATE_NAME);
            }

            final byte[] auditTemplate = TemplateUtils.loadTemplate("/" + IndexAuditTrail.INDEX_TEMPLATE_NAME + ".json",
                    Version.CURRENT.toString(), IndexLifecycleManager.TEMPLATE_VERSION_PATTERN).getBytes(StandardCharsets.UTF_8);

            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, auditTemplate)) {
                IndexTemplateMetaData auditMetadata = new IndexTemplateMetaData.Builder(
                        IndexTemplateMetaData.Builder.fromXContent(parser, IndexAuditTrail.INDEX_TEMPLATE_NAME))
                        .settings(IndexAuditTrail.customAuditIndexSettings(settings, logger))
                        .build();
                templates.put(IndexAuditTrail.INDEX_TEMPLATE_NAME, auditMetadata);
            } catch (IOException e) {
                // TODO: should we handle this with a thrown exception?
                logger.error("Error loading template [{}] as part of metadata upgrading", IndexAuditTrail.INDEX_TEMPLATE_NAME);
            }

            return templates;
        };
    }

    @Override
    public Map<String, Supplier<ClusterState.Custom>> getInitialClusterStateCustomSupplier() {
        if (enabled) {
            return Collections.singletonMap(TokenMetaData.TYPE, () -> tokenService.get().getTokenMetaData());
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public Function<String, Predicate<String>> getFieldFilter() {
        if (enabled) {
            return index -> {
                if (licenseState.isDocumentAndFieldLevelSecurityAllowed() == false) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                IndicesAccessControl indicesAccessControl = threadContext.get().getTransient(AuthorizationService.INDICES_PERMISSIONS_KEY);
                IndicesAccessControl.IndexAccessControl indexPermissions = indicesAccessControl.getIndexPermissions(index);
                if (indexPermissions == null) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                if (indexPermissions.isGranted() == false) {
                    throw new IllegalStateException("unexpected call to getFieldFilter for index [" + index + "] which is not granted");
                }
                FieldPermissions fieldPermissions = indexPermissions.getFieldPermissions();
                if (fieldPermissions == null) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                return fieldPermissions::grantsAccessTo;
            };
        }
        return MapperPlugin.super.getFieldFilter();
    }

    @Override
    public BiConsumer<DiscoveryNode, ClusterState> getJoinValidator() {
        if (enabled) {
            return new ValidateTLSOnJoin(XPackSettings.TRANSPORT_SSL_ENABLED.get(settings),
                    DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings))
                .andThen(new ValidateUpgradedSecurityIndex());
        }
        return null;
    }

    static final class ValidateTLSOnJoin implements BiConsumer<DiscoveryNode, ClusterState> {
        private final boolean isTLSEnabled;
        private final String discoveryType;

        ValidateTLSOnJoin(boolean isTLSEnabled, String discoveryType) {
            this.isTLSEnabled = isTLSEnabled;
            this.discoveryType = discoveryType;
        }

        @Override
        public void accept(DiscoveryNode node, ClusterState state) {
            License license = LicenseService.getLicense(state.metaData());
            if (license != null && license.isProductionLicense() &&
                    isTLSEnabled == false && "single-node".equals(discoveryType) == false) {
                throw new IllegalStateException("TLS setup is required for license type [" + license.operationMode().name() + "]");
            }
        }
    }

    static final class ValidateUpgradedSecurityIndex implements BiConsumer<DiscoveryNode, ClusterState> {
        @Override
        public void accept(DiscoveryNode node, ClusterState state) {
            if (state.getNodes().getMinNodeVersion().before(Version.V_7_0_0_alpha1)) {
                IndexMetaData indexMetaData = state.getMetaData().getIndices().get(SECURITY_INDEX_NAME);
                if (indexMetaData != null && INDEX_FORMAT_SETTING.get(indexMetaData.getSettings()) < INTERNAL_INDEX_FORMAT) {
                    throw new IllegalStateException("Security index is not on the current version [" + INTERNAL_INDEX_FORMAT + "] - " +
                        "The Upgrade API must be run for 7.x nodes to join the cluster");
                }
            }
        }
    }
}
