/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.LogManager;
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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.SharedGroupFactory;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.nio.NioGroupFactory;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.SecuritySettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.InternalRealmsSettings;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetBitsetCache;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.SecurityIndexReaderWrapper;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TLSLicenseBootstrapCheck;
import org.elasticsearch.xpack.core.ssl.action.GetCertificateInfoAction;
import org.elasticsearch.xpack.core.ssl.action.TransportGetCertificateInfoAction;
import org.elasticsearch.xpack.core.ssl.rest.RestGetCertificateInfoAction;
import org.elasticsearch.xpack.security.action.TransportCreateApiKeyAction;
import org.elasticsearch.xpack.security.action.TransportDelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.security.action.TransportGetApiKeyAction;
import org.elasticsearch.xpack.security.action.TransportGrantApiKeyAction;
import org.elasticsearch.xpack.security.action.TransportInvalidateApiKeyAction;
import org.elasticsearch.xpack.security.action.filter.SecurityActionFilter;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectLogoutAction;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.action.privilege.TransportDeletePrivilegesAction;
import org.elasticsearch.xpack.security.action.privilege.TransportGetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.security.action.privilege.TransportGetPrivilegesAction;
import org.elasticsearch.xpack.security.action.privilege.TransportPutPrivilegesAction;
import org.elasticsearch.xpack.security.action.realm.TransportClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.role.TransportClearRolesCacheAction;
import org.elasticsearch.xpack.security.action.role.TransportDeleteRoleAction;
import org.elasticsearch.xpack.security.action.role.TransportGetRolesAction;
import org.elasticsearch.xpack.security.action.role.TransportPutRoleAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportDeleteRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportGetRoleMappingsAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportPutRoleMappingAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlAuthenticateAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlInvalidateSessionAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlLogoutAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.action.token.TransportCreateTokenAction;
import org.elasticsearch.xpack.security.action.token.TransportInvalidateTokenAction;
import org.elasticsearch.xpack.security.action.token.TransportRefreshTokenAction;
import org.elasticsearch.xpack.security.action.user.TransportAuthenticateAction;
import org.elasticsearch.xpack.security.action.user.TransportChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.TransportDeleteUserAction;
import org.elasticsearch.xpack.security.action.user.TransportGetUserPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.TransportGetUsersAction;
import org.elasticsearch.xpack.security.action.user.TransportHasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.TransportPutUserAction;
import org.elasticsearch.xpack.security.action.user.TransportSetEnabledAction;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.SecuritySearchOperationListener;
import org.elasticsearch.xpack.security.authz.accesscontrol.OptOutQueryCache;
import org.elasticsearch.xpack.security.authz.interceptor.BulkShardRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.IndicesAliasesRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.RequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.ResizeRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.SearchRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.UpdateRequestInterceptor;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.DeprecationRoleDescriptorConsumer;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor;
import org.elasticsearch.xpack.security.rest.SecurityRestFilter;
import org.elasticsearch.xpack.security.rest.action.RestAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.RestDelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestCreateApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGetApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGrantApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestInvalidateApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectLogoutAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestDeletePrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestGetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestGetPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestPutPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.realm.RestClearRealmCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestClearRolesCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestDeleteRoleAction;
import org.elasticsearch.xpack.security.rest.action.role.RestGetRolesAction;
import org.elasticsearch.xpack.security.rest.action.role.RestPutRoleAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestDeleteRoleMappingAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestGetRoleMappingsAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestPutRoleMappingAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlInvalidateSessionAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlLogoutAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.rest.action.user.RestChangePasswordAction;
import org.elasticsearch.xpack.security.rest.action.user.RestDeleteUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestGetUserPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.user.RestGetUsersAction;
import org.elasticsearch.xpack.security.rest.action.user.RestHasPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestSetEnabledAction;
import org.elasticsearch.xpack.security.support.ExtensionComponents;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityStatusChangeListener;
import org.elasticsearch.xpack.security.transport.SecurityHttpSettings;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4HttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4ServerTransport;
import org.elasticsearch.xpack.security.transport.nio.SecurityNioHttpServerTransport;
import org.elasticsearch.xpack.security.transport.nio.SecurityNioTransport;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.API_KEY_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.INTERNAL_MAIN_INDEX_FORMAT;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_MAIN_TEMPLATE_7;

public class Security extends Plugin implements SystemIndexPlugin, IngestPlugin, NetworkPlugin, ClusterPlugin,
        DiscoveryPlugin, MapperPlugin, ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(Security.class);

    private final Settings settings;
    private final boolean enabled;
    /* what a PITA that we need an extra indirection to initialize this. Yet, once we got rid of guice we can thing about how
     * to fix this or make it simpler. Today we need several service that are created in createComponents but we need to register
     * an instance of TransportInterceptor way earlier before createComponents is called. */
    private final SetOnce<TransportInterceptor> securityInterceptor = new SetOnce<>();
    private final SetOnce<IPFilter> ipFilter = new SetOnce<>();
    private final SetOnce<AuthenticationService> authcService = new SetOnce<>();
    private final SetOnce<SecondaryAuthenticator> secondayAuthc = new SetOnce<>();
    private final SetOnce<AuditTrailService> auditTrailService = new SetOnce<>();
    private final SetOnce<SecurityContext> securityContext = new SetOnce<>();
    private final SetOnce<ThreadContext> threadContext = new SetOnce<>();
    private final SetOnce<TokenService> tokenService = new SetOnce<>();
    private final SetOnce<SecurityActionFilter> securityActionFilter = new SetOnce<>();
    private final SetOnce<SecurityIndexManager> securityIndex = new SetOnce<>();
    private final SetOnce<SharedGroupFactory> sharedGroupFactory = new SetOnce<>();
    private final SetOnce<NioGroupFactory> nioGroupFactory = new SetOnce<>();
    private final SetOnce<DocumentSubsetBitsetCache> dlsBitsetCache = new SetOnce<>();
    private final SetOnce<List<BootstrapCheck>> bootstrapChecks = new SetOnce<>();
    private final List<SecurityExtension> securityExtensions = new ArrayList<>();

    public Security(Settings settings, final Path configPath) {
        this(settings, configPath, Collections.emptyList());
    }

    Security(Settings settings, final Path configPath, List<SecurityExtension> extensions) {
        // TODO This is wrong. Settings can change after this. We should use the settings from createComponents
        this.settings = settings;
        // TODO this is wrong, we should only use the environment that is provided to createComponents
        this.enabled = XPackSettings.SECURITY_ENABLED.get(settings);
        if (enabled) {
            runStartupChecks(settings);
            // we load them all here otherwise we can't access secure settings since they are closed once the checks are
            // fetched

            Automatons.updateConfiguration(settings);
        } else {
            this.bootstrapChecks.set(Collections.emptyList());
        }
        this.securityExtensions.addAll(extensions);

    }

    private static void runStartupChecks(Settings settings) {
        validateRealmSettings(settings);
        if (XPackSettings.FIPS_MODE_ENABLED.get(settings)) {
            validateForFips(settings);
        }
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }
    protected SSLService getSslService() { return XPackPlugin.getSharedSslService(); }
    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver expressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        try {
            return createComponents(client, threadPool, clusterService, resourceWatcherService, scriptService, xContentRegistry,
                environment, expressionResolver);
        } catch (final Exception e) {
            throw new IllegalStateException("security initialization failed", e);
        }
    }

    // pkg private for testing - tests want to pass in their set of extensions hence we are not using the extension service directly
    Collection<Object> createComponents(Client client, ThreadPool threadPool, ClusterService clusterService,
                                        ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                        NamedXContentRegistry xContentRegistry, Environment environment,
                                        IndexNameExpressionResolver expressionResolver) throws Exception {
        if (enabled == false) {
            return Collections.singletonList(new SecurityUsageServices(null, null, null, null));
        }

        // We need to construct the checks here while the secure settings are still available.
        // If we wait until #getBoostrapChecks the secure settings will have been cleared/closed.
        final List<BootstrapCheck> checks = new ArrayList<>();
        checks.addAll(Arrays.asList(
            new ApiKeySSLBootstrapCheck(),
            new TokenSSLBootstrapCheck(),
            new PkiRealmBootstrapCheck(getSslService()),
            new TLSLicenseBootstrapCheck()));
        checks.addAll(InternalRealms.getBootstrapChecks(settings, environment));
        this.bootstrapChecks.set(Collections.unmodifiableList(checks));

        threadContext.set(threadPool.getThreadContext());
        List<Object> components = new ArrayList<>();
        securityContext.set(new SecurityContext(settings, threadPool.getThreadContext()));
        components.add(securityContext.get());

        // audit trail service construction
        final List<AuditTrail> auditTrails = XPackSettings.AUDIT_ENABLED.get(settings)
                ? Collections.singletonList(new LoggingAuditTrail(settings, clusterService, threadPool))
                : Collections.emptyList();
        final AuditTrailService auditTrailService = new AuditTrailService(auditTrails, getLicenseState());
        components.add(auditTrailService);
        this.auditTrailService.set(auditTrailService);

        securityIndex.set(SecurityIndexManager.buildSecurityMainIndexManager(client, clusterService));

        final TokenService tokenService = new TokenService(settings, Clock.systemUTC(), client, getLicenseState(), securityContext.get(),
            securityIndex.get(), SecurityIndexManager.buildSecurityTokensIndexManager(client, clusterService), clusterService);
        this.tokenService.set(tokenService);
        components.add(tokenService);

        // realms construction
        final NativeUsersStore nativeUsersStore = new NativeUsersStore(settings, client, securityIndex.get());
        final NativeRoleMappingStore nativeRoleMappingStore = new NativeRoleMappingStore(settings, client, securityIndex.get(),
            scriptService);
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        final ReservedRealm reservedRealm = new ReservedRealm(environment, settings, nativeUsersStore,
                anonymousUser, securityIndex.get(), threadPool);
        final SecurityExtension.SecurityComponents extensionComponents = new ExtensionComponents(environment, client, clusterService,
            resourceWatcherService, nativeRoleMappingStore);
        Map<String, Realm.Factory> realmFactories = new HashMap<>(InternalRealms.getFactories(threadPool, resourceWatcherService,
                getSslService(), nativeUsersStore, nativeRoleMappingStore, securityIndex.get()));
        for (SecurityExtension extension : securityExtensions) {
            Map<String, Realm.Factory> newRealms = extension.getRealms(extensionComponents);
            for (Map.Entry<String, Realm.Factory> entry : newRealms.entrySet()) {
                if (realmFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Realm type [" + entry.getKey() + "] is already registered");
                }
            }
        }
        final Realms realms =
            new Realms(settings, environment, realmFactories, getLicenseState(), threadPool.getThreadContext(), reservedRealm);
        components.add(nativeUsersStore);
        components.add(nativeRoleMappingStore);
        components.add(realms);
        components.add(reservedRealm);

        securityIndex.get().addIndexStateListener(nativeRoleMappingStore::onSecurityIndexStateChange);

        final NativePrivilegeStore privilegeStore = new NativePrivilegeStore(settings, client, securityIndex.get());
        components.add(privilegeStore);

        dlsBitsetCache.set(new DocumentSubsetBitsetCache(settings, threadPool));
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(settings);
        final FileRolesStore fileRolesStore = new FileRolesStore(settings, environment, resourceWatcherService, getLicenseState(),
            xContentRegistry);
        final NativeRolesStore nativeRolesStore = new NativeRolesStore(settings, client, getLicenseState(), securityIndex.get());
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();
        List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> rolesProviders = new ArrayList<>();
        for (SecurityExtension extension : securityExtensions) {
            rolesProviders.addAll(extension.getRolesProviders(extensionComponents));
        }

        final ApiKeyService apiKeyService = new ApiKeyService(settings, Clock.systemUTC(), client, getLicenseState(), securityIndex.get(),
            clusterService, threadPool);
        components.add(apiKeyService);
        final CompositeRolesStore allRolesStore = new CompositeRolesStore(settings, fileRolesStore, nativeRolesStore, reservedRolesStore,
            privilegeStore, rolesProviders, threadPool.getThreadContext(), getLicenseState(), fieldPermissionsCache, apiKeyService,
            dlsBitsetCache.get(), new DeprecationRoleDescriptorConsumer(clusterService, threadPool));
        securityIndex.get().addIndexStateListener(allRolesStore::onSecurityIndexStateChange);

        // to keep things simple, just invalidate all cached entries on license change. this happens so rarely that the impact should be
        // minimal
        getLicenseState().addListener(allRolesStore::invalidateAll);
        getLicenseState().addListener(new SecurityStatusChangeListener(getLicenseState()));

        final AuthenticationFailureHandler failureHandler = createAuthenticationFailureHandler(realms, extensionComponents);
        authcService.set(new AuthenticationService(settings, realms, auditTrailService, failureHandler, threadPool,
                anonymousUser, tokenService, apiKeyService));
        components.add(authcService.get());
        securityIndex.get().addIndexStateListener(authcService.get()::onSecurityIndexStateChange);

        Set<RequestInterceptor> requestInterceptors = Sets.newHashSet(
            new ResizeRequestInterceptor(threadPool, getLicenseState(), auditTrailService),
            new IndicesAliasesRequestInterceptor(threadPool.getThreadContext(), getLicenseState(), auditTrailService));
        if (XPackSettings.DLS_FLS_ENABLED.get(settings)) {
            requestInterceptors.addAll(Arrays.asList(
                new SearchRequestInterceptor(threadPool, getLicenseState()),
                new UpdateRequestInterceptor(threadPool, getLicenseState()),
                new BulkShardRequestInterceptor(threadPool, getLicenseState())
            ));
        }
        requestInterceptors = Collections.unmodifiableSet(requestInterceptors);

        final AuthorizationService authzService = new AuthorizationService(settings, allRolesStore, clusterService,
            auditTrailService, failureHandler, threadPool, anonymousUser, getAuthorizationEngine(), requestInterceptors,
            getLicenseState(), expressionResolver);

        components.add(nativeRolesStore); // used by roles actions
        components.add(reservedRolesStore); // used by roles actions
        components.add(allRolesStore); // for SecurityInfoTransportAction and clear roles cache
        components.add(authzService);

        final SecondaryAuthenticator secondaryAuthenticator = new SecondaryAuthenticator(securityContext.get(), authcService.get());
        this.secondayAuthc.set(secondaryAuthenticator);
        components.add(secondaryAuthenticator);

        ipFilter.set(new IPFilter(settings, auditTrailService, clusterService.getClusterSettings(), getLicenseState()));
        components.add(ipFilter.get());
        DestructiveOperations destructiveOperations = new DestructiveOperations(settings, clusterService.getClusterSettings());
        securityInterceptor.set(new SecurityServerTransportInterceptor(settings, threadPool, authcService.get(),
                authzService, getLicenseState(), getSslService(), securityContext.get(), destructiveOperations, clusterService));

        securityActionFilter.set(new SecurityActionFilter(authcService.get(), authzService, getLicenseState(),
            threadPool, securityContext.get(), destructiveOperations));

        components.add(new SecurityUsageServices(realms, allRolesStore, nativeRoleMappingStore, ipFilter.get()));

        return components;
    }

    private AuthorizationEngine getAuthorizationEngine() {
        AuthorizationEngine authorizationEngine = null;
        String extensionName = null;
        for (SecurityExtension extension : securityExtensions) {
            final AuthorizationEngine extensionEngine = extension.getAuthorizationEngine(settings);
            if (extensionEngine != null && authorizationEngine != null) {
                throw new IllegalStateException("Extensions [" + extensionName + "] and [" + extension.toString() + "] "
                    + "both set an authorization engine");
            }
            authorizationEngine = extensionEngine;
            extensionName = extension.toString();
        }

        if (authorizationEngine != null) {
            logger.debug("Using authorization engine from extension [" + extensionName + "]");
        }
        return authorizationEngine;
    }

    private AuthenticationFailureHandler createAuthenticationFailureHandler(final Realms realms,
                                                                            final SecurityExtension.SecurityComponents components) {
        AuthenticationFailureHandler failureHandler = null;
        String extensionName = null;
        for (SecurityExtension extension : securityExtensions) {
            AuthenticationFailureHandler extensionFailureHandler = extension.getAuthenticationFailureHandler(components);
            if (extensionFailureHandler != null && failureHandler != null) {
                throw new IllegalStateException("Extensions [" + extensionName + "] and [" + extension.toString() + "] "
                        + "both set an authentication failure handler");
            }
            failureHandler = extensionFailureHandler;
            extensionName = extension.toString();
        }
        if (failureHandler == null) {
            logger.debug("Using default authentication failure handler");
            final Map<String, List<String>> defaultFailureResponseHeaders = new HashMap<>();
            realms.asList().stream().forEach((realm) -> {
                Map<String, List<String>> realmFailureHeaders = realm.getAuthenticationFailureHeaders();
                realmFailureHeaders.entrySet().stream().forEach((e) -> {
                    String key = e.getKey();
                    e.getValue().stream()
                            .filter(v -> defaultFailureResponseHeaders.computeIfAbsent(key, x -> new ArrayList<>()).contains(v) == false)
                            .forEach(v -> defaultFailureResponseHeaders.get(key).add(v));
                });
            });

            if (TokenService.isTokenServiceEnabled(settings)) {
                String bearerScheme = "Bearer realm=\"" + XPackField.SECURITY + "\"";
                if (defaultFailureResponseHeaders.computeIfAbsent("WWW-Authenticate", x -> new ArrayList<>())
                        .contains(bearerScheme) == false) {
                    defaultFailureResponseHeaders.get("WWW-Authenticate").add(bearerScheme);
                }
            }
            if (API_KEY_SERVICE_ENABLED_SETTING.get(settings)) {
                final String apiKeyScheme = "ApiKey";
                if (defaultFailureResponseHeaders.computeIfAbsent("WWW-Authenticate", x -> new ArrayList<>())
                    .contains(apiKeyScheme) == false) {
                    defaultFailureResponseHeaders.get("WWW-Authenticate").add(apiKeyScheme);
                }
            }
            failureHandler = new DefaultAuthenticationFailureHandler(defaultFailureResponseHeaders);
        } else {
            logger.debug("Using authentication failure handler from extension [" + extensionName + "]");
        }
        return failureHandler;
    }

    @Override
    public Settings additionalSettings() {
        return additionalSettings(settings, enabled);
    }

    // visible for tests
    static Settings additionalSettings(final Settings settings, final boolean enabled) {
        if (enabled) {
            final Settings.Builder builder = Settings.builder();

            builder.put(SecuritySettings.addTransportSettings(settings));

            if (NetworkModule.HTTP_TYPE_SETTING.exists(settings)) {
                final String httpType = NetworkModule.HTTP_TYPE_SETTING.get(settings);
                if (httpType.equals(SecurityField.NAME4) || httpType.equals(SecurityField.NIO)) {
                    SecurityHttpSettings.overrideSettings(builder, settings);
                } else {
                    final String message = String.format(
                            Locale.ROOT,
                            "http type setting [%s] must be [%s] or [%s] but is [%s]",
                            NetworkModule.HTTP_TYPE_KEY,
                            SecurityField.NAME4,
                            SecurityField.NIO,
                            httpType);
                    throw new IllegalArgumentException(message);
                }
            } else {
                // default to security4
                builder.put(NetworkModule.HTTP_TYPE_KEY, SecurityField.NAME4);
                SecurityHttpSettings.overrideSettings(builder, settings);
            }
            builder.put(SecuritySettings.addUserSettings(settings));
            return builder.build();
        } else {
            return Settings.EMPTY;
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return getSettings(securityExtensions);
    }

        /**
         * Get the {@link Setting setting configuration} for all security components, including those defined in extensions.
         */
    public static List<Setting<?>> getSettings(List<SecurityExtension> securityExtensions) {
        List<Setting<?>> settingsList = new ArrayList<>();

        // The following just apply in node mode
        settingsList.add(XPackSettings.FIPS_MODE_ENABLED);

        SSLService.registerSettings(settingsList);
        // IP Filter settings
        IPFilter.addSettings(settingsList);

        // audit settings
        LoggingAuditTrail.registerSettings(settingsList);

        // authentication and authorization settings
        AnonymousUser.addSettings(settingsList);
        settingsList.addAll(InternalRealmsSettings.getSettings());
        ReservedRealm.addSettings(settingsList);
        AuthenticationService.addSettings(settingsList);
        AuthorizationService.addSettings(settingsList);
        Automatons.addSettings(settingsList);
        settingsList.addAll(CompositeRolesStore.getSettings());
        settingsList.addAll(DocumentSubsetBitsetCache.getSettings());
        settingsList.add(FieldPermissionsCache.CACHE_SIZE_SETTING);
        settingsList.add(TokenService.TOKEN_EXPIRATION);
        settingsList.add(TokenService.DELETE_INTERVAL);
        settingsList.add(TokenService.DELETE_TIMEOUT);
        settingsList.addAll(SSLConfigurationSettings.getProfileSettings());
        settingsList.add(ApiKeyService.PASSWORD_HASHING_ALGORITHM);
        settingsList.add(ApiKeyService.DELETE_TIMEOUT);
        settingsList.add(ApiKeyService.DELETE_INTERVAL);
        settingsList.add(ApiKeyService.CACHE_HASH_ALGO_SETTING);
        settingsList.add(ApiKeyService.CACHE_MAX_KEYS_SETTING);
        settingsList.add(ApiKeyService.CACHE_TTL_SETTING);

        // hide settings
        settingsList.add(Setting.listSetting(SecurityField.setting("hide_settings"), Collections.emptyList(), Function.identity(),
                Property.NodeScope, Property.Filtered));
        return settingsList;
    }

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        Set<RestHeaderDefinition> headers = new HashSet<>();
        headers.add(new RestHeaderDefinition(UsernamePasswordToken.BASIC_AUTH_HEADER, false));
        headers.add(new RestHeaderDefinition(SecondaryAuthenticator.SECONDARY_AUTH_HEADER_NAME, false));
        if (XPackSettings.AUDIT_ENABLED.get(settings)) {
            headers.add(new RestHeaderDefinition(AuditTrail.X_FORWARDED_FOR_HEADER, true));
        }
        if (AuthenticationServiceField.RUN_AS_ENABLED.get(settings)) {
            headers.add(new RestHeaderDefinition(AuthenticationServiceField.RUN_AS_USER_HEADER, false));
        }
        return headers;
    }

    @Override
    public List<String> getSettingsFilter() {
        List<String> asArray = settings.getAsList(SecurityField.setting("hide_settings"));
        ArrayList<String> settingsFilter = new ArrayList<>(asArray);
        // hide settings where we don't define them - they are part of a group...
        settingsFilter.add("transport.profiles.*." + SecurityField.setting("*"));
        return settingsFilter;
    }

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
       return bootstrapChecks.get();
    }

    @Override
    public void onIndexModule(IndexModule module) {
        if (enabled) {
            assert getLicenseState() != null;
            if (XPackSettings.DLS_FLS_ENABLED.get(settings)) {
                assert dlsBitsetCache.get() != null;
                module.setReaderWrapper(indexService ->
                        new SecurityIndexReaderWrapper(
                                shardId -> indexService.newQueryShardContext(shardId.id(),
                                // we pass a null index reader, which is legal and will disable rewrite optimizations
                                // based on index statistics, which is probably safer...
                                null,
                                () -> {
                                    throw new IllegalArgumentException("permission filters are not allowed to use the current timestamp");

                                }, null),
                                dlsBitsetCache.get(),
                                securityContext.get(),
                                getLicenseState(),
                                indexService.getScriptService()));
                /*
                 * We need to forcefully overwrite the query cache implementation to use security's opt-out query cache implementation. This
                 * implementation disables the query cache if field level security is used for a particular request. We have to forcefully
                 * overwrite the query cache implementation to prevent data leakage to unauthorized users.
                 */
                module.forceQueryCacheProvider(
                        (settings, cache) -> {
                            final OptOutQueryCache queryCache =
                                    new OptOutQueryCache(settings, cache, threadContext.get(), getLicenseState());
                            queryCache.listenForLicenseStateChanges();
                            return queryCache;
                        });
            }

            // in order to prevent scroll ids from being maliciously crafted and/or guessed, a listener is added that
            // attaches information to the scroll context so that we can validate the user that created the scroll against
            // the user that is executing a scroll operation
            module.addSearchOperationListener(
                    new SecuritySearchOperationListener(securityContext.get(), getLicenseState(), auditTrailService.get()));
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.SECURITY, SecurityUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.SECURITY, SecurityInfoTransportAction.class);
        if (enabled == false) {
            return Arrays.asList(usageAction, infoAction);
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
                new ActionHandler<>(GetUserPrivilegesAction.INSTANCE, TransportGetUserPrivilegesAction.class),
                new ActionHandler<>(GetRoleMappingsAction.INSTANCE, TransportGetRoleMappingsAction.class),
                new ActionHandler<>(PutRoleMappingAction.INSTANCE, TransportPutRoleMappingAction.class),
                new ActionHandler<>(DeleteRoleMappingAction.INSTANCE, TransportDeleteRoleMappingAction.class),
                new ActionHandler<>(CreateTokenAction.INSTANCE, TransportCreateTokenAction.class),
                new ActionHandler<>(InvalidateTokenAction.INSTANCE, TransportInvalidateTokenAction.class),
                new ActionHandler<>(GetCertificateInfoAction.INSTANCE, TransportGetCertificateInfoAction.class),
                new ActionHandler<>(RefreshTokenAction.INSTANCE, TransportRefreshTokenAction.class),
                new ActionHandler<>(SamlPrepareAuthenticationAction.INSTANCE, TransportSamlPrepareAuthenticationAction.class),
                new ActionHandler<>(SamlAuthenticateAction.INSTANCE, TransportSamlAuthenticateAction.class),
                new ActionHandler<>(SamlLogoutAction.INSTANCE, TransportSamlLogoutAction.class),
                new ActionHandler<>(SamlInvalidateSessionAction.INSTANCE, TransportSamlInvalidateSessionAction.class),
                new ActionHandler<>(OpenIdConnectPrepareAuthenticationAction.INSTANCE,
                    TransportOpenIdConnectPrepareAuthenticationAction.class),
                new ActionHandler<>(OpenIdConnectAuthenticateAction.INSTANCE, TransportOpenIdConnectAuthenticateAction.class),
                new ActionHandler<>(OpenIdConnectLogoutAction.INSTANCE, TransportOpenIdConnectLogoutAction.class),
                new ActionHandler<>(GetBuiltinPrivilegesAction.INSTANCE, TransportGetBuiltinPrivilegesAction.class),
                new ActionHandler<>(GetPrivilegesAction.INSTANCE, TransportGetPrivilegesAction.class),
                new ActionHandler<>(PutPrivilegesAction.INSTANCE, TransportPutPrivilegesAction.class),
                new ActionHandler<>(DeletePrivilegesAction.INSTANCE, TransportDeletePrivilegesAction.class),
                new ActionHandler<>(CreateApiKeyAction.INSTANCE, TransportCreateApiKeyAction.class),
                new ActionHandler<>(GrantApiKeyAction.INSTANCE, TransportGrantApiKeyAction.class),
                new ActionHandler<>(InvalidateApiKeyAction.INSTANCE, TransportInvalidateApiKeyAction.class),
                new ActionHandler<>(GetApiKeyAction.INSTANCE, TransportGetApiKeyAction.class),
                new ActionHandler<>(DelegatePkiAuthenticationAction.INSTANCE, TransportDelegatePkiAuthenticationAction.class),
                usageAction,
                infoAction);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        if (enabled == false) {
            return emptyList();
        }
        return singletonList(securityActionFilter.get());
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return emptyList();
        }
        return Arrays.asList(
                new RestAuthenticateAction(settings, securityContext.get(), getLicenseState()),
                new RestClearRealmCacheAction(settings, getLicenseState()),
                new RestClearRolesCacheAction(settings, getLicenseState()),
                new RestGetUsersAction(settings, getLicenseState()),
                new RestPutUserAction(settings, getLicenseState()),
                new RestDeleteUserAction(settings, getLicenseState()),
                new RestGetRolesAction(settings, getLicenseState()),
                new RestPutRoleAction(settings, getLicenseState()),
                new RestDeleteRoleAction(settings, getLicenseState()),
                new RestChangePasswordAction(settings, securityContext.get(), getLicenseState()),
                new RestSetEnabledAction(settings, getLicenseState()),
                new RestHasPrivilegesAction(settings, securityContext.get(), getLicenseState()),
                new RestGetUserPrivilegesAction(settings, securityContext.get(), getLicenseState()),
                new RestGetRoleMappingsAction(settings, getLicenseState()),
                new RestPutRoleMappingAction(settings, getLicenseState()),
                new RestDeleteRoleMappingAction(settings, getLicenseState()),
                new RestGetTokenAction(settings, getLicenseState()),
                new RestInvalidateTokenAction(settings, getLicenseState()),
                new RestGetCertificateInfoAction(),
                new RestSamlPrepareAuthenticationAction(settings, getLicenseState()),
                new RestSamlAuthenticateAction(settings, getLicenseState()),
                new RestSamlLogoutAction(settings, getLicenseState()),
                new RestSamlInvalidateSessionAction(settings, getLicenseState()),
                new RestOpenIdConnectPrepareAuthenticationAction(settings, getLicenseState()),
                new RestOpenIdConnectAuthenticateAction(settings, getLicenseState()),
                new RestOpenIdConnectLogoutAction(settings, getLicenseState()),
                new RestGetBuiltinPrivilegesAction(settings, getLicenseState()),
                new RestGetPrivilegesAction(settings, getLicenseState()),
                new RestPutPrivilegesAction(settings, getLicenseState()),
                new RestDeletePrivilegesAction(settings, getLicenseState()),
                new RestCreateApiKeyAction(settings, getLicenseState()),
                new RestGrantApiKeyAction(settings, getLicenseState()),
                new RestInvalidateApiKeyAction(settings, getLicenseState()),
                new RestGetApiKeyAction(settings, getLicenseState()),
                new RestDelegatePkiAuthenticationAction(settings, getLicenseState())
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(SetSecurityUserProcessor.TYPE,
            new SetSecurityUserProcessor.Factory(securityContext::get, this::getLicenseState));
    }

    /**
     * Realm settings were changed in 7.0. This method validates that the settings in use on this node match the new style of setting.
     * In 6.x a realm config would be
     * <pre>
     *   xpack.security.authc.realms.file1.type: file
     *   xpack.security.authc.realms.file1.order: 0
     * </pre>
     * In 7.x this realm should be
     * <pre>
     *   xpack.security.authc.realms.file.file1.order: 0
     * </pre>
     * If confronted with an old style config, the ES Settings validation would simply fail with an error such as
     * <em>unknown setting [xpack.security.authc.realms.file1.order]</em>. This validation method provides an error that is easier to
     * understand and take action on.
     */
     static void validateRealmSettings(Settings settings) {
        final Set<String> badRealmSettings = settings.keySet().stream()
            .filter(k -> k.startsWith(RealmSettings.PREFIX))
            .filter(key -> {
                final String suffix = key.substring(RealmSettings.PREFIX.length());
                // suffix-part, only contains a single '.'
                return suffix.indexOf('.') == suffix.lastIndexOf('.');
            })
            .collect(Collectors.toSet());
        if (badRealmSettings.isEmpty() == false) {
            String sampleRealmSetting = RealmSettings.realmSettingPrefix(new RealmConfig.RealmIdentifier("file", "my_file")) + "order";
            throw new IllegalArgumentException("Incorrect realm settings found. " +
                "Realm settings have been changed to include the type as part of the setting key.\n" +
                "For example '" + sampleRealmSetting + "'\n" +
                "Found invalid config: " + Strings.collectionToDelimitedString(badRealmSettings, ", ") + "\n" +
                "Please see the breaking changes documentation."
            );
        }
    }

    static void validateForFips(Settings settings) {
        final List<String> validationErrors = new ArrayList<>();
        Settings keystoreTypeSettings = settings.filter(k -> k.endsWith("keystore.type"))
            .filter(k -> settings.get(k).equalsIgnoreCase("jks"));
        if (keystoreTypeSettings.isEmpty() == false) {
            validationErrors.add("JKS Keystores cannot be used in a FIPS 140 compliant JVM. Please " +
                "revisit [" + keystoreTypeSettings.toDelimitedString(',') + "] settings");
        }
        Settings keystorePathSettings = settings.filter(k -> k.endsWith("keystore.path"))
            .filter(k -> settings.hasValue(k.replace(".path", ".type")) == false);
        if (keystorePathSettings.isEmpty() == false && SSLConfigurationSettings.inferKeyStoreType(null).equals("jks")) {
            validationErrors.add("JKS Keystores cannot be used in a FIPS 140 compliant JVM. Please " +
                "revisit [" + keystorePathSettings.toDelimitedString(',') + "] settings");
        }
        final String selectedAlgorithm = XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings);
        if (selectedAlgorithm.toLowerCase(Locale.ROOT).startsWith("pbkdf2") == false) {
            validationErrors.add("Only PBKDF2 is allowed for password hashing in a FIPS 140 JVM. Please set the " +
                "appropriate value for [ " + XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey() + " ] setting.");
        }

        if (validationErrors.isEmpty() == false) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Validation for FIPS 140 mode failed: \n");
            int index = 0;
            for (String error : validationErrors) {
                sb.append(++index).append(": ").append(error).append(";\n");
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }

    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        if (enabled == false) { // don't register anything if we are not enabled
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
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService) {
        if (enabled == false) { // don't register anything if we are not enabled
            return Collections.emptyMap();
        }

        IPFilter ipFilter = this.ipFilter.get();
        return Map.of(
                // security based on Netty 4
                SecurityField.NAME4,
                () -> new SecurityNetty4ServerTransport(
                        settings,
                        Version.CURRENT,
                        threadPool,
                        networkService,
                        pageCacheRecycler,
                        namedWriteableRegistry,
                        circuitBreakerService,
                        ipFilter,
                        getSslService(),
                        getNettySharedGroupFactory(settings)),
                // security based on NIO
                SecurityField.NIO,
                () -> new SecurityNioTransport(settings,
                        Version.CURRENT,
                        threadPool,
                        networkService,
                        pageCacheRecycler,
                        namedWriteableRegistry,
                        circuitBreakerService,
                        ipFilter,
                        getSslService(),
                        getNioGroupFactory(settings)));
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                        PageCacheRecycler pageCacheRecycler,
                                                                        CircuitBreakerService circuitBreakerService,
                                                                        NamedXContentRegistry xContentRegistry,
                                                                        NetworkService networkService,
                                                                        HttpServerTransport.Dispatcher dispatcher,
                                                                        ClusterSettings clusterSettings) {
        if (enabled == false) { // don't register anything if we are not enabled
            return Collections.emptyMap();
        }

        Map<String, Supplier<HttpServerTransport>> httpTransports = new HashMap<>();
        httpTransports.put(SecurityField.NAME4, () -> new SecurityNetty4HttpServerTransport(settings, networkService, bigArrays,
            ipFilter.get(), getSslService(), threadPool, xContentRegistry, dispatcher, clusterSettings,
            getNettySharedGroupFactory(settings)));
        httpTransports.put(SecurityField.NIO, () -> new SecurityNioHttpServerTransport(settings, networkService, bigArrays,
            pageCacheRecycler, threadPool, xContentRegistry, dispatcher, ipFilter.get(), getSslService(), getNioGroupFactory(settings),
            clusterSettings));

        return httpTransports;
    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        if (enabled == false) {
            return null;
        }
        final boolean ssl = HTTP_SSL_ENABLED.get(settings);
        final SSLConfiguration httpSSLConfig = getSslService().getHttpTransportSSLConfiguration();
        boolean extractClientCertificate = ssl && getSslService().isSSLClientAuthEnabled(httpSSLConfig);
        return handler -> new SecurityRestFilter(getLicenseState(), threadContext, authcService.get(), secondayAuthc.get(),
            handler, extractClientCertificate);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        if (enabled) {
            return Collections.singletonList(
                    new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool",
                        false));
        }
        return Collections.emptyList();
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return templates -> {
            // .security index is not managed by using templates anymore
            templates.remove(SECURITY_MAIN_TEMPLATE_7);
            templates.remove("security_audit_log");
            return templates;
        };
    }

    @Override
    public Function<String, Predicate<String>> getFieldFilter() {
        if (enabled) {
            return index -> {
                XPackLicenseState licenseState = getLicenseState();
                if (licenseState.isSecurityEnabled() == false || licenseState.isAllowed(Feature.SECURITY_DLS_FLS) == false) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                IndicesAccessControl indicesAccessControl = threadContext.get().getTransient(
                        AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                IndicesAccessControl.IndexAccessControl indexPermissions = indicesAccessControl.getIndexPermissions(index);
                if (indexPermissions == null) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                if (indexPermissions.isGranted() == false) {
                    throw new IllegalStateException("unexpected call to getFieldFilter for index [" + index + "] which is not granted");
                }
                FieldPermissions fieldPermissions = indexPermissions.getFieldPermissions();
                if (fieldPermissions.hasFieldLevelSecurity() == false) {
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
            return new ValidateUpgradedSecurityIndex()
                .andThen(new ValidateLicenseForFIPS(XPackSettings.FIPS_MODE_ENABLED.get(settings)));
        }
        return null;
    }

    static final class ValidateUpgradedSecurityIndex implements BiConsumer<DiscoveryNode, ClusterState> {
        @Override
        public void accept(DiscoveryNode node, ClusterState state) {
            if (state.getNodes().getMinNodeVersion().before(Version.V_7_0_0)) {
                IndexMetadata indexMetadata = state.getMetadata().getIndices().get(SECURITY_MAIN_ALIAS);
                if (indexMetadata != null && INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()) < INTERNAL_MAIN_INDEX_FORMAT) {
                    throw new IllegalStateException("Security index is not on the current version [" + INTERNAL_MAIN_INDEX_FORMAT + "] - " +
                        "The Upgrade API must be run for 7.x nodes to join the cluster");
                }
            }
        }
    }

    static final class ValidateLicenseForFIPS implements BiConsumer<DiscoveryNode, ClusterState> {
        private final boolean inFipsMode;

        ValidateLicenseForFIPS(boolean inFipsMode) {
            this.inFipsMode = inFipsMode;
        }

        @Override
        public void accept(DiscoveryNode node, ClusterState state) {
            if (inFipsMode) {
                License license = LicenseService.getLicense(state.metadata());
                if (license != null &&
                    XPackLicenseState.isFipsAllowedForOperationMode(license.operationMode()) == false) {
                    throw new IllegalStateException("FIPS mode cannot be used with a [" + license.operationMode() +
                        "] license. It is only allowed with a Platinum or Trial license.");

                }
            }
        }
    }

    @Override
    public void reloadSPI(ClassLoader loader) {
        securityExtensions.addAll(SecurityExtension.loadExtensions(loader));
    }

    private synchronized NioGroupFactory getNioGroupFactory(Settings settings) {
         if (nioGroupFactory.get() != null) {
             assert nioGroupFactory.get().getSettings().equals(settings) : "Different settings than originally provided";
             return nioGroupFactory.get();
         } else {
            nioGroupFactory.set(new NioGroupFactory(settings, logger));
            return nioGroupFactory.get();
         }
    }
    private synchronized SharedGroupFactory getNettySharedGroupFactory(Settings settings) {
        if (sharedGroupFactory.get() != null) {
            assert sharedGroupFactory.get().getSettings().equals(settings) : "Different settings than originally provided";
            return sharedGroupFactory.get();
        } else {
            sharedGroupFactory.set(new SharedGroupFactory(settings));
            return sharedGroupFactory.get();
        }
     }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            new SystemIndexDescriptor(SECURITY_MAIN_ALIAS, "Contains Security configuration"),
            new SystemIndexDescriptor(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6, "Contains Security configuration"),
            new SystemIndexDescriptor(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7, "Contains Security configuration"),

            new SystemIndexDescriptor(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS, "Contains auth token data"),
            new SystemIndexDescriptor(RestrictedIndicesNames.INTERNAL_SECURITY_TOKENS_INDEX_7, "Contains auth token data")
        );
    }
}
