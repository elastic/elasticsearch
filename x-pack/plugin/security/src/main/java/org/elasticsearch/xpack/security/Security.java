/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.plugins.interceptor.RestInterceptorActionPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.netty4.AcceptChannelHandler;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.SecuritySettings;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlCompleteLogoutAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlLogoutAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlSpMetadataAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountNodesCredentialsAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
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
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetBitsetCache;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.SecurityIndexReaderWrapper;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.SimpleRole;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TransportTLSBootstrapCheck;
import org.elasticsearch.xpack.core.ssl.action.GetCertificateInfoAction;
import org.elasticsearch.xpack.core.ssl.action.TransportGetCertificateInfoAction;
import org.elasticsearch.xpack.core.ssl.rest.RestGetCertificateInfoAction;
import org.elasticsearch.xpack.security.action.TransportClearSecurityCacheAction;
import org.elasticsearch.xpack.security.action.TransportDelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.security.action.apikey.TransportBulkUpdateApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportCreateApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportCreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportGetApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportGrantApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportInvalidateApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportQueryApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportUpdateApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportUpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.security.action.enrollment.TransportKibanaEnrollmentAction;
import org.elasticsearch.xpack.security.action.enrollment.TransportNodeEnrollmentAction;
import org.elasticsearch.xpack.security.action.filter.SecurityActionFilter;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectLogoutAction;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.action.privilege.TransportClearPrivilegesCacheAction;
import org.elasticsearch.xpack.security.action.privilege.TransportDeletePrivilegesAction;
import org.elasticsearch.xpack.security.action.privilege.TransportGetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.security.action.privilege.TransportGetPrivilegesAction;
import org.elasticsearch.xpack.security.action.privilege.TransportPutPrivilegesAction;
import org.elasticsearch.xpack.security.action.profile.TransportActivateProfileAction;
import org.elasticsearch.xpack.security.action.profile.TransportGetProfilesAction;
import org.elasticsearch.xpack.security.action.profile.TransportProfileHasPrivilegesAction;
import org.elasticsearch.xpack.security.action.profile.TransportSetProfileEnabledAction;
import org.elasticsearch.xpack.security.action.profile.TransportSuggestProfilesAction;
import org.elasticsearch.xpack.security.action.profile.TransportUpdateProfileDataAction;
import org.elasticsearch.xpack.security.action.realm.TransportClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.role.TransportClearRolesCacheAction;
import org.elasticsearch.xpack.security.action.role.TransportDeleteRoleAction;
import org.elasticsearch.xpack.security.action.role.TransportGetRolesAction;
import org.elasticsearch.xpack.security.action.role.TransportPutRoleAction;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportDeleteRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportGetRoleMappingsAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportPutRoleMappingAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlAuthenticateAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlCompleteLogoutAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlInvalidateSessionAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlLogoutAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.action.saml.TransportSamlSpMetadataAction;
import org.elasticsearch.xpack.security.action.service.TransportCreateServiceAccountTokenAction;
import org.elasticsearch.xpack.security.action.service.TransportDeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.security.action.service.TransportGetServiceAccountAction;
import org.elasticsearch.xpack.security.action.service.TransportGetServiceAccountCredentialsAction;
import org.elasticsearch.xpack.security.action.service.TransportGetServiceAccountNodesCredentialsAction;
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
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.jwt.JwtRealm;
import org.elasticsearch.xpack.security.authc.service.CachingServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.FileServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.DlsFlsRequestCacheDifferentiator;
import org.elasticsearch.xpack.security.authz.SecuritySearchOperationListener;
import org.elasticsearch.xpack.security.authz.accesscontrol.OptOutQueryCache;
import org.elasticsearch.xpack.security.authz.interceptor.BulkShardRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.DlsFlsLicenseRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.IndicesAliasesRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.RequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.ResizeRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.SearchRequestCacheDisablingInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.SearchRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.ShardSearchRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.UpdateRequestInterceptor;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.DeprecationRoleDescriptorConsumer;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.authz.store.RoleProviders;
import org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor;
import org.elasticsearch.xpack.security.operator.FileOperatorUsersStore;
import org.elasticsearch.xpack.security.operator.DefaultOperatorOnlyRegistry;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.rest.SecurityRestFilter;
import org.elasticsearch.xpack.security.rest.action.RestAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.RestDelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestBulkUpdateApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestClearApiKeyCacheAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestCreateApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestCreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGetApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGrantApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestInvalidateApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestQueryApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestUpdateApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestUpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.enrollment.RestKibanaEnrollAction;
import org.elasticsearch.xpack.security.rest.action.enrollment.RestNodeEnrollmentAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectLogoutAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestClearPrivilegesCacheAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestDeletePrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestGetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestGetPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.privilege.RestPutPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.profile.RestActivateProfileAction;
import org.elasticsearch.xpack.security.rest.action.profile.RestDisableProfileAction;
import org.elasticsearch.xpack.security.rest.action.profile.RestEnableProfileAction;
import org.elasticsearch.xpack.security.rest.action.profile.RestGetProfilesAction;
import org.elasticsearch.xpack.security.rest.action.profile.RestSuggestProfilesAction;
import org.elasticsearch.xpack.security.rest.action.profile.RestUpdateProfileDataAction;
import org.elasticsearch.xpack.security.rest.action.realm.RestClearRealmCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestClearRolesCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestDeleteRoleAction;
import org.elasticsearch.xpack.security.rest.action.role.RestGetRolesAction;
import org.elasticsearch.xpack.security.rest.action.role.RestPutRoleAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestDeleteRoleMappingAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestGetRoleMappingsAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestPutRoleMappingAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlCompleteLogoutAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlInvalidateSessionAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlLogoutAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlSpMetadataAction;
import org.elasticsearch.xpack.security.rest.action.service.RestClearServiceAccountTokenStoreCacheAction;
import org.elasticsearch.xpack.security.rest.action.service.RestCreateServiceAccountTokenAction;
import org.elasticsearch.xpack.security.rest.action.service.RestDeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.security.rest.action.service.RestGetServiceAccountAction;
import org.elasticsearch.xpack.security.rest.action.service.RestGetServiceAccountCredentialsAction;
import org.elasticsearch.xpack.security.rest.action.user.RestChangePasswordAction;
import org.elasticsearch.xpack.security.rest.action.user.RestDeleteUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestGetUserPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.user.RestGetUsersAction;
import org.elasticsearch.xpack.security.rest.action.user.RestHasPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.user.RestProfileHasPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestSetEnabledAction;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.ExtensionComponents;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;
import org.elasticsearch.xpack.security.transport.RemoteClusterCredentialsResolver;
import org.elasticsearch.xpack.security.transport.SecurityHttpSettings;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4ServerTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.XPackSettings.API_KEY_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED;
import static org.elasticsearch.xpack.security.transport.SSLEngineUtils.extractClientCertificates;

public class Security extends Plugin
    implements
        SystemIndexPlugin,
        IngestPlugin,
        NetworkPlugin,
        ClusterPlugin,
        ClusterCoordinationPlugin,
        MapperPlugin,
        ExtensiblePlugin,
        SearchPlugin,
        RestInterceptorActionPlugin {

    public static final String SECURITY_CRYPTO_THREAD_POOL_NAME = XPackField.SECURITY + "-crypto";

    // TODO: ip filtering does not actually track license usage yet
    public static final LicensedFeature.Momentary IP_FILTERING_FEATURE = LicensedFeature.momentaryLenient(
        null,
        "security-ip-filtering",
        License.OperationMode.GOLD
    );
    public static final LicensedFeature.Momentary AUDITING_FEATURE = LicensedFeature.momentary(
        null,
        "security-auditing",
        License.OperationMode.GOLD
    );
    public static final LicensedFeature.Momentary TOKEN_SERVICE_FEATURE = LicensedFeature.momentary(
        null,
        "security-token-service",
        License.OperationMode.STANDARD
    );

    private static final String REALMS_FEATURE_FAMILY = "security-realms";
    // Builtin realms (file/native) realms are Basic licensed, so don't need to be checked or tracked
    // Some realms (LDAP, AD, PKI) are Gold+
    public static final LicensedFeature.Persistent LDAP_REALM_FEATURE = LicensedFeature.persistent(
        REALMS_FEATURE_FAMILY,
        "ldap",
        License.OperationMode.GOLD
    );
    public static final LicensedFeature.Persistent AD_REALM_FEATURE = LicensedFeature.persistent(
        REALMS_FEATURE_FAMILY,
        "active-directory",
        License.OperationMode.GOLD
    );
    public static final LicensedFeature.Persistent PKI_REALM_FEATURE = LicensedFeature.persistent(
        REALMS_FEATURE_FAMILY,
        "pki",
        License.OperationMode.GOLD
    );
    // SSO realms are Platinum+
    public static final LicensedFeature.Persistent SAML_REALM_FEATURE = LicensedFeature.persistent(
        REALMS_FEATURE_FAMILY,
        "saml",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Persistent OIDC_REALM_FEATURE = LicensedFeature.persistent(
        REALMS_FEATURE_FAMILY,
        "oidc",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Persistent JWT_REALM_FEATURE = LicensedFeature.persistent(
        REALMS_FEATURE_FAMILY,
        "jwt",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Persistent KERBEROS_REALM_FEATURE = LicensedFeature.persistent(
        REALMS_FEATURE_FAMILY,
        "kerberos",
        License.OperationMode.PLATINUM
    );
    // Custom realms are Platinum+
    public static final LicensedFeature.Persistent CUSTOM_REALMS_FEATURE = LicensedFeature.persistent(
        REALMS_FEATURE_FAMILY,
        "custom",
        License.OperationMode.PLATINUM
    );

    public static final LicensedFeature.Momentary DELEGATED_AUTHORIZATION_FEATURE = LicensedFeature.momentary(
        null,
        "security-delegated-authorization",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Momentary AUTHORIZATION_ENGINE_FEATURE = LicensedFeature.momentary(
        null,
        "security-authorization-engine",
        License.OperationMode.PLATINUM
    );

    // Custom role providers are Platinum+
    public static final LicensedFeature.Persistent CUSTOM_ROLE_PROVIDERS_FEATURE = LicensedFeature.persistent(
        null,
        "security-roles-provider",
        License.OperationMode.PLATINUM
    );

    public static final LicensedFeature.Momentary OPERATOR_PRIVILEGES_FEATURE = LicensedFeature.momentary(
        null,
        "operator-privileges",
        License.OperationMode.ENTERPRISE
    );

    public static final LicensedFeature.Momentary USER_PROFILE_COLLABORATION_FEATURE = LicensedFeature.momentary(
        null,
        "user-profile-collaboration",
        License.OperationMode.STANDARD
    );

    /**
     * Configurable cross cluster access is Enterprise feature.
     */
    public static final LicensedFeature.Momentary ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE = LicensedFeature.momentary(
        null,
        "advanced-remote-cluster-security",
        License.OperationMode.ENTERPRISE
    );

    private static final Logger logger = LogManager.getLogger(Security.class);

    private final Settings settings;
    private final boolean enabled;
    private final SecuritySystemIndices systemIndices;
    private final ListenableFuture<Void> nodeStartedListenable;

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

    private final SetOnce<SharedGroupFactory> sharedGroupFactory = new SetOnce<>();
    private final SetOnce<DocumentSubsetBitsetCache> dlsBitsetCache = new SetOnce<>();
    private final SetOnce<List<BootstrapCheck>> bootstrapChecks = new SetOnce<>();
    private final List<SecurityExtension> securityExtensions = new ArrayList<>();
    private final SetOnce<Transport> transportReference = new SetOnce<>();
    private final SetOnce<ScriptService> scriptServiceReference = new SetOnce<>();

    private final SetOnce<ReservedRoleMappingAction> reservedRoleMappingAction = new SetOnce<>();

    public Security(Settings settings) {
        this(settings, Collections.emptyList());
    }

    Security(Settings settings, List<SecurityExtension> extensions) {
        // TODO This is wrong. Settings can change after this. We should use the settings from createComponents
        this.settings = settings;
        // TODO this is wrong, we should only use the environment that is provided to createComponents
        this.enabled = XPackSettings.SECURITY_ENABLED.get(settings);
        this.systemIndices = new SecuritySystemIndices();
        this.nodeStartedListenable = new ListenableFuture<>();
        if (enabled) {
            runStartupChecks(settings);
            Automatons.updateConfiguration(settings);
        } else {
            final List<String> remoteClusterCredentialsSettingKeys = RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS.getAllConcreteSettings(
                settings
            ).map(Setting::getKey).sorted().toList();
            if (false == remoteClusterCredentialsSettingKeys.isEmpty()) {
                throw new IllegalArgumentException(
                    format(
                        "Found [%s] remote clusters with credentials [%s]. Security [%s] must be enabled to connect to them. "
                            + "Please either enable security or remove these settings from the keystore.",
                        remoteClusterCredentialsSettingKeys.size(),
                        Strings.collectionToCommaDelimitedString(remoteClusterCredentialsSettingKeys),
                        XPackSettings.SECURITY_ENABLED.getKey()
                    )
                );
            }
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

    protected SSLService getSslService() {
        return XPackPlugin.getSharedSslService();
    }

    protected LicenseService getLicenseService() {
        return XPackPlugin.getSharedLicenseService();
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
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
        AllocationService allocationService
    ) {
        try {
            return createComponents(
                client,
                threadPool,
                clusterService,
                resourceWatcherService,
                scriptService,
                xContentRegistry,
                environment,
                nodeEnvironment.nodeMetadata(),
                expressionResolver
            );
        } catch (final Exception e) {
            throw new IllegalStateException("security initialization failed", e);
        }
    }

    // pkg private for testing - tests want to pass in their set of extensions hence we are not using the extension service directly
    Collection<Object> createComponents(
        Client client,
        ThreadPool threadPool,
        ClusterService clusterService,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeMetadata nodeMetadata,
        IndexNameExpressionResolver expressionResolver
    ) throws Exception {
        logger.info("Security is {}", enabled ? "enabled" : "disabled");
        if (enabled == false) {
            return Collections.singletonList(new SecurityUsageServices(null, null, null, null, null, null));
        }

        systemIndices.init(client, clusterService);

        scriptServiceReference.set(scriptService);
        // We need to construct the checks here while the secure settings are still available.
        // If we wait until #getBoostrapChecks the secure settings will have been cleared/closed.
        final List<BootstrapCheck> checks = new ArrayList<>();
        checks.addAll(
            Arrays.asList(
                new TokenSSLBootstrapCheck(),
                new PkiRealmBootstrapCheck(getSslService()),
                new SecurityImplicitBehaviorBootstrapCheck(nodeMetadata, getLicenseService()),
                new TransportTLSBootstrapCheck()
            )
        );
        checks.addAll(InternalRealms.getBootstrapChecks(settings, environment));
        this.bootstrapChecks.set(Collections.unmodifiableList(checks));

        threadContext.set(threadPool.getThreadContext());
        List<Object> components = new ArrayList<>();
        securityContext.set(new SecurityContext(settings, threadPool.getThreadContext()));
        components.add(securityContext.get());

        final RestrictedIndices restrictedIndices = new RestrictedIndices(expressionResolver);

        // audit trail service construction
        final AuditTrail auditTrail = XPackSettings.AUDIT_ENABLED.get(settings)
            ? new LoggingAuditTrail(settings, clusterService, threadPool)
            : null;
        final AuditTrailService auditTrailService = new AuditTrailService(auditTrail, getLicenseState());
        components.add(auditTrailService);
        this.auditTrailService.set(auditTrailService);

        final TokenService tokenService = new TokenService(
            settings,
            Clock.systemUTC(),
            client,
            getLicenseState(),
            securityContext.get(),
            systemIndices.getMainIndexManager(),
            systemIndices.getTokenIndexManager(),
            clusterService
        );
        this.tokenService.set(tokenService);
        components.add(tokenService);

        // realms construction
        final NativeUsersStore nativeUsersStore = new NativeUsersStore(settings, client, systemIndices.getMainIndexManager());
        final NativeRoleMappingStore nativeRoleMappingStore = new NativeRoleMappingStore(
            settings,
            client,
            systemIndices.getMainIndexManager(),
            scriptService
        );
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        components.add(anonymousUser);
        final ReservedRealm reservedRealm = new ReservedRealm(environment, settings, nativeUsersStore, anonymousUser, threadPool);
        final SecurityExtension.SecurityComponents extensionComponents = new ExtensionComponents(
            environment,
            client,
            clusterService,
            resourceWatcherService,
            nativeRoleMappingStore
        );
        Map<String, Realm.Factory> realmFactories = new HashMap<>(
            InternalRealms.getFactories(
                threadPool,
                settings,
                resourceWatcherService,
                getSslService(),
                nativeUsersStore,
                nativeRoleMappingStore,
                systemIndices.getMainIndexManager()
            )
        );
        for (SecurityExtension extension : securityExtensions) {
            Map<String, Realm.Factory> newRealms = extension.getRealms(extensionComponents);
            for (Map.Entry<String, Realm.Factory> entry : newRealms.entrySet()) {
                if (realmFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Realm type [" + entry.getKey() + "] is already registered");
                }
            }
        }
        final Realms realms = new Realms(
            settings,
            environment,
            realmFactories,
            getLicenseState(),
            threadPool.getThreadContext(),
            reservedRealm
        );
        components.add(nativeUsersStore);
        components.add(nativeRoleMappingStore);
        components.add(realms);
        components.add(reservedRealm);

        systemIndices.getMainIndexManager().addStateListener(nativeRoleMappingStore::onSecurityIndexStateChange);

        final CacheInvalidatorRegistry cacheInvalidatorRegistry = new CacheInvalidatorRegistry();
        cacheInvalidatorRegistry.registerAlias("service", Set.of("file_service_account_token", "index_service_account_token"));
        components.add(cacheInvalidatorRegistry);
        systemIndices.getMainIndexManager().addStateListener(cacheInvalidatorRegistry::onSecurityIndexStateChange);

        final NativePrivilegeStore privilegeStore = new NativePrivilegeStore(
            settings,
            client,
            systemIndices.getMainIndexManager(),
            cacheInvalidatorRegistry
        );
        components.add(privilegeStore);

        dlsBitsetCache.set(new DocumentSubsetBitsetCache(settings, threadPool));
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(settings);
        final FileRolesStore fileRolesStore = new FileRolesStore(
            settings,
            environment,
            resourceWatcherService,
            getLicenseState(),
            xContentRegistry
        );
        final NativeRolesStore nativeRolesStore = new NativeRolesStore(
            settings,
            client,
            getLicenseState(),
            systemIndices.getMainIndexManager(),
            clusterService
        );
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();
        RoleDescriptor.setFieldPermissionsCache(fieldPermissionsCache);

        final Map<String, List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>> customRoleProviders = new LinkedHashMap<>();
        for (SecurityExtension extension : securityExtensions) {
            final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> providers = extension.getRolesProviders(
                extensionComponents
            );
            if (providers != null && providers.isEmpty() == false) {
                customRoleProviders.put(extension.extensionName(), providers);
            }
        }

        final ApiKeyService apiKeyService = new ApiKeyService(
            settings,
            Clock.systemUTC(),
            client,
            systemIndices.getMainIndexManager(),
            clusterService,
            cacheInvalidatorRegistry,
            threadPool
        );
        components.add(apiKeyService);

        final IndexServiceAccountTokenStore indexServiceAccountTokenStore = new IndexServiceAccountTokenStore(
            settings,
            threadPool,
            getClock(),
            client,
            systemIndices.getMainIndexManager(),
            clusterService,
            cacheInvalidatorRegistry
        );
        components.add(indexServiceAccountTokenStore);

        final FileServiceAccountTokenStore fileServiceAccountTokenStore = new FileServiceAccountTokenStore(
            environment,
            resourceWatcherService,
            threadPool,
            clusterService,
            cacheInvalidatorRegistry
        );
        components.add(fileServiceAccountTokenStore);

        final ServiceAccountService serviceAccountService = new ServiceAccountService(
            client,
            fileServiceAccountTokenStore,
            indexServiceAccountTokenStore
        );
        components.add(serviceAccountService);

        final RoleProviders roleProviders = new RoleProviders(
            reservedRolesStore,
            fileRolesStore,
            nativeRolesStore,
            customRoleProviders,
            getLicenseState()
        );
        final CompositeRolesStore allRolesStore = new CompositeRolesStore(
            settings,
            roleProviders,
            privilegeStore,
            threadPool.getThreadContext(),
            getLicenseState(),
            fieldPermissionsCache,
            apiKeyService,
            serviceAccountService,
            dlsBitsetCache.get(),
            restrictedIndices,
            new DeprecationRoleDescriptorConsumer(clusterService, threadPool)
        );
        systemIndices.getMainIndexManager().addStateListener(allRolesStore::onSecurityIndexStateChange);

        final ProfileService profileService = new ProfileService(
            settings,
            getClock(),
            client,
            systemIndices.getProfileIndexManager(),
            clusterService,
            realms::getDomainConfig,
            threadPool
        );
        components.add(profileService);

        // We use the value of the {@code ENROLLMENT_ENABLED} setting to determine if the node is starting up with auto-generated
        // certificates (which have been generated by pre-startup scripts). In this case, and further if the node forms a new cluster by
        // itself, rather than joining an existing one, we complete the auto-configuration by generating and printing credentials and
        // enrollment tokens (when the .security index becomes available).
        // The generated information is output on node's standard out (if
        InitialNodeSecurityAutoConfiguration.maybeGenerateEnrollmentTokensAndElasticCredentialsOnNodeStartup(
            nativeUsersStore,
            systemIndices.getMainIndexManager(),
            getSslService(),
            client,
            environment,
            (runnable -> nodeStartedListenable.addListener(ActionListener.running(runnable))),
            threadPool
        );

        // to keep things simple, just invalidate all cached entries on license change. this happens so rarely that the impact should be
        // minimal
        getLicenseState().addListener(allRolesStore::invalidateAll);

        final AuthenticationFailureHandler failureHandler = createAuthenticationFailureHandler(realms, extensionComponents);
        final OperatorPrivilegesService operatorPrivilegesService;
        final boolean operatorPrivilegesEnabled = OPERATOR_PRIVILEGES_ENABLED.get(settings);
        if (operatorPrivilegesEnabled) {
            logger.info("operator privileges are enabled");
            operatorPrivilegesService = new OperatorPrivileges.DefaultOperatorPrivilegesService(
                getLicenseState(),
                new FileOperatorUsersStore(environment, resourceWatcherService),
                new DefaultOperatorOnlyRegistry(clusterService.getClusterSettings())
            );
        } else {
            operatorPrivilegesService = OperatorPrivileges.NOOP_OPERATOR_PRIVILEGES_SERVICE;
        }
        authcService.set(
            new AuthenticationService(
                settings,
                realms,
                auditTrailService,
                failureHandler,
                threadPool,
                anonymousUser,
                tokenService,
                apiKeyService,
                serviceAccountService,
                operatorPrivilegesService
            )
        );
        components.add(authcService.get());
        systemIndices.getMainIndexManager().addStateListener(authcService.get()::onSecurityIndexStateChange);

        Set<RequestInterceptor> requestInterceptors = Sets.newHashSet(
            new ResizeRequestInterceptor(threadPool, getLicenseState(), auditTrailService),
            new IndicesAliasesRequestInterceptor(threadPool.getThreadContext(), getLicenseState(), auditTrailService)
        );
        if (XPackSettings.DLS_FLS_ENABLED.get(settings)) {
            requestInterceptors.addAll(
                Arrays.asList(
                    new SearchRequestInterceptor(threadPool, getLicenseState(), clusterService),
                    new ShardSearchRequestInterceptor(threadPool, getLicenseState(), clusterService),
                    new UpdateRequestInterceptor(threadPool, getLicenseState()),
                    new BulkShardRequestInterceptor(threadPool, getLicenseState()),
                    new DlsFlsLicenseRequestInterceptor(threadPool.getThreadContext(), getLicenseState()),
                    new SearchRequestCacheDisablingInterceptor(threadPool, getLicenseState())
                )
            );
        }
        requestInterceptors = Collections.unmodifiableSet(requestInterceptors);

        final AuthorizationService authzService = new AuthorizationService(
            settings,
            allRolesStore,
            fieldPermissionsCache,
            clusterService,
            auditTrailService,
            failureHandler,
            threadPool,
            anonymousUser,
            getAuthorizationEngine(),
            requestInterceptors,
            getLicenseState(),
            expressionResolver,
            operatorPrivilegesService,
            restrictedIndices
        );

        components.add(nativeRolesStore); // used by roles actions
        components.add(reservedRolesStore); // used by roles actions
        components.add(allRolesStore); // for SecurityInfoTransportAction and clear roles cache
        components.add(authzService);

        final SecondaryAuthenticator secondaryAuthenticator = new SecondaryAuthenticator(
            securityContext.get(),
            authcService.get(),
            auditTrailService
        );
        this.secondayAuthc.set(secondaryAuthenticator);
        components.add(secondaryAuthenticator);

        ipFilter.set(new IPFilter(settings, auditTrailService, clusterService.getClusterSettings(), getLicenseState()));
        components.add(ipFilter.get());

        final RemoteClusterCredentialsResolver remoteClusterCredentialsResolver = new RemoteClusterCredentialsResolver(settings);

        DestructiveOperations destructiveOperations = new DestructiveOperations(settings, clusterService.getClusterSettings());
        final CrossClusterAccessAuthenticationService crossClusterAccessAuthcService = new CrossClusterAccessAuthenticationService(
            clusterService,
            apiKeyService,
            authcService.get()
        );
        components.add(crossClusterAccessAuthcService);
        securityInterceptor.set(
            new SecurityServerTransportInterceptor(
                settings,
                threadPool,
                authcService.get(),
                authzService,
                getSslService(),
                securityContext.get(),
                destructiveOperations,
                crossClusterAccessAuthcService,
                remoteClusterCredentialsResolver,
                getLicenseState()
            )
        );

        securityActionFilter.set(
            new SecurityActionFilter(
                authcService.get(),
                authzService,
                auditTrailService,
                getLicenseState(),
                threadPool,
                securityContext.get(),
                destructiveOperations
            )
        );

        components.add(
            new SecurityUsageServices(realms, allRolesStore, nativeRoleMappingStore, ipFilter.get(), profileService, apiKeyService)
        );

        reservedRoleMappingAction.set(new ReservedRoleMappingAction(nativeRoleMappingStore));
        systemIndices.getMainIndexManager().onStateRecovered(state -> reservedRoleMappingAction.get().securityIndexRecovered());

        cacheInvalidatorRegistry.validate();

        return components;
    }

    private AuthorizationEngine getAuthorizationEngine() {
        return findValueFromExtensions("authorization engine", extension -> extension.getAuthorizationEngine(settings));
    }

    private AuthenticationFailureHandler createAuthenticationFailureHandler(
        final Realms realms,
        final SecurityExtension.SecurityComponents components
    ) {
        AuthenticationFailureHandler failureHandler = findValueFromExtensions(
            "authentication failure handler",
            extension -> extension.getAuthenticationFailureHandler(components)
        );
        if (failureHandler == null) {
            logger.debug("Using default authentication failure handler");
            Supplier<Map<String, List<String>>> headersSupplier = () -> {
                final Map<String, List<String>> defaultFailureResponseHeaders = new HashMap<>();
                realms.getActiveRealms().stream().forEach((realm) -> {
                    Map<String, List<String>> realmFailureHeaders = realm.getAuthenticationFailureHeaders();
                    realmFailureHeaders.entrySet().stream().forEach((e) -> {
                        String key = e.getKey();
                        e.getValue()
                            .stream()
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
                return defaultFailureResponseHeaders;
            };
            DefaultAuthenticationFailureHandler finalDefaultFailureHandler = new DefaultAuthenticationFailureHandler(headersSupplier.get());
            failureHandler = finalDefaultFailureHandler;
            getLicenseState().addListener(() -> { finalDefaultFailureHandler.setHeaders(headersSupplier.get()); });
        }
        return failureHandler;
    }

    /**
     * Calls the provided function for each configured extension and return the value that was generated by the extensions.
     * If multiple extensions provide a value, throws {@link IllegalStateException}.
     * If no extensions provide a value (or if there are no extensions) returns {@code null}.
     */
    @Nullable
    private <T> T findValueFromExtensions(String valueType, Function<SecurityExtension, T> method) {
        T foundValue = null;
        String fromExtension = null;
        for (SecurityExtension extension : securityExtensions) {
            final T extensionValue = method.apply(extension);
            if (extensionValue == null) {
                continue;
            }
            if (foundValue == null) {
                foundValue = extensionValue;
                fromExtension = extension.extensionName();
            } else {
                throw new IllegalStateException(
                    "Extensions ["
                        + fromExtension
                        + "] and ["
                        + extension.extensionName()
                        + "] "
                        + " both attempted to provide a value for ["
                        + valueType
                        + "]"
                );
            }
        }
        if (foundValue == null) {
            return null;
        } else {
            logger.debug("Using [{}] [{}] from extension [{}]", valueType, foundValue, fromExtension);
            return foundValue;
        }
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
                if (httpType.equals(SecurityField.NAME4)) {
                    SecurityHttpSettings.overrideSettings(builder, settings);
                } else {
                    final String message = String.format(
                        Locale.ROOT,
                        "http type setting [%s] must be [%s] but is [%s]",
                        NetworkModule.HTTP_TYPE_KEY,
                        SecurityField.NAME4,
                        httpType
                    );
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
        settingsList.add(ApiKeyService.DELETE_RETENTION_PERIOD);
        settingsList.add(ApiKeyService.CACHE_HASH_ALGO_SETTING);
        settingsList.add(ApiKeyService.CACHE_MAX_KEYS_SETTING);
        settingsList.add(ApiKeyService.CACHE_TTL_SETTING);
        settingsList.add(ApiKeyService.DOC_CACHE_TTL_SETTING);
        settingsList.add(NativePrivilegeStore.CACHE_MAX_APPLICATIONS_SETTING);
        settingsList.add(NativePrivilegeStore.CACHE_TTL_SETTING);
        settingsList.add(OPERATOR_PRIVILEGES_ENABLED);
        settingsList.add(CachingServiceAccountTokenStore.CACHE_TTL_SETTING);
        settingsList.add(CachingServiceAccountTokenStore.CACHE_HASH_ALGO_SETTING);
        settingsList.add(CachingServiceAccountTokenStore.CACHE_MAX_TOKENS_SETTING);
        settingsList.add(SimpleRole.CACHE_SIZE_SETTING);

        // hide settings
        settingsList.add(Setting.stringListSetting(SecurityField.setting("hide_settings"), Property.NodeScope, Property.Filtered));
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
        headers.add(new RestHeaderDefinition(JwtRealm.HEADER_CLIENT_AUTHENTICATION, false));
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
                module.setReaderWrapper(
                    indexService -> new SecurityIndexReaderWrapper(
                        shardId -> indexService.newSearchExecutionContext(
                            shardId.id(),
                            0,
                            // we pass a null index reader, which is legal and will disable rewrite optimizations
                            // based on index statistics, which is probably safer...
                            null,
                            () -> {
                                throw new IllegalArgumentException("permission filters are not allowed to use the current timestamp");

                            },
                            null,
                            // Don't use runtime mappings in the security query
                            emptyMap()
                        ),
                        dlsBitsetCache.get(),
                        securityContext.get(),
                        getLicenseState(),
                        indexService.getScriptService()
                    )
                );
                /*
                 * We need to forcefully overwrite the query cache implementation to use security's opt-out query cache implementation. This
                 * implementation disables the query cache if field level security is used for a particular request. We have to forcefully
                 * overwrite the query cache implementation to prevent data leakage to unauthorized users.
                 */
                module.forceQueryCacheProvider(
                    (indexSettings, cache) -> new OptOutQueryCache(indexSettings.getIndex(), cache, threadContext.get())
                );
            }

            // in order to prevent scroll ids from being maliciously crafted and/or guessed, a listener is added that
            // attaches information to the scroll context so that we can validate the user that created the scroll against
            // the user that is executing a scroll operation
            module.addSearchOperationListener(new SecuritySearchOperationListener(securityContext.get(), auditTrailService.get()));
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.SECURITY, SecurityUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.SECURITY, SecurityInfoTransportAction.class);
        if (enabled == false) {
            return Arrays.asList(usageAction, infoAction);
        }

        return Stream.of(
            new ActionHandler<>(ClearRealmCacheAction.INSTANCE, TransportClearRealmCacheAction.class),
            new ActionHandler<>(ClearRolesCacheAction.INSTANCE, TransportClearRolesCacheAction.class),
            new ActionHandler<>(ClearPrivilegesCacheAction.INSTANCE, TransportClearPrivilegesCacheAction.class),
            new ActionHandler<>(ClearSecurityCacheAction.INSTANCE, TransportClearSecurityCacheAction.class),
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
            new ActionHandler<>(SamlCompleteLogoutAction.INSTANCE, TransportSamlCompleteLogoutAction.class),
            new ActionHandler<>(SamlSpMetadataAction.INSTANCE, TransportSamlSpMetadataAction.class),
            new ActionHandler<>(OpenIdConnectPrepareAuthenticationAction.INSTANCE, TransportOpenIdConnectPrepareAuthenticationAction.class),
            new ActionHandler<>(OpenIdConnectAuthenticateAction.INSTANCE, TransportOpenIdConnectAuthenticateAction.class),
            new ActionHandler<>(OpenIdConnectLogoutAction.INSTANCE, TransportOpenIdConnectLogoutAction.class),
            new ActionHandler<>(GetBuiltinPrivilegesAction.INSTANCE, TransportGetBuiltinPrivilegesAction.class),
            new ActionHandler<>(GetPrivilegesAction.INSTANCE, TransportGetPrivilegesAction.class),
            new ActionHandler<>(PutPrivilegesAction.INSTANCE, TransportPutPrivilegesAction.class),
            new ActionHandler<>(DeletePrivilegesAction.INSTANCE, TransportDeletePrivilegesAction.class),
            new ActionHandler<>(CreateApiKeyAction.INSTANCE, TransportCreateApiKeyAction.class),
            TcpTransport.isUntrustedRemoteClusterEnabled()
                ? new ActionHandler<>(CreateCrossClusterApiKeyAction.INSTANCE, TransportCreateCrossClusterApiKeyAction.class)
                : null,
            new ActionHandler<>(GrantApiKeyAction.INSTANCE, TransportGrantApiKeyAction.class),
            new ActionHandler<>(InvalidateApiKeyAction.INSTANCE, TransportInvalidateApiKeyAction.class),
            new ActionHandler<>(GetApiKeyAction.INSTANCE, TransportGetApiKeyAction.class),
            new ActionHandler<>(QueryApiKeyAction.INSTANCE, TransportQueryApiKeyAction.class),
            new ActionHandler<>(UpdateApiKeyAction.INSTANCE, TransportUpdateApiKeyAction.class),
            new ActionHandler<>(BulkUpdateApiKeyAction.INSTANCE, TransportBulkUpdateApiKeyAction.class),
            TcpTransport.isUntrustedRemoteClusterEnabled()
                ? new ActionHandler<>(UpdateCrossClusterApiKeyAction.INSTANCE, TransportUpdateCrossClusterApiKeyAction.class)
                : null,
            new ActionHandler<>(DelegatePkiAuthenticationAction.INSTANCE, TransportDelegatePkiAuthenticationAction.class),
            new ActionHandler<>(CreateServiceAccountTokenAction.INSTANCE, TransportCreateServiceAccountTokenAction.class),
            new ActionHandler<>(DeleteServiceAccountTokenAction.INSTANCE, TransportDeleteServiceAccountTokenAction.class),
            new ActionHandler<>(GetServiceAccountCredentialsAction.INSTANCE, TransportGetServiceAccountCredentialsAction.class),
            new ActionHandler<>(GetServiceAccountNodesCredentialsAction.INSTANCE, TransportGetServiceAccountNodesCredentialsAction.class),
            new ActionHandler<>(GetServiceAccountAction.INSTANCE, TransportGetServiceAccountAction.class),
            new ActionHandler<>(KibanaEnrollmentAction.INSTANCE, TransportKibanaEnrollmentAction.class),
            new ActionHandler<>(NodeEnrollmentAction.INSTANCE, TransportNodeEnrollmentAction.class),
            new ActionHandler<>(ProfileHasPrivilegesAction.INSTANCE, TransportProfileHasPrivilegesAction.class),
            new ActionHandler<>(GetProfilesAction.INSTANCE, TransportGetProfilesAction.class),
            new ActionHandler<>(ActivateProfileAction.INSTANCE, TransportActivateProfileAction.class),
            new ActionHandler<>(UpdateProfileDataAction.INSTANCE, TransportUpdateProfileDataAction.class),
            new ActionHandler<>(SuggestProfilesAction.INSTANCE, TransportSuggestProfilesAction.class),
            new ActionHandler<>(SetProfileEnabledAction.INSTANCE, TransportSetProfileEnabledAction.class),
            usageAction,
            infoAction
        ).filter(Objects::nonNull).toList();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        if (enabled == false) {
            return emptyList();
        }
        return singletonList(securityActionFilter.get());
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
        if (enabled == false) {
            return emptyList();
        }
        return Stream.<RestHandler>of(
            new RestAuthenticateAction(settings, securityContext.get(), getLicenseState()),
            new RestClearRealmCacheAction(settings, getLicenseState()),
            new RestClearRolesCacheAction(settings, getLicenseState()),
            new RestClearPrivilegesCacheAction(settings, getLicenseState()),
            new RestClearApiKeyCacheAction(settings, getLicenseState()),
            new RestClearServiceAccountTokenStoreCacheAction(settings, getLicenseState()),
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
            new RestSamlCompleteLogoutAction(settings, getLicenseState()),
            new RestSamlSpMetadataAction(settings, getLicenseState()),
            new RestOpenIdConnectPrepareAuthenticationAction(settings, getLicenseState()),
            new RestOpenIdConnectAuthenticateAction(settings, getLicenseState()),
            new RestOpenIdConnectLogoutAction(settings, getLicenseState()),
            new RestGetBuiltinPrivilegesAction(settings, getLicenseState()),
            new RestGetPrivilegesAction(settings, getLicenseState()),
            new RestPutPrivilegesAction(settings, getLicenseState()),
            new RestDeletePrivilegesAction(settings, getLicenseState()),
            new RestCreateApiKeyAction(settings, getLicenseState()),
            TcpTransport.isUntrustedRemoteClusterEnabled() ? new RestCreateCrossClusterApiKeyAction(settings, getLicenseState()) : null,
            new RestUpdateApiKeyAction(settings, getLicenseState()),
            new RestBulkUpdateApiKeyAction(settings, getLicenseState()),
            TcpTransport.isUntrustedRemoteClusterEnabled() ? new RestUpdateCrossClusterApiKeyAction(settings, getLicenseState()) : null,
            new RestGrantApiKeyAction(settings, getLicenseState()),
            new RestInvalidateApiKeyAction(settings, getLicenseState()),
            new RestGetApiKeyAction(settings, getLicenseState()),
            new RestQueryApiKeyAction(settings, getLicenseState()),
            new RestDelegatePkiAuthenticationAction(settings, getLicenseState()),
            new RestCreateServiceAccountTokenAction(settings, getLicenseState()),
            new RestDeleteServiceAccountTokenAction(settings, getLicenseState()),
            new RestGetServiceAccountCredentialsAction(settings, getLicenseState()),
            new RestGetServiceAccountAction(settings, getLicenseState()),
            new RestKibanaEnrollAction(settings, getLicenseState()),
            new RestNodeEnrollmentAction(settings, getLicenseState()),
            new RestProfileHasPrivilegesAction(settings, securityContext.get(), getLicenseState()),
            new RestGetProfilesAction(settings, getLicenseState()),
            new RestActivateProfileAction(settings, getLicenseState()),
            new RestUpdateProfileDataAction(settings, getLicenseState()),
            new RestSuggestProfilesAction(settings, getLicenseState()),
            new RestEnableProfileAction(settings, getLicenseState()),
            new RestDisableProfileAction(settings, getLicenseState())
        ).filter(Objects::nonNull).toList();
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(SetSecurityUserProcessor.TYPE, new SetSecurityUserProcessor.Factory(securityContext::get, settings));
    }

    @Override
    public void onNodeStarted() {
        this.nodeStartedListenable.onResponse(null);
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
        final Set<String> badRealmSettings = settings.keySet().stream().filter(k -> k.startsWith(RealmSettings.PREFIX)).filter(key -> {
            final String suffix = key.substring(RealmSettings.PREFIX.length());
            // suffix-part, only contains a single '.'
            return suffix.indexOf('.') == suffix.lastIndexOf('.');
        }).collect(Collectors.toSet());
        if (badRealmSettings.isEmpty() == false) {
            String sampleRealmSetting = RealmSettings.realmSettingPrefix(new RealmConfig.RealmIdentifier("file", "my_file")) + "order";
            throw new IllegalArgumentException(
                "Incorrect realm settings found. "
                    + "Realm settings have been changed to include the type as part of the setting key.\n"
                    + "For example '"
                    + sampleRealmSetting
                    + "'\n"
                    + "Found invalid config: "
                    + Strings.collectionToDelimitedString(badRealmSettings, ", ")
                    + "\n"
                    + "Please see the breaking changes documentation."
            );
        }
    }

    static void validateForFips(Settings settings) {
        final List<String> validationErrors = new ArrayList<>();
        Settings keystoreTypeSettings = settings.filter(k -> k.endsWith("keystore.type"))
            .filter(k -> settings.get(k).equalsIgnoreCase("jks"));
        if (keystoreTypeSettings.isEmpty() == false) {
            validationErrors.add(
                "JKS Keystores cannot be used in a FIPS 140 compliant JVM. Please "
                    + "revisit ["
                    + keystoreTypeSettings.toDelimitedString(',')
                    + "] settings"
            );
        }
        Settings keystorePathSettings = settings.filter(k -> k.endsWith("keystore.path"))
            .filter(k -> settings.hasValue(k.replace(".path", ".type")) == false)
            .filter(k -> KeyStoreUtil.inferKeyStoreType(settings.get(k)).equals("jks"));
        if (keystorePathSettings.isEmpty() == false) {
            validationErrors.add(
                "JKS Keystores cannot be used in a FIPS 140 compliant JVM. Please "
                    + "revisit ["
                    + keystorePathSettings.toDelimitedString(',')
                    + "] settings"
            );
        }
        final String selectedAlgorithm = XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings);
        if (selectedAlgorithm.toLowerCase(Locale.ROOT).startsWith("pbkdf2") == false) {
            validationErrors.add(
                "Only PBKDF2 is allowed for stored credential hashing in a FIPS 140 JVM. Please set the "
                    + "appropriate value for [ "
                    + XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey()
                    + " ] setting."
            );
        }
        Stream.of(ApiKeyService.PASSWORD_HASHING_ALGORITHM, XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM).forEach((setting) -> {
            final var storedHashAlgo = setting.get(settings);
            if (storedHashAlgo.toLowerCase(Locale.ROOT).startsWith("pbkdf2") == false) {
                // log instead of validation error for backwards compatibility
                logger.warn(
                    "Only PBKDF2 is allowed for stored credential hashing in a FIPS 140 JVM. "
                        + "Please set the appropriate value for [{}] setting.",
                    setting.getKey()
                );
            }
        });
        final var cacheHashAlgoSettings = settings.filter(k -> k.endsWith(".cache.hash_algo"));
        cacheHashAlgoSettings.keySet().forEach((key) -> {
            final var setting = cacheHashAlgoSettings.get(key);
            assert setting != null;
            final var hashAlgoName = setting.toLowerCase(Locale.ROOT);
            if (hashAlgoName.equals("ssha256") == false && hashAlgoName.startsWith("pbkdf2") == false) {
                logger.warn(
                    "[{}] is not recommended for in-memory credential hashing in a FIPS 140 JVM. "
                        + "The recommended hasher for [{}] is SSHA256.",
                    setting,
                    key
                );
            }
        });

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
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                String action,
                String executor,
                boolean forceExecution,
                TransportRequestHandler<T> actualHandler
            ) {
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
    public Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService
    ) {
        if (enabled == false) { // don't register anything if we are not enabled
            return Collections.emptyMap();
        }

        IPFilter ipFilter = this.ipFilter.get();
        return Map.of(
            // security based on Netty 4
            SecurityField.NAME4,
            () -> {
                transportReference.set(
                    new SecurityNetty4ServerTransport(
                        settings,
                        TransportVersion.CURRENT,
                        threadPool,
                        networkService,
                        pageCacheRecycler,
                        namedWriteableRegistry,
                        circuitBreakerService,
                        ipFilter,
                        getSslService(),
                        getNettySharedGroupFactory(settings)
                    )
                );
                return transportReference.get();
            }
        );
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
        if (enabled == false) { // don't register anything if we are not enabled
            return Collections.emptyMap();
        }

        final IPFilter ipFilter = this.ipFilter.get();
        final AcceptChannelHandler.AcceptPredicate acceptPredicate = new AcceptChannelHandler.AcceptPredicate() {
            @Override
            public void setBoundAddress(BoundTransportAddress boundHttpTransportAddress) {
                ipFilter.setBoundHttpTransportAddress(boundHttpTransportAddress);
            }

            @Override
            public boolean test(String profile, InetSocketAddress peerAddress) {
                return ipFilter.accept(profile, peerAddress);
            }
        };

        Map<String, Supplier<HttpServerTransport>> httpTransports = new HashMap<>();
        httpTransports.put(SecurityField.NAME4, () -> {
            final boolean ssl = HTTP_SSL_ENABLED.get(settings);
            final SSLService sslService = getSslService();
            final SslConfiguration sslConfiguration;
            final BiConsumer<Channel, ThreadContext> populateClientCertificate;
            if (ssl) {
                sslConfiguration = sslService.getHttpTransportSSLConfiguration();
                if (SSLService.isConfigurationValidForServerUsage(sslConfiguration) == false) {
                    throw new IllegalArgumentException(
                        "a key must be provided to run as a server. the key should be configured using the "
                            + "[xpack.security.http.ssl.key] or [xpack.security.http.ssl.keystore.path] setting"
                    );
                }
                if (SSLService.isSSLClientAuthEnabled(sslConfiguration)) {
                    populateClientCertificate = (channel, threadContext) -> extractClientCertificates(logger, threadContext, channel);
                } else {
                    populateClientCertificate = (channel, threadContext) -> {};
                }
            } else {
                sslConfiguration = null;
                populateClientCertificate = (channel, threadContext) -> {};
            }
            final AuthenticationService authenticationService = this.authcService.get();
            final ThreadContext threadContext = this.threadContext.get();
            return getHttpServerTransportWithHeadersValidator(
                settings,
                networkService,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                getNettySharedGroupFactory(settings),
                tracer,
                new TLSConfig(sslConfiguration, sslService::createSSLEngine),
                acceptPredicate,
                (httpRequest, channel, listener) -> {
                    HttpPreRequest httpPreRequest = HttpHeadersAuthenticatorUtils.asHttpPreRequest(httpRequest);
                    // step 1: Populate the thread context with credentials and any other HTTP request header values (eg run-as) that the
                    // authentication process looks for while doing its duty.
                    perRequestThreadContext.accept(httpPreRequest, threadContext);
                    populateClientCertificate.accept(channel, threadContext);
                    RemoteHostHeader.process(channel, threadContext);
                    // step 2: Run authentication on the now properly prepared thread-context.
                    // This inspects and modifies the thread context.
                    authenticationService.authenticate(
                        httpPreRequest,
                        ActionListener.wrap(ignored -> listener.onResponse(null), listener::onFailure)
                    );
                },
                (httpRequest, channel, listener) -> {
                    // allow unauthenticated OPTIONS request through
                    // this includes CORS preflight, and regular OPTIONS that return permitted methods for a given path
                    // But still populate the thread context with the usual request headers (as for any other request that is dispatched)
                    HttpPreRequest httpPreRequest = HttpHeadersAuthenticatorUtils.asHttpPreRequest(httpRequest);
                    perRequestThreadContext.accept(httpPreRequest, threadContext);
                    populateClientCertificate.accept(channel, threadContext);
                    RemoteHostHeader.process(channel, threadContext);
                    listener.onResponse(null);
                }
            );
        });
        return httpTransports;
    }

    // "public" so it can be used in tests
    public static Netty4HttpServerTransport getHttpServerTransportWithHeadersValidator(
        Settings settings,
        NetworkService networkService,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        HttpServerTransport.Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        SharedGroupFactory sharedGroupFactory,
        Tracer tracer,
        TLSConfig tlsConfig,
        @Nullable AcceptChannelHandler.AcceptPredicate acceptPredicate,
        HttpValidator httpValidator,
        HttpValidator httpOptionsValidator
    ) {
        return getHttpServerTransportWithHeadersValidator(
            settings,
            networkService,
            threadPool,
            xContentRegistry,
            dispatcher,
            clusterSettings,
            sharedGroupFactory,
            tracer,
            tlsConfig,
            acceptPredicate,
            (httpRequest, channel, listener) -> {
                if (httpRequest.method() == HttpMethod.OPTIONS) {
                    if (HttpUtil.getContentLength(httpRequest, -1L) > 1 || HttpUtil.isTransferEncodingChunked(httpRequest)) {
                        // OPTIONS requests with a body are not supported
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                "OPTIONS requests with a payload body are not supported",
                                RestStatus.BAD_REQUEST
                            )
                        );
                    } else {
                        httpOptionsValidator.validate(httpRequest, channel, listener);
                    }
                } else {
                    httpValidator.validate(httpRequest, channel, listener);
                }
            }
        );
    }

    // "public" so it can be used in tests
    public static Netty4HttpServerTransport getHttpServerTransportWithHeadersValidator(
        Settings settings,
        NetworkService networkService,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        HttpServerTransport.Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        SharedGroupFactory sharedGroupFactory,
        Tracer tracer,
        TLSConfig tlsConfig,
        @Nullable AcceptChannelHandler.AcceptPredicate acceptPredicate,
        HttpValidator httpValidator
    ) {
        return new Netty4HttpServerTransport(
            settings,
            networkService,
            threadPool,
            xContentRegistry,
            dispatcher,
            clusterSettings,
            sharedGroupFactory,
            tracer,
            tlsConfig,
            acceptPredicate,
            Objects.requireNonNull(httpValidator)
        ) {
            @Override
            protected void populatePerRequestThreadContext(RestRequest restRequest, ThreadContext threadContext) {
                ThreadContext.StoredContext authenticationThreadContext = HttpHeadersAuthenticatorUtils.extractAuthenticationContext(
                    restRequest.getHttpRequest()
                );
                if (authenticationThreadContext != null) {
                    authenticationThreadContext.restore();
                } else {
                    // this is an unexpected internal error condition where {@code Netty4HttpHeaderValidator} does not work correctly
                    throw new ElasticsearchSecurityException("Request is not authenticated");
                }
            }
        };
    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerInterceptor(ThreadContext threadContext) {
        return handler -> new SecurityRestFilter(enabled, threadContext, secondayAuthc.get(), auditTrailService.get(), handler);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        if (enabled) {
            final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
            return List.of(
                new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool", false),
                new FixedExecutorBuilder(
                    settings,
                    SECURITY_CRYPTO_THREAD_POOL_NAME,
                    (allocatedProcessors + 1) / 2,
                    1000,
                    "xpack.security.crypto.thread_pool",
                    false
                )
            );
        }
        return Collections.emptyList();
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return templates -> {
            // .security index is not managed by using templates anymore
            templates.remove("security_audit_log");
            // .security is a system index now. deleting another legacy template that's not used anymore
            templates.remove("security-index-template");
            return templates;
        };
    }

    @Override
    public Function<String, Predicate<String>> getFieldFilter() {
        if (enabled) {
            return index -> {
                XPackLicenseState licenseState = getLicenseState();
                IndicesAccessControl indicesAccessControl = threadContext.get()
                    .getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                if (indicesAccessControl == null) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                assert indicesAccessControl.isGranted();
                IndicesAccessControl.IndexAccessControl indexPermissions = indicesAccessControl.getIndexPermissions(index);
                if (indexPermissions == null) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                FieldPermissions fieldPermissions = indexPermissions.getFieldPermissions();
                if (fieldPermissions.hasFieldLevelSecurity() == false) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                if (FIELD_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState) == false) {
                    // check license last, once we know FLS is actually used
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
            return new ValidateLicenseForFIPS(XPackSettings.FIPS_MODE_ENABLED.get(settings), getLicenseService());
        }
        return null;
    }

    static final class ValidateLicenseForFIPS implements BiConsumer<DiscoveryNode, ClusterState> {
        private final boolean inFipsMode;
        private final LicenseService licenseService;

        ValidateLicenseForFIPS(boolean inFipsMode, LicenseService licenseService) {
            this.inFipsMode = inFipsMode;
            this.licenseService = licenseService;
        }

        @Override
        public void accept(DiscoveryNode node, ClusterState state) {
            if (inFipsMode) {
                License license;
                if (licenseService instanceof ClusterStateLicenseService clusterStateLicenseService) {
                    license = clusterStateLicenseService.getLicense(state.metadata());
                } else {
                    license = licenseService.getLicense();
                }
                if (license != null && XPackLicenseState.isFipsAllowedForOperationMode(license.operationMode()) == false) {
                    throw new IllegalStateException(
                        "FIPS mode cannot be used with a ["
                            + license.operationMode()
                            + "] license. It is only allowed with a Platinum or Trial license."
                    );

                }
            }
        }
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        securityExtensions.addAll(loader.loadExtensions(SecurityExtension.class));
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
        return systemIndices.getSystemIndexDescriptors();
    }

    @Override
    public String getFeatureName() {
        return "security";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages configuration for Security features, such as users and roles";
    }

    @Override
    public CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> getRequestCacheKeyDifferentiator() {
        if (enabled == false) {
            return null;
        }
        return new DlsFlsRequestCacheDifferentiator(getLicenseState(), securityContext, scriptServiceReference);
    }

    List<ReservedClusterStateHandler<?>> reservedClusterStateHandlers() {
        // If security is disabled we never call the plugin createComponents
        if (enabled == false) {
            return Collections.emptyList();
        }
        return List.of(reservedRoleMappingAction.get());
    }
}
