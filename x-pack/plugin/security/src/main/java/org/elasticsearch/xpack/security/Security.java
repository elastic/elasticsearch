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
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Providers;
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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
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
import org.elasticsearch.transport.SharedGroupFactory;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.nio.NioGroupFactory;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.SecuritySettings;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheAction;
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
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
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
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TLSLicenseBootstrapCheck;
import org.elasticsearch.xpack.core.ssl.action.GetCertificateInfoAction;
import org.elasticsearch.xpack.core.ssl.action.TransportGetCertificateInfoAction;
import org.elasticsearch.xpack.core.ssl.rest.RestGetCertificateInfoAction;
import org.elasticsearch.xpack.security.action.TransportClearSecurityCacheAction;
import org.elasticsearch.xpack.security.action.TransportCreateApiKeyAction;
import org.elasticsearch.xpack.security.action.TransportDelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.security.action.TransportGetApiKeyAction;
import org.elasticsearch.xpack.security.action.TransportGrantApiKeyAction;
import org.elasticsearch.xpack.security.action.TransportInvalidateApiKeyAction;
import org.elasticsearch.xpack.security.action.apikey.TransportQueryApiKeyAction;
import org.elasticsearch.xpack.security.action.filter.SecurityActionFilter;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectLogoutAction;
import org.elasticsearch.xpack.security.action.oidc.TransportOpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.action.privilege.TransportClearPrivilegesCacheAction;
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
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
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
import org.elasticsearch.xpack.security.authz.interceptor.SearchRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.ShardSearchRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.UpdateRequestInterceptor;
import org.elasticsearch.xpack.security.authz.interceptor.ValidateRequestInterceptor;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.DeprecationRoleDescriptorConsumer;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.authz.store.RoleProviders;
import org.elasticsearch.xpack.security.ingest.SetSecurityUserProcessor;
import org.elasticsearch.xpack.security.operator.FileOperatorUsersStore;
import org.elasticsearch.xpack.security.operator.OperatorOnlyRegistry;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.rest.SecurityRestFilter;
import org.elasticsearch.xpack.security.rest.action.RestAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.RestDelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestClearApiKeyCacheAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestCreateApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGetApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGrantApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestInvalidateApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.apikey.RestQueryApiKeyAction;
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
import org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestSetEnabledAction;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.XPackSettings.API_KEY_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_TOKENS_ALIAS;
import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.INTERNAL_MAIN_INDEX_FORMAT;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.INTERNAL_TOKENS_INDEX_FORMAT;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_VERSION_STRING;
import static org.elasticsearch.xpack.security.transport.SSLEngineUtils.extractClientCertificates;

public class Security extends Plugin
    implements
        SystemIndexPlugin,
        IngestPlugin,
        NetworkPlugin,
        ClusterPlugin,
        DiscoveryPlugin,
        MapperPlugin,
        ExtensiblePlugin,
        SearchPlugin {

    public static final String SECURITY_CRYPTO_THREAD_POOL_NAME = XPackField.SECURITY + "-crypto";
    public static final Version FLATTENED_FIELD_TYPE_INTRODUCED = Version.V_7_3_0;

    // TODO: ip filtering does not actually track license usage yet
    public static final LicensedFeature.Momentary IP_FILTERING_FEATURE = LicensedFeature.momentaryLenient(
        null,
        "security-ip-filtering",
        License.OperationMode.GOLD
    );
    public static final LicensedFeature.Momentary AUDITING_FEATURE = LicensedFeature.momentaryLenient(
        null,
        "security-auditing",
        License.OperationMode.GOLD
    );
    public static final LicensedFeature.Momentary TOKEN_SERVICE_FEATURE = LicensedFeature.momentaryLenient(
        null,
        "security-token-service",
        License.OperationMode.STANDARD
    );

    private static final String REALMS_FEATURE_FAMILY = "security-realms";
    // Builtin realms (file/native) realms are Basic licensed, so don't need to be checked or tracked
    // Some realms (LDAP, AD, PKI) are Gold+
    public static final LicensedFeature.Persistent LDAP_REALM_FEATURE = LicensedFeature.persistentLenient(
        REALMS_FEATURE_FAMILY,
        "ldap",
        License.OperationMode.GOLD
    );
    public static final LicensedFeature.Persistent AD_REALM_FEATURE = LicensedFeature.persistentLenient(
        REALMS_FEATURE_FAMILY,
        "active-directory",
        License.OperationMode.GOLD
    );
    public static final LicensedFeature.Persistent PKI_REALM_FEATURE = LicensedFeature.persistentLenient(
        REALMS_FEATURE_FAMILY,
        "pki",
        License.OperationMode.GOLD
    );
    // SSO realms are Platinum+
    public static final LicensedFeature.Persistent SAML_REALM_FEATURE = LicensedFeature.persistentLenient(
        REALMS_FEATURE_FAMILY,
        "saml",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Persistent OIDC_REALM_FEATURE = LicensedFeature.persistentLenient(
        REALMS_FEATURE_FAMILY,
        "oidc",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Persistent KERBEROS_REALM_FEATURE = LicensedFeature.persistentLenient(
        REALMS_FEATURE_FAMILY,
        "kerberos",
        License.OperationMode.PLATINUM
    );
    // Custom realms are Platinum+
    public static final LicensedFeature.Persistent CUSTOM_REALMS_FEATURE = LicensedFeature.persistentLenient(
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

    /**
     * 7.17.x only setting to help mitigate any potential issues for how DLS applies to terms aggs + min_doc_count=0.
     * New versions default to stricter DLS rules and setting this to false will allow to revert to the less strict DLS behavior.
     */
    public static final Setting<Boolean> DLS_FORCE_TERMS_AGGS_TO_EXCLUDE_DELETED_DOCS = Setting.boolSetting(
        "xpack.security.dls.force_terms_aggs_to_exclude_deleted_docs.enabled",
        true,
        Property.NodeScope,
        Property.Deprecated
    );

    /**
     * 7.17.x only setting to help mitigate any potential issues for how DLS applies to the validate API + rewrite=true.
     * New versions default to stricter DLS rules and setting this to false will allow to revert to the less strict DLS behavior.
     */
    public static final Setting<Boolean> DLS_ERROR_WHEN_VALIDATE_QUERY_WITH_REWRITE = Setting.boolSetting(
        "xpack.security.dls.error_when_validate_query_with_rewrite.enabled",
        true,
        Property.NodeScope,
        Property.Deprecated
    );

    private static final Logger logger = LogManager.getLogger(Security.class);

    public static final SystemIndexDescriptor SECURITY_MAIN_INDEX_DESCRIPTOR = getSecurityMainIndexDescriptor();
    public static final SystemIndexDescriptor SECURITY_TOKEN_INDEX_DESCRIPTOR = getSecurityTokenIndexDescriptor();

    private final Settings settings;
    private final boolean enabled;
    private final boolean transportClientMode;
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
    private final SetOnce<Transport> transportReference = new SetOnce<>();
    private final SetOnce<ScriptService> scriptServiceReference = new SetOnce<>();

    public Security(Settings settings, final Path configPath) {
        this(settings, configPath, Collections.emptyList());
    }

    Security(Settings settings, final Path configPath, List<SecurityExtension> extensions) {
        // TODO This is wrong. Settings can change after this. We should use the settings from createComponents
        this.settings = settings;
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        // TODO this is wrong, we should only use the environment that is provided to createComponents
        this.enabled = XPackSettings.SECURITY_ENABLED.get(settings);
        if (enabled && transportClientMode == false) {
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

    @Override
    public Collection<Module> createGuiceModules() {
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
                b.bind(SSLService.class).toProvider(this::getSslService);
            });

            return modules;
        }
        modules.add(b -> XPackPlugin.bindFeatureSet(b, SecurityFeatureSet.class));

        if (enabled == false) {
            modules.add(b -> {
                b.bind(Realms.class).toProvider(Providers.of(null)); // for SecurityFeatureSet
                b.bind(CompositeRolesStore.class).toProvider(Providers.of(null)); // for SecurityFeatureSet
                b.bind(NativeRoleMappingStore.class).toProvider(Providers.of(null)); // for SecurityFeatureSet
                b.bind(AuditTrailService.class).toInstance(new AuditTrailService(Collections.emptyList(), getLicenseState()));
            });
            return modules;
        }

        return modules;
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    protected SSLService getSslService() {
        return XPackPlugin.getSharedSslService();
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
        Supplier<RepositoriesService> repositoriesServiceSupplier
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
        IndexNameExpressionResolver expressionResolver
    ) throws Exception {
        if (enabled == false) {
            return Collections.emptyList();
        }

        scriptServiceReference.set(scriptService);

        // We need to construct the checks here while the secure settings are still available.
        // If we wait until #getBoostrapChecks the secure settings will have been cleared/closed.
        final List<BootstrapCheck> checks = new ArrayList<>();
        checks.addAll(asList(new TokenSSLBootstrapCheck(), new PkiRealmBootstrapCheck(getSslService()), new TLSLicenseBootstrapCheck()));
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

        securityIndex.set(SecurityIndexManager.buildSecurityIndexManager(client, clusterService, SECURITY_MAIN_INDEX_DESCRIPTOR));

        final TokenService tokenService = new TokenService(
            settings,
            Clock.systemUTC(),
            client,
            getLicenseState(),
            securityContext.get(),
            securityIndex.get(),
            SecurityIndexManager.buildSecurityIndexManager(client, clusterService, SECURITY_TOKEN_INDEX_DESCRIPTOR),
            clusterService
        );
        this.tokenService.set(tokenService);
        components.add(tokenService);

        // realms construction
        final NativeUsersStore nativeUsersStore = new NativeUsersStore(settings, client, securityIndex.get());
        final NativeRoleMappingStore nativeRoleMappingStore = new NativeRoleMappingStore(
            settings,
            client,
            securityIndex.get(),
            scriptService
        );
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        components.add(anonymousUser);
        final ReservedRealm reservedRealm = new ReservedRealm(
            environment,
            settings,
            nativeUsersStore,
            anonymousUser,
            securityIndex.get(),
            threadPool
        );
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
                resourceWatcherService,
                getSslService(),
                nativeUsersStore,
                nativeRoleMappingStore,
                securityIndex.get()
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

        securityIndex.get().addStateListener(nativeRoleMappingStore::onSecurityIndexStateChange);

        final CacheInvalidatorRegistry cacheInvalidatorRegistry = new CacheInvalidatorRegistry();
        cacheInvalidatorRegistry.registerAlias(
            "service",
            org.elasticsearch.core.Set.of("file_service_account_token", "index_service_account_token")
        );
        components.add(cacheInvalidatorRegistry);
        securityIndex.get().addStateListener(cacheInvalidatorRegistry::onSecurityIndexStateChange);

        final NativePrivilegeStore privilegeStore = new NativePrivilegeStore(
            settings,
            client,
            securityIndex.get(),
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
        final NativeRolesStore nativeRolesStore = new NativeRolesStore(settings, client, getLicenseState(), securityIndex.get());
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
            getLicenseState(),
            securityIndex.get(),
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
            securityIndex.get(),
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
            new DeprecationRoleDescriptorConsumer(clusterService, threadPool)
        );
        securityIndex.get().addStateListener(allRolesStore::onSecurityIndexStateChange);

        // to keep things simple, just invalidate all cached entries on license change. this happens so rarely that the impact should be
        // minimal
        getLicenseState().addListener(allRolesStore::invalidateAll);
        getLicenseState().addListener(new SecurityStatusChangeListener(getLicenseState()));

        final AuthenticationFailureHandler failureHandler = createAuthenticationFailureHandler(realms, extensionComponents);
        final OperatorPrivilegesService operatorPrivilegesService;
        final boolean operatorPrivilegesEnabled = OPERATOR_PRIVILEGES_ENABLED.get(settings);
        if (operatorPrivilegesEnabled) {
            logger.info("operator privileges are enabled");
            operatorPrivilegesService = new OperatorPrivileges.DefaultOperatorPrivilegesService(
                getLicenseState(),
                new FileOperatorUsersStore(environment, resourceWatcherService),
                new OperatorOnlyRegistry(clusterService.getClusterSettings())
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
        securityIndex.get().addStateListener(authcService.get()::onSecurityIndexStateChange);

        Set<RequestInterceptor> requestInterceptors = Sets.newHashSet(
            new ResizeRequestInterceptor(threadPool, getLicenseState(), auditTrailService),
            new IndicesAliasesRequestInterceptor(threadPool.getThreadContext(), getLicenseState(), auditTrailService)
        );
        if (XPackSettings.DLS_FLS_ENABLED.get(settings)) {
            requestInterceptors.addAll(
                asList(
                    new SearchRequestInterceptor(threadPool, getLicenseState(), clusterService),
                    new ShardSearchRequestInterceptor(threadPool, getLicenseState(), clusterService),
                    new UpdateRequestInterceptor(threadPool, getLicenseState()),
                    new BulkShardRequestInterceptor(threadPool, getLicenseState()),
                    new DlsFlsLicenseRequestInterceptor(threadPool.getThreadContext(), getLicenseState()),
                    new ValidateRequestInterceptor(threadPool, getLicenseState(), settings)
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
            operatorPrivilegesService
        );

        components.add(nativeRolesStore); // used by roles actions
        components.add(reservedRolesStore); // used by roles actions
        components.add(allRolesStore); // for SecurityFeatureSet and clear roles cache
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
        DestructiveOperations destructiveOperations = new DestructiveOperations(settings, clusterService.getClusterSettings());
        securityInterceptor.set(
            new SecurityServerTransportInterceptor(
                settings,
                threadPool,
                authcService.get(),
                authzService,
                getLicenseState(),
                getSslService(),
                securityContext.get(),
                destructiveOperations,
                clusterService
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
        return additionalSettings(settings, enabled, transportClientMode);
    }

    // visible for tests
    static Settings additionalSettings(final Settings settings, final boolean enabled, final boolean transportClientMode) {
        if (enabled && transportClientMode == false) {
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
        return getSettings(transportClientMode, securityExtensions);
    }

    /**
     * Get the {@link Setting setting configuration} for all security components, including those defined in extensions.
     */
    public static List<Setting<?>> getSettings(boolean transportClientMode, List<SecurityExtension> securityExtensions) {
        List<Setting<?>> settingsList = new ArrayList<>();

        if (transportClientMode) {
            return settingsList;
        }

        // IP Filter settings
        IPFilter.addSettings(settingsList);

        // audit settings
        LoggingAuditTrail.registerSettings(settingsList);

        // authentication and authorization settings
        AnonymousUser.addSettings(settingsList);
        settingsList.addAll(InternalRealmsSettings.getSettings());
        NativeRolesStore.addSettings(settingsList);
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
        settingsList.add(SecurityServerTransportInterceptor.TRANSPORT_TYPE_PROFILE_SETTING);
        settingsList.addAll(SSLConfigurationSettings.getProfileSettings());
        settingsList.add(ApiKeyService.PASSWORD_HASHING_ALGORITHM);
        settingsList.add(ApiKeyService.DELETE_TIMEOUT);
        settingsList.add(ApiKeyService.DELETE_INTERVAL);
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
        settingsList.add(DLS_FORCE_TERMS_AGGS_TO_EXCLUDE_DELETED_DOCS);
        settingsList.add(DLS_ERROR_WHEN_VALIDATE_QUERY_WITH_REWRITE);

        // hide settings
        settingsList.add(
            Setting.listSetting(
                SecurityField.setting("hide_settings"),
                Collections.emptyList(),
                Function.identity(),
                Property.NodeScope,
                Property.Filtered
            )
        );
        return settingsList;
    }

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        if (transportClientMode) {
            return Collections.emptyList();
        }
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
                module.forceQueryCacheProvider((settings, cache) -> {
                    final OptOutQueryCache queryCache = new OptOutQueryCache(settings, cache, threadContext.get(), getLicenseState());
                    queryCache.listenForLicenseStateChanges();
                    return queryCache;
                });
            }

            // in order to prevent scroll ids from being maliciously crafted and/or guessed, a listener is added that
            // attaches information to the scroll context so that we can validate the user that created the scroll against
            // the user that is executing a scroll operation
            module.addSearchOperationListener(
                new SecuritySearchOperationListener(securityContext.get(), getLicenseState(), auditTrailService.get())
            );
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return emptyList();
        }
        return asList(
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
            new ActionHandler<>(GrantApiKeyAction.INSTANCE, TransportGrantApiKeyAction.class),
            new ActionHandler<>(InvalidateApiKeyAction.INSTANCE, TransportInvalidateApiKeyAction.class),
            new ActionHandler<>(GetApiKeyAction.INSTANCE, TransportGetApiKeyAction.class),
            new ActionHandler<>(QueryApiKeyAction.INSTANCE, TransportQueryApiKeyAction.class),
            new ActionHandler<>(DelegatePkiAuthenticationAction.INSTANCE, TransportDelegatePkiAuthenticationAction.class),
            new ActionHandler<>(CreateServiceAccountTokenAction.INSTANCE, TransportCreateServiceAccountTokenAction.class),
            new ActionHandler<>(DeleteServiceAccountTokenAction.INSTANCE, TransportDeleteServiceAccountTokenAction.class),
            new ActionHandler<>(GetServiceAccountCredentialsAction.INSTANCE, TransportGetServiceAccountCredentialsAction.class),
            new ActionHandler<>(GetServiceAccountNodesCredentialsAction.INSTANCE, TransportGetServiceAccountNodesCredentialsAction.class),
            new ActionHandler<>(GetServiceAccountAction.INSTANCE, TransportGetServiceAccountAction.class)
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
        return asList(
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
            new RestGrantApiKeyAction(settings, getLicenseState()),
            new RestInvalidateApiKeyAction(settings, getLicenseState()),
            new RestGetApiKeyAction(settings, getLicenseState()),
            new RestQueryApiKeyAction(settings, getLicenseState()),
            new RestDelegatePkiAuthenticationAction(settings, getLicenseState()),
            new RestCreateServiceAccountTokenAction(settings, getLicenseState()),
            new RestDeleteServiceAccountTokenAction(settings, getLicenseState()),
            new RestGetServiceAccountCredentialsAction(settings, getLicenseState()),
            new RestGetServiceAccountAction(settings, getLicenseState())
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(
            SetSecurityUserProcessor.TYPE,
            new SetSecurityUserProcessor.Factory(securityContext::get, this::getLicenseState)
        );
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
            .filter(k -> settings.hasValue(k.replace(".path", ".type")) == false);
        if (keystorePathSettings.isEmpty() == false && SSLConfigurationSettings.inferKeyStoreType(null).equals("jks")) {
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
                "Only PBKDF2 is allowed for password hashing in a FIPS 140 JVM. Please set the "
                    + "appropriate value for [ "
                    + XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey()
                    + " ] setting."
            );
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
        if (transportClientMode || enabled == false) { // don't register anything if we are not enabled
            // interceptors are not installed if we are running on the transport client
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
        if (transportClientMode || enabled == false) { // don't register anything if we are not enabled, or in transport client mode
            return Collections.emptyMap();
        }

        IPFilter ipFilter = this.ipFilter.get();

        Map<String, Supplier<Transport>> transports = new HashMap<>();
        transports.put(SecurityField.NAME4, () -> {
            transportReference.set(
                new SecurityNetty4ServerTransport(
                    settings,
                    Version.CURRENT,
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
        });
        transports.put(SecurityField.NIO, () -> {
            transportReference.set(
                new SecurityNioTransport(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    networkService,
                    pageCacheRecycler,
                    namedWriteableRegistry,
                    circuitBreakerService,
                    ipFilter,
                    getSslService(),
                    getNioGroupFactory(settings)
                )
            );
            return transportReference.get();
        });

        return Collections.unmodifiableMap(transports);
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
        ClusterSettings clusterSettings
    ) {
        if (enabled == false) { // don't register anything if we are not enabled
            return Collections.emptyMap();
        }

        Map<String, Supplier<HttpServerTransport>> httpTransports = new HashMap<>();
        httpTransports.put(SecurityField.NAME4, () -> {
            final BiConsumer<Channel, ThreadContext> populateClientCertificate;
            if (HTTP_SSL_ENABLED.get(settings)
                && getSslService().isSSLClientAuthEnabled(getSslService().getHttpTransportSSLConfiguration())) {
                populateClientCertificate = (channel, threadContext) -> extractClientCertificates(logger, threadContext, channel);
            } else {
                populateClientCertificate = (channel, threadContext) -> {};
            }
            final AuthenticationService authenticationService = this.authcService.get();
            final ThreadContext threadContext = this.threadContext.get();
            return getHttpServerTransportWithHeadersValidator(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                ipFilter.get(),
                getSslService(),
                getNettySharedGroupFactory(settings),
                clusterSettings,
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
        httpTransports.put(SecurityField.NIO, () -> {
            final BiConsumer<HttpChannel, ThreadContext> populateClientCertificate;
            if (HTTP_SSL_ENABLED.get(settings)
                && getSslService().isSSLClientAuthEnabled(getSslService().getHttpTransportSSLConfiguration())) {
                populateClientCertificate = (channel, threadContext) -> extractClientCertificates(logger, threadContext, channel);
            } else {
                populateClientCertificate = (channel, threadContext) -> {};
            }
            return new SecurityNioHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                pageCacheRecycler,
                threadPool,
                xContentRegistry,
                dispatcher,
                ipFilter.get(),
                getSslService(),
                getNioGroupFactory(settings),
                clusterSettings
            ) {
                @Override
                protected void populatePerRequestThreadContext(RestRequest restRequest, ThreadContext threadContext) {
                    perRequestThreadContext.accept(restRequest.getHttpRequest(), threadContext);
                    populateClientCertificate.accept(restRequest.getHttpChannel(), threadContext);
                    RemoteHostHeader.process(restRequest.getHttpChannel(), threadContext);
                }
            };
        });

        return httpTransports;
    }

    // "public" so it can be used in tests
    public static SecurityNetty4HttpServerTransport getHttpServerTransportWithHeadersValidator(
        Settings settings,
        NetworkService networkService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        HttpServerTransport.Dispatcher dispatcher,
        IPFilter ipFilter,
        SSLService sslService,
        SharedGroupFactory sharedGroupFactory,
        ClusterSettings clusterSettings,
        HttpValidator httpValidator,
        HttpValidator httpOptionsValidator
    ) {
        return getHttpServerTransportWithHeadersValidator(
            settings,
            networkService,
            bigArrays,
            threadPool,
            xContentRegistry,
            dispatcher,
            ipFilter,
            sslService,
            sharedGroupFactory,
            clusterSettings,
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
    public static SecurityNetty4HttpServerTransport getHttpServerTransportWithHeadersValidator(
        Settings settings,
        NetworkService networkService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        HttpServerTransport.Dispatcher dispatcher,
        IPFilter ipFilter,
        SSLService sslService,
        SharedGroupFactory sharedGroupFactory,
        ClusterSettings clusterSettings,
        HttpValidator httpValidator
    ) {
        return new SecurityNetty4HttpServerTransport(
            settings,
            networkService,
            bigArrays,
            ipFilter,
            sslService,
            threadPool,
            xContentRegistry,
            dispatcher,
            clusterSettings,
            sharedGroupFactory,
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
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        if (enabled == false || transportClientMode) {
            return null;
        }
        return handler -> new SecurityRestFilter(
            getLicenseState(),
            authcService.get(),
            secondayAuthc.get(),
            auditTrailService.get(),
            handler
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        if (enabled && transportClientMode == false) {
            final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
            return org.elasticsearch.core.List.of(
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
                if (licenseState.isSecurityEnabled() == false) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
                IndicesAccessControl indicesAccessControl = threadContext.get()
                    .getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
                if (indicesAccessControl == null) {
                    return MapperPlugin.NOOP_FIELD_PREDICATE;
                }
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
            return new ValidateUpgradedSecurityIndex().andThen(new ValidateLicenseForFIPS(XPackSettings.FIPS_MODE_ENABLED.get(settings)));
        }
        return null;
    }

    static final class ValidateUpgradedSecurityIndex implements BiConsumer<DiscoveryNode, ClusterState> {
        @Override
        public void accept(DiscoveryNode node, ClusterState state) {
            if (state.getNodes().getMinNodeVersion().before(Version.V_7_0_0)) {
                IndexMetadata indexMetadata = state.getMetadata().getIndices().get(SECURITY_MAIN_ALIAS);
                if (indexMetadata != null && INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()) < INTERNAL_MAIN_INDEX_FORMAT) {
                    throw new IllegalStateException(
                        "Security index is not on the current version ["
                            + INTERNAL_MAIN_INDEX_FORMAT
                            + "] - "
                            + "The Upgrade API must be run for 7.x nodes to join the cluster"
                    );
                }
            }
        }
    }

    static final class ValidateLicenseCanBeDeserialized implements BiConsumer<DiscoveryNode, ClusterState> {
        @Override
        public void accept(DiscoveryNode node, ClusterState state) {
            License license = LicenseService.getLicense(state.metadata());
            if (license != null && license.version() >= License.VERSION_CRYPTO_ALGORITHMS && node.getVersion().before(Version.V_6_4_0)) {
                throw new IllegalStateException(
                    "node "
                        + node
                        + " is on version ["
                        + node.getVersion()
                        + "] that cannot deserialize the license format ["
                        + license.version()
                        + "], upgrade node to at least 6.4.0"
                );
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

    private static SystemIndexDescriptor getSecurityMainIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setMinimumNodeVersion(FLATTENED_FIELD_TYPE_INTRODUCED)
            // This can't just be `.security-*` because that would overlap with the tokens index pattern
            .setIndexPattern(".security-[0-9]+*")
            .setPrimaryIndex(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7)
            .setDescription("Contains Security configuration")
            .setMappings(getIndexMappings())
            .setSettings(getIndexSettings())
            .setAliasName(SECURITY_MAIN_ALIAS)
            .setIndexFormat(INTERNAL_MAIN_INDEX_FORMAT)
            .setVersionMetaKey("security-version")
            .setOrigin(SECURITY_ORIGIN)
            .setPriorSystemIndexDescriptors(
                org.elasticsearch.core.List.of(
                    SystemIndexDescriptor.builder()
                        .setIndexPattern(".security-[0-9]+*")
                        .setPrimaryIndex(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7)
                        .setDescription("Contains Security configuration")
                        .setMappings(getIndexMappings(Version.V_7_12_0))
                        .setSettings(getIndexSettings())
                        .setAliasName(SECURITY_MAIN_ALIAS)
                        .setIndexFormat(INTERNAL_MAIN_INDEX_FORMAT)
                        .setVersionMetaKey("security-version")
                        .setOrigin(SECURITY_ORIGIN)
                        .build()
                )
            )
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static SystemIndexDescriptor getSecurityTokenIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".security-tokens-[0-9]+*")
            .setPrimaryIndex(RestrictedIndicesNames.INTERNAL_SECURITY_TOKENS_INDEX_7)
            .setDescription("Contains auth token data")
            .setMappings(getTokenIndexMappings())
            .setSettings(getTokenIndexSettings())
            .setAliasName(SECURITY_TOKENS_ALIAS)
            .setIndexFormat(INTERNAL_TOKENS_INDEX_FORMAT)
            .setVersionMetaKey(SECURITY_VERSION_STRING)
            .setOrigin(SECURITY_ORIGIN)
            .setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.unmodifiableList(asList(SECURITY_MAIN_INDEX_DESCRIPTOR, SECURITY_TOKEN_INDEX_DESCRIPTOR));
    }

    private static Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 1000)
            .put("index.refresh_interval", "1s")
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), INTERNAL_MAIN_INDEX_FORMAT)
            .put("analysis.filter.email.type", "pattern_capture")
            .put("analysis.filter.email.preserve_original", true)
            .putList("analysis.filter.email.patterns", asList("([^@]+)", "(\\p{L}+)", "(\\d+)", "@(.+)"))
            .put("analysis.analyzer.email.tokenizer", "uax_url_email")
            .putList("analysis.analyzer.email.filter", asList("email", "lowercase", "unique"))
            .build();
    }

    private static XContentBuilder getIndexMappings() {
        return getIndexMappings(Version.CURRENT);
    }

    private static XContentBuilder getIndexMappings(Version mappingVersion) {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field(SECURITY_VERSION_STRING, mappingVersion.toString());
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("username");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("roles");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("role_templates");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("template");
                            builder.field("type", "text");
                            builder.endObject();

                            builder.startObject("format");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("password");
                    builder.field("type", "keyword");
                    builder.field("index", false);
                    builder.field("doc_values", false);
                    builder.endObject();

                    builder.startObject("full_name");
                    builder.field("type", "text");
                    builder.endObject();

                    builder.startObject("email");
                    builder.field("type", "text");
                    builder.field("analyzer", "email");
                    builder.endObject();

                    builder.startObject("metadata");
                    builder.field("type", "object");
                    builder.field("dynamic", false);
                    builder.endObject();

                    if (mappingVersion.onOrAfter(Version.V_7_13_0)) {
                        builder.startObject("metadata_flattened");
                        builder.field("type", "flattened");
                        builder.endObject();
                    }

                    builder.startObject("enabled");
                    builder.field("type", "boolean");
                    builder.endObject();

                    builder.startObject("cluster");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("indices");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("field_security");
                            {
                                builder.startObject("properties");
                                {
                                    builder.startObject("grant");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("except");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("names");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("privileges");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("query");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("allow_restricted_indices");
                            builder.field("type", "boolean");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("applications");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("application");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("privileges");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("resources");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("application");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("global");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("application");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("manage");
                                    {
                                        builder.field("type", "object");
                                        builder.startObject("properties");
                                        {
                                            builder.startObject("applications");
                                            builder.field("type", "keyword");
                                            builder.endObject();
                                        }
                                        builder.endObject();
                                    }
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("name");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("run_as");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("doc_type");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("type");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("actions");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("expiration_time");
                    builder.field("type", "date");
                    builder.field("format", "epoch_millis");
                    builder.endObject();

                    builder.startObject("creation_time");
                    builder.field("type", "date");
                    builder.field("format", "epoch_millis");
                    builder.endObject();

                    builder.startObject("api_key_hash");
                    builder.field("type", "keyword");
                    builder.field("index", false);
                    builder.field("doc_values", false);
                    builder.endObject();

                    builder.startObject("api_key_invalidated");
                    builder.field("type", "boolean");
                    builder.endObject();

                    builder.startObject("role_descriptors");
                    builder.field("type", "object");
                    builder.field("enabled", false);
                    builder.endObject();

                    builder.startObject("limited_by_role_descriptors");
                    builder.field("type", "object");
                    builder.field("enabled", false);
                    builder.endObject();

                    builder.startObject("version");
                    builder.field("type", "integer");
                    builder.endObject();

                    builder.startObject("creator");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("principal");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("full_name");
                            builder.field("type", "text");
                            builder.endObject();

                            builder.startObject("email");
                            builder.field("type", "text");
                            builder.field("analyzer", "email");
                            builder.endObject();

                            builder.startObject("metadata");
                            builder.field("type", "object");
                            builder.field("dynamic", false);
                            builder.endObject();

                            builder.startObject("realm");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("realm_type");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("rules");
                    builder.field("type", "object");
                    builder.field("dynamic", false);
                    builder.endObject();

                    builder.startObject("refresh_token");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("token");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("refreshed");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("refresh_time");
                            builder.field("type", "date");
                            builder.field("format", "epoch_millis");
                            builder.endObject();

                            builder.startObject("superseding");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("encrypted_tokens");
                                    builder.field("type", "binary");
                                    builder.endObject();

                                    builder.startObject("encryption_iv");
                                    builder.field("type", "binary");
                                    builder.endObject();

                                    builder.startObject("encryption_salt");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("invalidated");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("client");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("type");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("user");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("realm");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("access_token");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("user_token");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("id");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("expiration_time");
                                    builder.field("type", "date");
                                    builder.field("format", "epoch_millis");
                                    builder.endObject();

                                    builder.startObject("version");
                                    builder.field("type", "integer");
                                    builder.endObject();

                                    builder.startObject("metadata");
                                    builder.field("type", "object");
                                    builder.field("dynamic", false);
                                    builder.endObject();

                                    builder.startObject("authentication");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("invalidated");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("realm");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();

            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build " + RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7 + " index mappings", e);
            throw new UncheckedIOException(
                "Failed to build " + RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7 + " index mappings",
                e
            );
        }
    }

    private static Settings getTokenIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 1000)
            .put("index.refresh_interval", "1s")
            .put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), INTERNAL_TOKENS_INDEX_FORMAT)
            .build();
    }

    private static XContentBuilder getTokenIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field(SECURITY_VERSION_STRING, Version.CURRENT);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("doc_type");
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject("creation_time");
                    builder.field("type", "date");
                    builder.field("format", "epoch_millis");
                    builder.endObject();

                    builder.startObject("refresh_token");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("token");
                            builder.field("type", "keyword");
                            builder.endObject();

                            builder.startObject("refreshed");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("refresh_time");
                            builder.field("type", "date");
                            builder.field("format", "epoch_millis");
                            builder.endObject();

                            builder.startObject("superseding");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("encrypted_tokens");
                                    builder.field("type", "binary");
                                    builder.endObject();

                                    builder.startObject("encryption_iv");
                                    builder.field("type", "binary");
                                    builder.endObject();

                                    builder.startObject("encryption_salt");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("invalidated");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("client");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("type");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("user");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("realm");
                                    builder.field("type", "keyword");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                    builder.startObject("access_token");
                    {
                        builder.field("type", "object");
                        builder.startObject("properties");
                        {
                            builder.startObject("user_token");
                            {
                                builder.field("type", "object");
                                builder.startObject("properties");
                                {
                                    builder.startObject("id");
                                    builder.field("type", "keyword");
                                    builder.endObject();

                                    builder.startObject("expiration_time");
                                    builder.field("type", "date");
                                    builder.field("format", "epoch_millis");
                                    builder.endObject();

                                    builder.startObject("version");
                                    builder.field("type", "integer");
                                    builder.endObject();

                                    builder.startObject("metadata");
                                    builder.field("type", "object");
                                    builder.field("dynamic", false);
                                    builder.endObject();

                                    builder.startObject("authentication");
                                    builder.field("type", "binary");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();

                            builder.startObject("invalidated");
                            builder.field("type", "boolean");
                            builder.endObject();

                            builder.startObject("realm");
                            builder.field("type", "keyword");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Failed to build " + RestrictedIndicesNames.INTERNAL_SECURITY_TOKENS_INDEX_7 + " index mappings",
                e
            );
        }
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
}
