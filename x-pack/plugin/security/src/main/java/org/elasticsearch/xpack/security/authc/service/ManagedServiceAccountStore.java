/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.support.ManagedServiceAccountIdValidator;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.InvalidationCountingCacheWrapper;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * Index-backed store for API-managed service account definitions.
 * <p>
 * Not supported in multi-project clusters: service account credential caching
 * ({@link CachingServiceAccountTokenStore}) is keyed by qualified token name with no project
 * dimension, so identically named accounts in different projects would share cache entries.
 * Multi-project deployments (serverless) instead replace the token store wholesale via
 * {@code SecurityExtension#getServiceAccountTokenStore}, which also disables managed accounts.
 * {@link org.elasticsearch.xpack.security.Security} therefore does not construct this store when
 * the project resolver supports multiple projects, and the caches here assume a single project.
 */
public class ManagedServiceAccountStore implements CacheInvalidatorRegistry.CacheInvalidator {

    public static final TransportVersion MANAGED_SERVICE_ACCOUNTS = ServiceAccountInfo.MANAGED_SERVICE_ACCOUNTS;

    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        "xpack.security.authc.managed_service_account.cache.ttl",
        TimeValue.timeValueMinutes(20),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> CACHE_MAX_ACCOUNTS_SETTING = Setting.intSetting(
        "xpack.security.authc.managed_service_account.cache.max_accounts",
        10_000,
        Setting.Property.NodeScope
    );

    static final String SERVICE_ACCOUNT_DOC_TYPE = "service_account";
    public static final String CACHE_NAME = "managed_service_account";

    private static final Logger logger = LogManager.getLogger(ManagedServiceAccountStore.class);

    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final Settings settings;
    @Nullable
    private final InvalidationCountingCacheWrapper<String, CachedAccount> accountCache;

    @SuppressWarnings("this-escape")
    public ManagedServiceAccountStore(
        Settings settings,
        Client client,
        SecurityIndexManager securityIndex,
        ClusterService clusterService,
        CacheInvalidatorRegistry cacheInvalidatorRegistry
    ) {
        this.settings = settings;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        if (ttl.getNanos() > 0) {
            accountCache = new InvalidationCountingCacheWrapper<>(
                CacheBuilder.<String, CachedAccount>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(CACHE_MAX_ACCOUNTS_SETTING.get(settings))
                    .build()
            );
        } else {
            accountCache = null;
        }
        cacheInvalidatorRegistry.registerCacheInvalidator(CACHE_NAME, this);
    }

    public void getByPrincipal(String principal, ActionListener<ManagedServiceAccount> listener) {
        final String principalError = ManagedServiceAccountIdValidator.validatePrincipal(principal);
        if (principalError != null) {
            listener.onFailure(new IllegalArgumentException(principalError));
            return;
        }
        if (accountCache != null) {
            final CachedAccount cached = accountCache.get(principal);
            if (cached != null) {
                listener.onResponse(cached.account());
                return;
            }
        }
        final long invalidationCount = accountCache != null ? accountCache.getInvalidationCount() : 0;
        loadAccountFromIndex(principal, invalidationCount, listener);
    }

    private void loadAccountFromIndex(String principal, long invalidationCount, ActionListener<ManagedServiceAccount> listener) {
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            cacheAccount(principal, null, invalidationCount);
            listener.onResponse(null);
            return;
        }
        if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
            return;
        }
        projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
            final GetRequest getRequest = client.prepareGet(SECURITY_MAIN_ALIAS, docIdForPrincipal(principal))
                .setFetchSource(true)
                .request();
            executeAsyncWithOrigin(client, SECURITY_ORIGIN, TransportGetAction.TYPE, getRequest, ActionListener.wrap(response -> {
                final ManagedServiceAccount account = response.isExists() ? parseAccountDocument(principal, response.getSource()) : null;
                cacheAccount(principal, account, invalidationCount);
                listener.onResponse(account);
            }, listener::onFailure));
        });
    }

    private void cacheAccount(String principal, @Nullable ManagedServiceAccount account, long invalidationCount) {
        if (accountCache != null) {
            accountCache.putIfNoInvalidationSince(principal, CachedAccount.of(account), invalidationCount);
        }
    }

    public void listAccounts(
        @Nullable String namespace,
        @Nullable String serviceName,
        ActionListener<List<ManagedServiceAccount>> listener
    ) {
        try {
            validateListParameters(namespace, serviceName);
        } catch (IllegalArgumentException e) {
            listener.onResponse(List.of());
            return;
        }
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(List.of());
            return;
        }
        if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
            return;
        }
        projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
            final Supplier<ThreadContext.StoredContext> contextSupplier = client.threadPool()
                .getThreadContext()
                .newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
                var query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE));
                if (namespace != null && serviceName != null) {
                    query = query.filter(QueryBuilders.termQuery("username", namespace + "/" + serviceName));
                } else if (namespace != null) {
                    query = query.filter(QueryBuilders.wildcardQuery("username", namespace + "/*"));
                } else if (serviceName != null) {
                    query = query.filter(QueryBuilders.wildcardQuery("username", "*/" + serviceName));
                }
                final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                    .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                    .setQuery(query)
                    .setSize(1000)
                    .setFetchSource(true)
                    .request();
                request.indicesOptions().ignoreUnavailable();
                ScrollHelper.fetchAllByEntity(
                    client,
                    request,
                    new ContextPreservingActionListener<>(
                        contextSupplier,
                        ActionListener.wrap(accounts -> listener.onResponse(List.copyOf(accounts)), listener::onFailure)
                    ),
                    hit -> {
                        final Map<String, Object> source = hit.getSourceAsMap();
                        if (source == null) {
                            logger.warn("managed service account search hit has no source");
                            return null;
                        }
                        final Object username = source.get("username");
                        if (username instanceof String principal) {
                            return parseAccountDocument(principal, source);
                        }
                        logger.warn("managed service account search hit has invalid username field");
                        return null;
                    }
                );
            }
        });
    }

    public void putAccount(
        ServiceAccount.ServiceAccountId accountId,
        List<String> roles,
        boolean enabled,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<PutResult> listener
    ) {
        if (false == clusterService.state().getMinTransportVersion().supports(MANAGED_SERVICE_ACCOUNTS)) {
            listener.onFailure(
                new IllegalStateException(
                    "managed service accounts require all nodes to be upgraded to a version that supports ["
                        + MANAGED_SERVICE_ACCOUNTS
                        + "]"
                )
            );
            return;
        }
        final ValidationException validationException = validatePutRequest(accountId, roles);
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        try (XContentBuilder builder = newAccountDocument(accountId, roles, enabled)) {
            final IndexRequest indexRequest = client.prepareIndex(SECURITY_MAIN_ALIAS)
                .setId(docIdForPrincipal(accountId.asPrincipal()))
                .setSource(builder)
                .setOpType(DocWriteRequest.OpType.INDEX)
                .setRefreshPolicy(refreshPolicy)
                .request();
            final BulkRequest bulkRequest = toSingleItemBulkRequest(indexRequest);
            securityIndex.forCurrentProject()
                .prepareIndexIfNeededThenExecute(
                    listener::onFailure,
                    () -> executeAsyncWithOrigin(
                        client,
                        SECURITY_ORIGIN,
                        TransportBulkAction.TYPE,
                        bulkRequest,
                        TransportBulkAction.<IndexResponse>unwrappingSingleItemBulkResponse(ActionListener.wrap(response -> {
                            final PutResult.Type type = response.getResult() == DocWriteResponse.Result.CREATED
                                ? PutResult.Type.CREATED
                                : PutResult.Type.UPDATED;
                            final ManagedServiceAccount account = new ManagedServiceAccount(accountId, roles, enabled);
                            invalidateManagedAccountCache(
                                accountId.asPrincipal(),
                                ActionListener.wrap(ignore -> listener.onResponse(new PutResult(type, account)), listener::onFailure)
                            );
                        }, listener::onFailure))
                    )
                );
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    public void deleteAccount(
        ServiceAccount.ServiceAccountId accountId,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<Boolean> listener
    ) {
        if (false == clusterService.state().getMinTransportVersion().supports(MANAGED_SERVICE_ACCOUNTS)) {
            listener.onFailure(
                new IllegalStateException(
                    "managed service accounts require all nodes to be upgraded to a version that supports ["
                        + MANAGED_SERVICE_ACCOUNTS
                        + "]"
                )
            );
            return;
        }
        final String principalError = ManagedServiceAccountIdValidator.validatePrincipal(accountId.asPrincipal());
        if (principalError != null) {
            listener.onFailure(new IllegalArgumentException(principalError));
            return;
        }
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(false);
            return;
        }
        if (projectSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
            return;
        }
        projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
            final DeleteRequest deleteRequest = client.prepareDelete(SECURITY_MAIN_ALIAS, docIdForPrincipal(accountId.asPrincipal()))
                .setRefreshPolicy(refreshPolicy)
                .request();
            executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                TransportDeleteAction.TYPE,
                deleteRequest,
                ActionListener.wrap(deleteResponse -> {
                    final boolean deleted = deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                    if (deleted) {
                        clearManagedAccountCaches(accountId, ActionListener.wrap(ignore -> listener.onResponse(true), listener::onFailure));
                    } else {
                        listener.onResponse(false);
                    }
                }, listener::onFailure)
            );
        });
    }

    @Override
    public void invalidate(Collection<String> keys) {
        if (accountCache != null) {
            accountCache.invalidate(keys);
        }
    }

    @Override
    public void invalidateAll() {
        if (accountCache != null) {
            accountCache.invalidateAll();
        }
    }

    // package private for testing
    @Nullable
    InvalidationCountingCacheWrapper<String, CachedAccount> getAccountCache() {
        return accountCache;
    }

    static String docIdForPrincipal(String principal) {
        return SERVICE_ACCOUNT_DOC_TYPE + "-" + principal;
    }

    static List<String> deduplicateRoles(List<String> roles) {
        return new ArrayList<>(new TreeSet<>(roles));
    }

    private static void validateListParameters(@Nullable String namespace, @Nullable String serviceName) {
        if (namespace != null) {
            final String namespaceError = ManagedServiceAccountIdValidator.validateNamespace(namespace);
            if (namespaceError != null) {
                throw new IllegalArgumentException(namespaceError);
            }
        }
        if (serviceName != null) {
            final String serviceNameError = ManagedServiceAccountIdValidator.validateServiceName(serviceName);
            if (serviceNameError != null) {
                throw new IllegalArgumentException(serviceNameError);
            }
        }
    }

    @Nullable
    private ValidationException validatePutRequest(ServiceAccount.ServiceAccountId accountId, List<String> roles) {
        ValidationException validationException = null;
        final String namespaceError = ManagedServiceAccountIdValidator.validateNamespace(accountId.namespace());
        if (namespaceError != null) {
            validationException = new ValidationException().addValidationError(namespaceError);
        }
        final String serviceNameError = ManagedServiceAccountIdValidator.validateServiceName(accountId.serviceName());
        if (serviceNameError != null) {
            if (validationException == null) {
                validationException = new ValidationException();
            }
            validationException.addValidationError(serviceNameError);
        }
        if (roles == null) {
            if (validationException == null) {
                validationException = new ValidationException();
            }
            validationException.addValidationError("roles is required");
        } else {
            for (String role : roles) {
                final Validation.Error roleNameError = NativeRealmValidationUtil.validateRoleName(role, true);
                if (roleNameError != null) {
                    if (validationException == null) {
                        validationException = new ValidationException();
                    }
                    validationException.addValidationError(roleNameError.toString());
                }
            }
        }
        return validationException;
    }

    private XContentBuilder newAccountDocument(ServiceAccount.ServiceAccountId accountId, List<String> roles, boolean enabled)
        throws IOException {
        final Version version = clusterService.state().nodes().getMinNodeVersion();
        final List<String> deduplicatedRoles = deduplicateRoles(roles);
        return XContentFactory.jsonBuilder()
            .startObject()
            .field("doc_type", SERVICE_ACCOUNT_DOC_TYPE)
            .field("version", version.id)
            .field("username", accountId.asPrincipal())
            .field("roles", deduplicatedRoles)
            .field("enabled", enabled)
            .endObject();
    }

    @Nullable
    private ManagedServiceAccount parseAccountDocument(String expectedPrincipal, Map<String, Object> source) {
        if (source == null) {
            logger.warn("managed service account document [{}] has no source", expectedPrincipal);
            return null;
        }
        final Object docTypeValue = source.get("doc_type");
        if (docTypeValue instanceof String docType) {
            if (SERVICE_ACCOUNT_DOC_TYPE.equals(docType) == false) {
                logger.warn("managed service account document [{}] has invalid doc_type", expectedPrincipal);
                return null;
            }
        } else {
            logger.warn("managed service account document [{}] has invalid doc_type", expectedPrincipal);
            return null;
        }
        final Object usernameValue = source.get("username");
        if (usernameValue instanceof String username) {
            if (username.equals(expectedPrincipal) == false) {
                logger.warn(
                    "managed service account document id principal [{}] does not match stored username [{}]",
                    expectedPrincipal,
                    username
                );
                return null;
            }
            if (ManagedServiceAccountIdValidator.validatePrincipal(username) != null) {
                logger.warn("managed service account document [{}] has invalid principal", expectedPrincipal);
                return null;
            }
            final Object rolesValue = source.get("roles");
            if (rolesValue instanceof List<?> rolesList) {
                final List<String> roles = new ArrayList<>(rolesList.size());
                for (Object roleValue : rolesList) {
                    if (roleValue instanceof String role) {
                        final Validation.Error roleNameError = NativeRealmValidationUtil.validateRoleName(role, true);
                        if (roleNameError != null) {
                            logger.warn("managed service account document [{}] has invalid role [{}]", expectedPrincipal, role);
                            return null;
                        }
                        roles.add(role);
                    } else {
                        logger.warn("managed service account document [{}] has non-string role entry", expectedPrincipal);
                        return null;
                    }
                }
                final Object enabledValue = source.get("enabled");
                if (enabledValue instanceof Boolean enabled) {
                    return new ManagedServiceAccount(ServiceAccount.ServiceAccountId.fromPrincipal(username), roles, enabled);
                }
                logger.warn("managed service account document [{}] has invalid enabled field", expectedPrincipal);
                return null;
            }
            logger.warn("managed service account document [{}] has invalid roles field", expectedPrincipal);
            return null;
        }
        logger.warn("managed service account document [{}] has invalid username field", expectedPrincipal);
        return null;
    }

    private void clearManagedAccountCaches(ServiceAccount.ServiceAccountId accountId, ActionListener<Void> listener) {
        invalidateManagedAccountCache(
            accountId.asPrincipal(),
            ActionListener.wrap(ignore -> clearManagedTokenCache(accountId, listener), listener::onFailure)
        );
    }

    private void invalidateManagedAccountCache(String principal, ActionListener<Void> listener) {
        final ClearSecurityCacheRequest clearSecurityCacheRequest = new ClearSecurityCacheRequest().cacheName(CACHE_NAME).keys(principal);
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            ClearSecurityCacheAction.INSTANCE,
            clearSecurityCacheRequest,
            ActionListener.wrap(response -> listener.onResponse(null), e -> {
                final String message = org.elasticsearch.core.Strings.format(
                    "clearing managed service account cache for [%s] failed; please clear the cache manually",
                    principal
                );
                logger.error(message, e);
                listener.onFailure(new ElasticsearchException(message, e));
            })
        );
    }

    private void clearManagedTokenCache(ServiceAccount.ServiceAccountId accountId, ActionListener<Void> listener) {
        final ClearSecurityCacheRequest clearSecurityCacheRequest = new ClearSecurityCacheRequest().cacheName("index_service_account_token")
            .keys(accountId.asPrincipal() + "/");
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            ClearSecurityCacheAction.INSTANCE,
            clearSecurityCacheRequest,
            ActionListener.wrap(response -> listener.onResponse(null), e -> {
                final String message = org.elasticsearch.core.Strings.format(
                    "clearing managed service account token cache for [%s] failed; please clear the cache manually",
                    accountId.asPrincipal()
                );
                logger.error(message, e);
                listener.onFailure(new ElasticsearchException(message, e));
            })
        );
    }

    public record PutResult(Type type, ManagedServiceAccount account) {
        public enum Type {
            CREATED,
            UPDATED
        }
    }

    record CachedAccount(@Nullable ManagedServiceAccount account) {
        static CachedAccount of(@Nullable ManagedServiceAccount account) {
            return new CachedAccount(account);
        }
    }
}
