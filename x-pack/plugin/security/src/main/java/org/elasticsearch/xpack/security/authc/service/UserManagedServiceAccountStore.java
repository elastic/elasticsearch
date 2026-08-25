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
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.SecurityFeatures;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.InvalidationCountingCacheWrapper;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * Stores the service accounts created through the API as {@code service_account} documents in the security index,
 * alongside the {@code service_account_token} documents that {@link IndexServiceAccountTokenStore} manages. Every
 * field written here is already part of the index's strict mapping.
 * <p>
 * Not supported in multi-project clusters, which replace the service account token store through
 * {@code SecurityExtension#getServiceAccountTokenStore} and so leave an account created here unable to hold a
 * credential.
 */
public class UserManagedServiceAccountStore implements CacheInvalidatorRegistry.CacheInvalidator {

    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        "xpack.security.authc.user_managed_service_account.cache.ttl",
        TimeValue.timeValueMinutes(20),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> CACHE_MAX_ACCOUNTS_SETTING = Setting.intSetting(
        "xpack.security.authc.user_managed_service_account.cache.max_accounts",
        10_000,
        Setting.Property.NodeScope
    );

    public static final String CACHE_NAME = "user_managed_service_account";

    static final String SERVICE_ACCOUNT_DOC_TYPE = "service_account";

    private static final Logger logger = LogManager.getLogger(UserManagedServiceAccountStore.class);

    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final TimeValue scrollKeepAlive;
    @Nullable
    private final InvalidationCountingCacheWrapper<String, CachedAccount> accountCache;

    @SuppressWarnings("this-escape")
    public UserManagedServiceAccountStore(
        Settings settings,
        Client client,
        SecurityIndexManager securityIndex,
        ClusterService clusterService,
        FeatureService featureService,
        CacheInvalidatorRegistry cacheInvalidatorRegistry
    ) {
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.scrollKeepAlive = DEFAULT_KEEPALIVE_SETTING.get(settings);
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        if (ttl.getNanos() > 0) {
            this.accountCache = new InvalidationCountingCacheWrapper<>(
                CacheBuilder.<String, CachedAccount>builder()
                    .setExpireAfterWrite(ttl)
                    .setMaximumWeight(CACHE_MAX_ACCOUNTS_SETTING.get(settings))
                    .build()
            );
        } else {
            this.accountCache = null;
        }
        // Always register: the TTL is node-scope, so another node may still cache. A write
        // clears the principal on every node and fails if that clear fails, and an
        // unregistered name is an error rather than a no-op. invalidate() does nothing
        // when this node has no cache.
        cacheInvalidatorRegistry.registerCacheInvalidator(CACHE_NAME, this);
    }

    /**
     * Looks up a single account, from the cache when it holds an entry for the principal.
     * <p>
     * Responds with {@code null} rather than failing when the principal could not name a user-managed account at all
     * — one in the reserved {@link org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings#BUILTIN_NAMESPACE}
     * namespace, or a malformed ID — because no such account can exist, which is what "not found" means to a caller.
     * Reporting validation errors is left to the write paths.
     */
    void getByPrincipal(String principal, ActionListener<UserManagedServiceAccount> listener) {
        if (Validation.UserManagedServiceAccounts.validatePrincipal(principal) != null) {
            listener.onResponse(null);
            return;
        }
        if (accountCache != null) {
            final CachedAccount cached = accountCache.get(principal);
            if (cached != null) {
                listener.onResponse(cached.account());
                return;
            }
        }
        // Sampled before the read starts so that an invalidation racing it discards the result instead of caching it.
        final long invalidationCount = accountCache != null ? accountCache.getInvalidationCount() : 0;
        loadAccountFromIndex(principal, invalidationCount, listener);
    }

    private void loadAccountFromIndex(String principal, long invalidationCount, ActionListener<UserManagedServiceAccount> listener) {
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
                final UserManagedServiceAccount account = response.isExists()
                    ? parseAccountDocument(principal, response.getSource())
                    : null;
                cacheAccount(principal, account, invalidationCount);
                listener.onResponse(account);
            }, listener::onFailure));
        });
    }

    /**
     * Lists the stored accounts, narrowed to a namespace and a service name when they are given. Reads the index
     * rather than the cache, so the result always reflects the last completed write. As in {@link #getByPrincipal},
     * an ID that no user-managed account could carry matches nothing rather than failing.
     */
    void listAccounts(@Nullable String namespace, @Nullable String serviceName, ActionListener<List<UserManagedServiceAccount>> listener) {
        if (namespace != null && Validation.UserManagedServiceAccounts.validateNamespace(namespace) != null) {
            listener.onResponse(List.of());
            return;
        }
        if (serviceName != null && Validation.UserManagedServiceAccounts.validateServiceName(serviceName) != null) {
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
                final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                    .setScroll(scrollKeepAlive)
                    .setQuery(accountsQuery(namespace, serviceName))
                    .setSize(1000)
                    .setFetchSource(true)
                    .request();
                ScrollHelper.fetchAllByEntity(
                    client,
                    request,
                    new ContextPreservingActionListener<>(
                        contextSupplier,
                        listener.map(accounts -> narrowToServiceName(accounts, namespace, serviceName))
                    ),
                    hit -> {
                        final Map<String, Object> source = hit.getSourceAsMap();
                        if (source == null) {
                            logger.warn("service account document [{}] has no source", hit.getId());
                            return null;
                        }
                        if (source.get("username") instanceof String principal) {
                            return parseAccountDocument(principal, source);
                        }
                        logger.warn("service account document [{}] has an invalid [username] field", hit.getId());
                        return null;
                    }
                );
            }
        });
    }

    private static BoolQueryBuilder accountsQuery(@Nullable String namespace, @Nullable String serviceName) {
        final BoolQueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE));
        if (namespace != null && serviceName != null) {
            query.filter(QueryBuilders.termQuery("username", namespace + "/" + serviceName));
        } else if (namespace != null) {
            // A stored principal is a namespace, a slash, and a non-empty service name, so this prefix selects
            // exactly the accounts in the namespace.
            query.filter(QueryBuilders.prefixQuery("username", namespace + "/"));
        }
        return query;
    }

    /**
     * A service name given without a namespace could only be matched by a leading wildcard over every stored
     * principal, so it is applied to the parsed accounts instead of to the query.
     */
    private static List<UserManagedServiceAccount> narrowToServiceName(
        Collection<UserManagedServiceAccount> accounts,
        @Nullable String namespace,
        @Nullable String serviceName
    ) {
        if (namespace != null || serviceName == null) {
            return List.copyOf(accounts);
        }
        return accounts.stream().filter(account -> serviceName.equals(account.id().serviceName())).toList();
    }

    /**
     * Creates the account, or replaces it wholesale if it already exists.
     */
    void putAccount(
        ServiceAccountId accountId,
        List<String> roles,
        boolean enabled,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<PutResult> listener
    ) {
        if (featureService.clusterHasFeature(clusterService.state(), SecurityFeatures.USER_MANAGED_SERVICE_ACCOUNTS) == false) {
            listener.onFailure(
                new IllegalStateException(
                    "cannot create a user-managed service account because not all nodes in the cluster support them yet"
                )
            );
            return;
        }
        final ValidationException validationException = validatePutRequest(accountId, roles);
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        try (XContentBuilder builder = newAccountDocument(accountId, sortedDistinct(roles), enabled)) {
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
                            final PutResult result = switch (response.getResult()) {
                                case CREATED -> PutResult.CREATED;
                                case UPDATED -> PutResult.UPDATED;
                                default -> throw new IllegalStateException(
                                    "unexpected result [" + response.getResult() + "] while writing service account [" + accountId + "]"
                                );
                            };
                            invalidateAccountCacheClusterWide(accountId.asPrincipal(), listener.map(ignore -> result));
                        }, listener::onFailure))
                    )
                );
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    /**
     * Deletes the account, responding with whether a document was actually removed. Tokens issued for the account
     * are not touched; refusing to strand them is the caller's concern.
     */
    void deleteAccount(ServiceAccountId accountId, WriteRequest.RefreshPolicy refreshPolicy, ActionListener<Boolean> listener) {
        final Validation.Error principalError = Validation.UserManagedServiceAccounts.validatePrincipal(accountId.asPrincipal());
        if (principalError != null) {
            listener.onFailure(new IllegalArgumentException(principalError.toString()));
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
                    if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                        invalidateAccountCacheClusterWide(accountId.asPrincipal(), listener.map(ignore -> true));
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

    private void cacheAccount(String principal, @Nullable UserManagedServiceAccount account, long invalidationCount) {
        if (accountCache != null) {
            accountCache.putIfNoInvalidationSince(principal, new CachedAccount(account), invalidationCount);
        }
    }

    @Nullable
    private static ValidationException validatePutRequest(ServiceAccountId accountId, @Nullable List<String> roles) {
        final ValidationException validationException = new ValidationException();
        addIfError(validationException, Validation.UserManagedServiceAccounts.validateNamespace(accountId.namespace()));
        addIfError(validationException, Validation.UserManagedServiceAccounts.validateServiceName(accountId.serviceName()));
        if (roles == null) {
            validationException.addValidationError("roles is required");
        } else {
            roles.forEach(role -> addIfError(validationException, NativeRealmValidationUtil.validateRoleName(role, true)));
        }
        return validationException.validationErrors().isEmpty() ? null : validationException;
    }

    private static void addIfError(ValidationException validationException, @Nullable Validation.Error error) {
        if (error != null) {
            validationException.addValidationError(error.toString());
        }
    }

    /**
     * Roles are stored sorted and de-duplicated, so that an account's document does not depend on the order the
     * caller happened to list them in.
     */
    private static List<String> sortedDistinct(List<String> roles) {
        return roles.stream().distinct().sorted().toList();
    }

    private XContentBuilder newAccountDocument(ServiceAccountId accountId, List<String> roles, boolean enabled) throws IOException {
        final Version version = clusterService.state().nodes().getMinNodeVersion();
        return XContentFactory.jsonBuilder()
            .startObject()
            .field("doc_type", SERVICE_ACCOUNT_DOC_TYPE)
            .field("version", version.id)
            .field("username", accountId.asPrincipal())
            .field("roles", roles)
            .field("enabled", enabled)
            .endObject();
    }

    /**
     * Turns a stored document into an account, or logs why it could not and responds with {@code null}. A document
     * that does not parse is treated as an absent account rather than as a failure, so that one damaged document
     * cannot fail authentication for the accounts around it.
     */
    @Nullable
    private static UserManagedServiceAccount parseAccountDocument(String expectedPrincipal, Map<String, Object> source) {
        if (SERVICE_ACCOUNT_DOC_TYPE.equals(source.get("doc_type")) == false) {
            logger.warn("service account document [{}] has an unexpected [doc_type] of [{}]", expectedPrincipal, source.get("doc_type"));
            return null;
        }
        if (expectedPrincipal.equals(source.get("username")) == false) {
            logger.warn("service account document [{}] holds a different [username] of [{}]", expectedPrincipal, source.get("username"));
            return null;
        }
        // Re-validated on read so that a document written by hand cannot shadow a built-in account by claiming a
        // principal in the reserved namespace.
        if (Validation.UserManagedServiceAccounts.validatePrincipal(expectedPrincipal) != null) {
            logger.warn("service account document [{}] does not name a user-managed service account", expectedPrincipal);
            return null;
        }
        final List<String> roles = parseRoles(expectedPrincipal, source.get("roles"));
        if (roles == null) {
            return null;
        }
        if (source.get("enabled") instanceof Boolean enabled) {
            return new UserManagedServiceAccount(ServiceAccountId.fromPrincipal(expectedPrincipal), roles, enabled);
        }
        logger.warn("service account document [{}] has an invalid [enabled] field", expectedPrincipal);
        return null;
    }

    /**
     * Accepts any list of strings. Role-name rules are enforced on write, not on read: a later tightening of those
     * rules must not make an already-stored account unreadable.
     */
    @Nullable
    private static List<String> parseRoles(String principal, @Nullable Object rolesValue) {
        if (rolesValue instanceof List<?> rolesList) {
            final List<String> roles = new ArrayList<>(rolesList.size());
            for (Object roleValue : rolesList) {
                if (roleValue instanceof String role) {
                    roles.add(role);
                } else {
                    logger.warn("service account document [{}] has an invalid role entry [{}]", principal, roleValue);
                    return null;
                }
            }
            return roles;
        }
        logger.warn("service account document [{}] has an invalid [roles] field", principal);
        return null;
    }

    /**
     * Drops the principal's entry from every node's cache, so that a write takes effect cluster-wide rather than
     * after this node's cache expires.
     */
    private void invalidateAccountCacheClusterWide(String principal, ActionListener<Void> listener) {
        final ClearSecurityCacheRequest clearSecurityCacheRequest = new ClearSecurityCacheRequest().cacheName(CACHE_NAME).keys(principal);
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            ClearSecurityCacheAction.INSTANCE,
            clearSecurityCacheRequest,
            ActionListener.wrap(response -> listener.onResponse(null), e -> {
                final String message = Strings.format(
                    "clearing the cache for service account [%s] failed. please clear the cache manually",
                    principal
                );
                logger.error(message, e);
                listener.onFailure(new ElasticsearchException(message, e));
            })
        );
    }

    public enum PutResult {
        CREATED,
        UPDATED
    }

    /**
     * Wraps the looked-up account so that "this principal has no account" can be cached too; the cache itself
     * cannot hold a null value.
     */
    record CachedAccount(@Nullable UserManagedServiceAccount account) {}
}
