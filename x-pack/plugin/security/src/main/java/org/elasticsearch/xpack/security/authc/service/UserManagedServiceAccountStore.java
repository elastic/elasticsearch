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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountAuthor;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.SecurityFeatures;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.InvalidationCountingCacheWrapper;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * Stores the service accounts created through the API as {@code service_account} documents in the security index,
 * alongside the {@code service_account_token} documents that {@link IndexServiceAccountTokenStore} manages. Every
 * field written here is part of the index's strict mapping, so a field the mapping does not yet hold is only
 * written once every node is on a version that declares it.
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

    public static final String SERVICE_ACCOUNT_DOC_TYPE = "service_account";

    private static final Logger logger = LogManager.getLogger(UserManagedServiceAccountStore.class);

    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final TimeValue scrollKeepAlive;
    @Nullable
    private final InvalidationCountingCacheWrapper<String, UserManagedServiceAccount> accountCache;
    private volatile boolean allowExpensiveQueries;

    @SuppressWarnings("this-escape")
    public UserManagedServiceAccountStore(
        Settings settings,
        Clock clock,
        Client client,
        SecurityIndexManager securityIndex,
        ClusterService clusterService,
        FeatureService featureService,
        CacheInvalidatorRegistry cacheInvalidatorRegistry
    ) {
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.scrollKeepAlive = DEFAULT_KEEPALIVE_SETTING.get(settings);
        this.allowExpensiveQueries = ALLOW_EXPENSIVE_QUERIES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ALLOW_EXPENSIVE_QUERIES, this::setAllowExpensiveQueries);
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        if (ttl.getNanos() > 0) {
            this.accountCache = new InvalidationCountingCacheWrapper<>(
                CacheBuilder.<String, UserManagedServiceAccount>builder()
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
     * Looks up a single account, from the cache when it holds a found account for the principal.
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
            final UserManagedServiceAccount cached = accountCache.get(principal);
            if (cached != null) {
                listener.onResponse(cached);
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
     * <p>
     * A namespace-only list uses a prefix query on the stored principal. Prefix queries are refused when
     * {@code search.allow_expensive_queries} is false, so that case fetches every service-account document and
     * keeps the ones in the namespace.
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
                final boolean allowExpensiveQueries = this.allowExpensiveQueries;
                final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                    .setScroll(scrollKeepAlive)
                    .setQuery(accountsQuery(namespace, serviceName, allowExpensiveQueries))
                    .setSize(1000)
                    .setFetchSource(true)
                    .request();
                ScrollHelper.fetchAllByEntity(
                    client,
                    request,
                    new ContextPreservingActionListener<>(
                        contextSupplier,
                        listener.map(accounts -> maybeFilterListedAccounts(accounts, namespace, serviceName, allowExpensiveQueries))
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

    private static BoolQueryBuilder accountsQuery(@Nullable String namespace, @Nullable String serviceName, boolean allowExpensiveQueries) {
        final BoolQueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE));
        if (namespace != null && serviceName != null) {
            query.filter(QueryBuilders.termQuery("username", namespace + "/" + serviceName));
        } else if (namespace != null && allowExpensiveQueries) {
            // A stored principal is a namespace, a slash, and a non-empty service name, so this prefix selects
            // exactly the accounts in the namespace. Prefix queries are refused when expensive queries are
            // disabled, and that case is filtered after parse instead.
            query.filter(QueryBuilders.prefixQuery("username", namespace + "/"));
        }
        return query;
    }

    /**
     * Applies list narrowing that the query cannot express cheaply. A service name given without a
     * namespace would need a leading wildcard. A namespace given without a service name uses a prefix
     * query, which is refused when expensive queries are disabled, so that case is filtered here too.
     */
    private static List<UserManagedServiceAccount> maybeFilterListedAccounts(
        Collection<UserManagedServiceAccount> accounts,
        @Nullable String namespace,
        @Nullable String serviceName,
        boolean allowExpensiveQueries
    ) {
        final boolean filterByNamespace = namespace != null && serviceName == null && allowExpensiveQueries == false;
        final boolean filterByServiceName = namespace == null && serviceName != null;
        if (filterByNamespace == false && filterByServiceName == false) {
            return List.copyOf(accounts);
        }
        if (filterByNamespace) {
            logger.trace("expensive queries are not allowed, filtering service accounts by namespace in memory");
        }
        return accounts.stream()
            .filter(account -> filterByNamespace == false || namespace.equals(account.id().namespace()))
            .filter(account -> filterByServiceName == false || serviceName.equals(account.id().serviceName()))
            .toList();
    }

    private void setAllowExpensiveQueries(boolean allowExpensiveQueries) {
        this.allowExpensiveQueries = allowExpensiveQueries;
    }

    /**
     * Runs a caller-shaped search over the stored accounts and reports one page of it. The caller has already
     * restricted the query to service-account documents and translated its field names; this only adds the checks
     * that the index can be searched at all and turns hits into accounts. A hit that does not parse is dropped, as in
     * {@link #listAccounts}, which can leave fewer items than {@link QueryResult#total()} claims.
     */
    void queryAccounts(SearchSourceBuilder searchSourceBuilder, ActionListener<QueryResult> listener) {
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            logger.debug("security index does not exist");
            listener.onResponse(QueryResult.EMPTY);
            return;
        }
        if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
            return;
        }
        final SearchRequest searchRequest = new SearchRequest(new String[] { SECURITY_MAIN_ALIAS }, searchSourceBuilder);
        projectSecurityIndex.checkIndexVersionThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                TransportSearchAction.TYPE,
                searchRequest,
                ActionListener.wrap(searchResponse -> {
                    final long total = searchResponse.getHits().getTotalHits().value();
                    if (total == 0) {
                        logger.debug("no service accounts found for query [{}]", searchSourceBuilder.query());
                        listener.onResponse(QueryResult.EMPTY);
                        return;
                    }
                    final List<QueryResult.Item> items = Arrays.stream(searchResponse.getHits().getHits())
                        .map(UserManagedServiceAccountStore::toQueryResultItem)
                        .filter(Objects::nonNull)
                        .toList();
                    listener.onResponse(new QueryResult(items, total));
                }, listener::onFailure)
            )
        );
    }

    @Nullable
    private static QueryResult.Item toQueryResultItem(SearchHit hit) {
        final Map<String, Object> source = hit.getSourceAsMap();
        if (source == null) {
            logger.warn("service account document [{}] has no source", hit.getId());
            return null;
        }
        if (source.get("username") instanceof String principal) {
            final UserManagedServiceAccount account = parseAccountDocument(principal, source);
            return account == null ? null : new QueryResult.Item(account, hit.getSortValues());
        }
        logger.warn("service account document [{}] has an invalid [username] field", hit.getId());
        return null;
    }

    /**
     * One page of a query. {@code total} counts every hit of the query, not just the page's, so a caller can tell how
     * far through the result it is.
     */
    public record QueryResult(List<Item> items, long total) {

        public static final QueryResult EMPTY = new QueryResult(List.of(), 0);

        /**
         * An account and the sort values of the hit it came from, which are what a caller passes back as
         * {@code search_after}. Empty when the query was not sorted.
         */
        public record Item(UserManagedServiceAccount account, Object[] sortValues) {}
    }

    /**
     * Creates the account, or replaces it wholesale if it already exists. A {@code null} description leaves the
     * account without one.
     * <p>
     * The write is a single update with an upsert, so that which of the two happened is decided by the index rather
     * than by a read that a concurrent write could make stale. The upsert is the whole document with the caller as
     * its creator; the update is every field a write may change, with the caller as its editor, and leaves the
     * creator alone. An update merges into the stored document field by field, so a field that a write may leave
     * empty is written as an explicit {@code null} rather than omitted, or the previous value would survive. The
     * attribution fields are not written to a cluster whose nodes do not all declare them, because until then the
     * index mapping may not hold them and the write would be rejected.
     */
    void putAccount(
        ServiceAccountId accountId,
        List<String> roles,
        boolean enabled,
        @Nullable String description,
        Authentication authentication,
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
        final ValidationException validationException = validatePutRequest(accountId, roles, description);
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        final boolean recordAttribution = featureService.clusterHasFeature(
            clusterService.state(),
            SecurityFeatures.USER_MANAGED_SERVICE_ACCOUNT_ATTRIBUTION
        );
        final ServiceAccountAuthor author = recordAttribution ? ServiceAccountAuthor.fromAuthentication(authentication) : null;
        final Instant now = clock.instant();
        final List<String> distinctRoles = sortedDistinct(roles);
        try (
            XContentBuilder upsert = newAccountDocument(accountId, distinctRoles, enabled, description, author, now);
            XContentBuilder doc = accountChanges(distinctRoles, enabled, description, author, now)
        ) {
            final UpdateRequest updateRequest = client.prepareUpdate(SECURITY_MAIN_ALIAS, docIdForPrincipal(accountId.asPrincipal()))
                .setDoc(doc)
                .setUpsert(upsert)
                .setRefreshPolicy(refreshPolicy)
                .request();
            securityIndex.forCurrentProject()
                .prepareIndexIfNeededThenExecute(
                    listener::onFailure,
                    () -> executeAsyncWithOrigin(
                        client,
                        SECURITY_ORIGIN,
                        TransportUpdateAction.TYPE,
                        updateRequest,
                        ActionListener.wrap(response -> {
                            final PutResult result = switch (response.getResult()) {
                                case CREATED -> PutResult.CREATED;
                                // A no-op needs the same caller to write the same account within the same
                                // millisecond, so it is not worth distinguishing from an update.
                                case UPDATED, NOOP -> PutResult.UPDATED;
                                default -> throw new IllegalStateException(
                                    "unexpected result [" + response.getResult() + "] while writing service account [" + accountId + "]"
                                );
                            };
                            invalidateAccountCacheClusterWide(accountId.asPrincipal(), listener.map(ignore -> result));
                        }, listener::onFailure)
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
                ActionListener.wrap(
                    deleteResponse -> invalidateAccountCacheClusterWide(
                        accountId.asPrincipal(),
                        listener.map(ignore -> deleteResponse.getResult() == DocWriteResponse.Result.DELETED)
                    ),
                    listener::onFailure
                )
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
    InvalidationCountingCacheWrapper<String, UserManagedServiceAccount> getAccountCache() {
        return accountCache;
    }

    static String docIdForPrincipal(String principal) {
        return SERVICE_ACCOUNT_DOC_TYPE + "-" + principal;
    }

    private void cacheAccount(String principal, @Nullable UserManagedServiceAccount account, long invalidationCount) {
        if (accountCache != null && account != null) {
            accountCache.putIfNoInvalidationSince(principal, account, invalidationCount);
        }
    }

    @Nullable
    private static ValidationException validatePutRequest(
        ServiceAccountId accountId,
        @Nullable List<String> roles,
        @Nullable String description
    ) {
        final ValidationException validationException = new ValidationException();
        addIfError(validationException, Validation.UserManagedServiceAccounts.validateNamespace(accountId.namespace()));
        addIfError(validationException, Validation.UserManagedServiceAccounts.validateServiceName(accountId.serviceName()));
        if (roles == null) {
            validationException.addValidationError("roles is required");
        } else {
            roles.forEach(role -> addIfError(validationException, NativeRealmValidationUtil.validateRoleName(role, true)));
            addIfError(validationException, Validation.UserManagedServiceAccounts.validateRoles(roles));
        }
        addIfError(validationException, Validation.UserManagedServiceAccounts.validateDescription(description));
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

    /**
     * The whole document, for when the account does not exist yet. The description is left out rather than written
     * as {@code null}, so that an account without one looks the same as one written before the field existed. The
     * caller is recorded as the creator; there is no editor until the account is replaced.
     */
    private static XContentBuilder newAccountDocument(
        ServiceAccountId accountId,
        List<String> roles,
        boolean enabled,
        @Nullable String description,
        @Nullable ServiceAccountAuthor creator,
        Instant now
    ) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("doc_type", SERVICE_ACCOUNT_DOC_TYPE)
            .field("version", UserManagedServiceAccount.Version.CURRENT.id())
            .field("username", accountId.asPrincipal())
            .field("roles", roles)
            .field("enabled", enabled);
        if (description != null) {
            builder.field("description", description);
        }
        if (creator != null) {
            addAuthor(builder, "creator", creator);
            builder.field("created_at", now.toEpochMilli());
        }
        return builder.endObject();
    }

    /**
     * The fields a write changes, for when the account already exists. Merged into the stored document, so the
     * description is written as an explicit {@code null} when there is none: left out, the old one would survive.
     * The caller is recorded as the editor; the creator is not touched.
     */
    private static XContentBuilder accountChanges(
        List<String> roles,
        boolean enabled,
        @Nullable String description,
        @Nullable ServiceAccountAuthor editor,
        Instant now
    ) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("version", UserManagedServiceAccount.Version.CURRENT.id())
            .field("roles", roles)
            .field("enabled", enabled)
            .field("description", description);
        if (editor != null) {
            addAuthor(builder, "editor", editor);
            builder.field("edited_at", now.toEpochMilli());
        }
        return builder.endObject();
    }

    /**
     * Writes every field of the author, the absent ones as explicit {@code null}s. An update merges objects field
     * by field, so leaving a field out would keep whatever the previous editor had there. The realm domain is written
     * whole, matching the {@code creator} mapping that API keys established. The user's metadata is deliberately not
     * recorded.
     */
    private static void addAuthor(XContentBuilder builder, String fieldName, ServiceAccountAuthor author) throws IOException {
        builder.startObject(fieldName)
            .field(ServiceAccountAuthor.PRINCIPAL_FIELD, author.principal())
            .field(ServiceAccountAuthor.FULL_NAME_FIELD, author.fullName())
            .field(ServiceAccountAuthor.EMAIL_FIELD, author.email())
            .field(ServiceAccountAuthor.REALM_FIELD, author.realm())
            .field(ServiceAccountAuthor.REALM_TYPE_FIELD, author.realmType())
            .field(ServiceAccountAuthor.REALM_DOMAIN_FIELD, author.realmDomain());
        if (author.apiKey() == null) {
            builder.nullField(ServiceAccountAuthor.API_KEY_FIELD);
        } else {
            // Unlike the response, the stored object must clear an absent name from the previous editor's key.
            builder.startObject(ServiceAccountAuthor.API_KEY_FIELD)
                .field(ServiceAccountAuthor.ApiKey.ID_FIELD, author.apiKey().id())
                .field(ServiceAccountAuthor.ApiKey.NAME_FIELD, author.apiKey().name())
                .endObject();
        }
        builder.endObject();
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
        // Absent in documents written before the field existed, and for accounts written without one since.
        final Object descriptionValue = source.get("description");
        if (descriptionValue != null && descriptionValue instanceof String == false) {
            logger.warn("service account document [{}] has an invalid [description] field", expectedPrincipal);
            return null;
        }
        if (source.get("enabled") instanceof Boolean enabled) {
            // Each attribution field is absent in documents written before they were recorded, and the editor and
            // its timestamp in every document until the account is first replaced.
            try {
                return new UserManagedServiceAccount(
                    ServiceAccountId.fromPrincipal(expectedPrincipal),
                    roles,
                    enabled,
                    (String) descriptionValue,
                    parseAuthor(source.get("creator"), "creator"),
                    parseTimestamp(source.get("created_at"), "created_at"),
                    parseAuthor(source.get("editor"), "editor"),
                    parseTimestamp(source.get("edited_at"), "edited_at")
                );
            } catch (IllegalArgumentException e) {
                logger.warn(() -> Strings.format("service account document [%s] %s", expectedPrincipal, e.getMessage()), e);
                return null;
            }
        }
        logger.warn("service account document [{}] has an invalid [enabled] field", expectedPrincipal);
        return null;
    }

    @Nullable
    private static Instant parseTimestamp(@Nullable Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number millis) {
            return Instant.ofEpochMilli(millis.longValue());
        }
        throw new IllegalArgumentException("has an invalid [" + fieldName + "] field");
    }

    /**
     * Reads a stored author. Fields the writer had no value for are stored as explicit {@code null}s, so those are
     * accepted where the author allows them and refused where it does not.
     */
    @Nullable
    private static ServiceAccountAuthor parseAuthor(@Nullable Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            try {
                return new ServiceAccountAuthor(
                    requiredString(map, ServiceAccountAuthor.PRINCIPAL_FIELD),
                    optionalString(map, ServiceAccountAuthor.FULL_NAME_FIELD),
                    optionalString(map, ServiceAccountAuthor.EMAIL_FIELD),
                    requiredString(map, ServiceAccountAuthor.REALM_FIELD),
                    requiredString(map, ServiceAccountAuthor.REALM_TYPE_FIELD),
                    parseRealmDomain(map.get(ServiceAccountAuthor.REALM_DOMAIN_FIELD)),
                    parseApiKey(map.get(ServiceAccountAuthor.API_KEY_FIELD))
                );
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("has an invalid [" + fieldName + "] field: " + e.getMessage(), e);
            }
        }
        throw new IllegalArgumentException("has an invalid [" + fieldName + "] field");
    }

    private static String requiredString(Map<?, ?> map, String key) {
        if (map.get(key) instanceof String string) {
            return string;
        }
        throw new IllegalArgumentException("[" + key + "] is missing or not a string");
    }

    @Nullable
    private static String optionalString(Map<?, ?> map, String key) {
        final Object value = map.get(key);
        if (value == null || value instanceof String) {
            return (String) value;
        }
        throw new IllegalArgumentException("[" + key + "] is not a string");
    }

    @Nullable
    private static ServiceAccountAuthor.ApiKey parseApiKey(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            return new ServiceAccountAuthor.ApiKey(
                requiredString(map, ServiceAccountAuthor.ApiKey.ID_FIELD),
                optionalString(map, ServiceAccountAuthor.ApiKey.NAME_FIELD)
            );
        }
        throw new IllegalArgumentException("[" + ServiceAccountAuthor.API_KEY_FIELD + "] is not an object");
    }

    @Nullable
    private static RealmDomain parseRealmDomain(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            final Map<String, ?> domainMap = (Map<String, ?>) map;
            try (XContentParser parser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, domainMap)) {
                return RealmDomain.fromXContent(parser);
            } catch (Exception e) {
                throw new IllegalArgumentException("[" + ServiceAccountAuthor.REALM_DOMAIN_FIELD + "] could not be parsed", e);
            }
        }
        throw new IllegalArgumentException("[" + ServiceAccountAuthor.REALM_DOMAIN_FIELD + "] is not an object");
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
}
