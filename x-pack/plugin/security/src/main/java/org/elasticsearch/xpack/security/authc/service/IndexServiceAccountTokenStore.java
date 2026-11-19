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
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo.TokenSource;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken.ServiceAccountTokenId;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class IndexServiceAccountTokenStore extends CachingServiceAccountTokenStore {

    private static final Logger logger = LogManager.getLogger(IndexServiceAccountTokenStore.class);
    static final String SERVICE_ACCOUNT_TOKEN_DOC_TYPE = "service_account_token";

    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final Hasher hasher;

    @SuppressWarnings("this-escape")
    public IndexServiceAccountTokenStore(
        Settings settings,
        ThreadPool threadPool,
        Clock clock,
        Client client,
        SecurityIndexManager securityIndex,
        ClusterService clusterService,
        CacheInvalidatorRegistry cacheInvalidatorRegistry
    ) {
        super(settings, threadPool);
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        cacheInvalidatorRegistry.registerCacheInvalidator("index_service_account_token", this);
        this.hasher = Hasher.resolve(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(settings));
    }

    @Override
    void doAuthenticate(ServiceAccountToken token, ActionListener<StoreAuthenticationResult> listener) {
        final GetRequest getRequest = client.prepareGet(SECURITY_MAIN_ALIAS, docIdForToken(token.getQualifiedName()))
            .setFetchSource(true)
            .request();
        securityIndex.forCurrentProject()
            .checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    TransportGetAction.TYPE,
                    getRequest,
                    ActionListener.<GetResponse>wrap(response -> {
                        if (response.isExists()) {
                            final String tokenHash = (String) response.getSource().get("password");
                            assert tokenHash != null : "service account token hash cannot be null";
                            listener.onResponse(
                                StoreAuthenticationResult.fromBooleanResult(
                                    getTokenSource(),
                                    Hasher.verifyHash(token.getSecret(), tokenHash.toCharArray())
                                )
                            );
                        } else {
                            logger.trace("service account token [{}] not found in index", token.getQualifiedName());
                            listener.onResponse(StoreAuthenticationResult.failed(getTokenSource()));
                        }
                    }, listener::onFailure)
                )
            );
    }

    @Override
    public TokenSource getTokenSource() {
        return TokenSource.INDEX;
    }

    /**
     * Creates a token for a built-in service account, failing if no built-in account carries the requested principal.
     */
    void createBuiltInToken(
        Authentication authentication,
        CreateServiceAccountTokenRequest request,
        ActionListener<CreateServiceAccountTokenResponse> listener
    ) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        if (false == ServiceAccountService.isBuiltInServiceAccountPrincipal(accountId.asPrincipal())) {
            listener.onFailure(new IllegalArgumentException("service account [" + accountId + "] does not exist"));
            return;
        }
        createToken(authentication, request, listener);
    }

    /**
     * Creates a token for a user-managed service account, failing if the principal is not a well-formed user-managed
     * account ID. That the account actually <em>exists</em> cannot be checked here — user-managed accounts live in
     * {@link UserManagedServiceAccountStore}, which this store cannot consult synchronously — so the caller must resolve
     * it first. The ID itself is still checked because nothing downstream would: the document is written under whatever
     * principal it is handed, so an unchecked one would store a working credential under a name no account could carry.
     */
    void createUserManagedToken(
        Authentication authentication,
        CreateServiceAccountTokenRequest request,
        ActionListener<CreateServiceAccountTokenResponse> listener
    ) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        final Validation.Error error = Validation.UserManagedServiceAccounts.validatePrincipal(accountId.asPrincipal());
        if (error != null) {
            listener.onFailure(new IllegalArgumentException(error.toString()));
            return;
        }
        createToken(authentication, request, listener);
    }

    private void createToken(
        Authentication authentication,
        CreateServiceAccountTokenRequest request,
        ActionListener<CreateServiceAccountTokenResponse> listener
    ) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        final ServiceAccountToken token = ServiceAccountToken.newToken(accountId, request.getTokenName());
        try (XContentBuilder builder = newDocument(authentication, token)) {
            final IndexRequest indexRequest = client.prepareIndex(SECURITY_MAIN_ALIAS)
                .setId(docIdForToken(token.getQualifiedName()))
                .setSource(builder)
                .setOpType(OpType.CREATE)
                .setRefreshPolicy(request.getRefreshPolicy())
                .request();
            final BulkRequest bulkRequest = toSingleItemBulkRequest(indexRequest);

            securityIndex.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
                executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    TransportBulkAction.TYPE,
                    bulkRequest,
                    TransportBulkAction.<IndexResponse>unwrappingSingleItemBulkResponse(ActionListener.wrap(response -> {
                        assert DocWriteResponse.Result.CREATED == response.getResult()
                            : "an successful response of an OpType.CREATE request must have result of CREATED";
                        listener.onResponse(CreateServiceAccountTokenResponse.created(token.getTokenName(), token.asBearerString()));
                    }, listener::onFailure))
                );
            });
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    void findTokensFor(ServiceAccountId accountId, ActionListener<Collection<TokenInfo>> listener) {
        final IndexState projectSecurityIndex = this.securityIndex.forCurrentProject();
        if (false == projectSecurityIndex.indexExists()) {
            listener.onResponse(List.of());
        } else if (false == projectSecurityIndex.isAvailable(SEARCH_SHARDS)) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final Supplier<ThreadContext.StoredContext> contextSupplier = client.threadPool()
                    .getThreadContext()
                    .newRestorableContext(false);
                try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
                    // TODO: wildcard support?
                    final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                        .setScroll(DEFAULT_KEEPALIVE_SETTING.get(getSettings()))
                        .setQuery(tokensForAccountQuery(accountId))
                        .setSize(1000)
                        .setFetchSource(false)
                        .request();

                    logger.trace("Searching tokens for service account [{}]", accountId);
                    ScrollHelper.fetchAllByEntity(
                        client,
                        request,
                        new ContextPreservingActionListener<>(contextSupplier, listener),
                        hit -> extractTokenInfo(hit.getId(), accountId)
                    );
                }
            });
        }
    }

    /**
     * Reports whether the account has at least one index-backed token, so that a caller can refuse to delete an account
     * whose tokens would otherwise be stranded. Unlike {@link #findTokensFor} this is a bounded existence check that
     * stops at the first match and neither enumerates nor returns token names.
     */
    void hasTokensFor(ServiceAccountId accountId, ActionListener<Boolean> listener) {
        final IndexState projectSecurityIndex = this.securityIndex.forCurrentProject();
        if (false == projectSecurityIndex.indexExists()) {
            listener.onResponse(false);
        } else if (false == projectSecurityIndex.isAvailable(SEARCH_SHARDS)) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                    .setQuery(tokensForAccountQuery(accountId))
                    .setSize(0)
                    .setTerminateAfter(1)
                    .setTrackTotalHitsUpTo(1)
                    .request();

                logger.trace("Checking whether service account [{}] has any token", accountId);
                executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    TransportSearchAction.TYPE,
                    request,
                    ActionListener.wrap(response -> listener.onResponse(response.getHits().getTotalHits().value() > 0), listener::onFailure)
                );
            });
        }
    }

    /**
     * Deletes a token belonging to a built-in service account, responding {@code false} when no built-in account carries
     * the requested principal, since no such token can exist.
     */
    void deleteBuiltInToken(DeleteServiceAccountTokenRequest request, ActionListener<Boolean> listener) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        if (false == ServiceAccountService.isBuiltInServiceAccountPrincipal(accountId.asPrincipal())) {
            listener.onResponse(false);
            return;
        }
        deleteToken(request, listener);
    }

    /**
     * Deletes a token belonging to a user-managed service account, responding {@code false} when the principal is not a
     * well-formed user-managed account ID, since no such token can have been stored. As with
     * {@link #createUserManagedToken}, whether the account exists is the caller's to establish; only the ID is checked
     * here, and it keeps this entry point from operating on a built-in account's tokens.
     */
    void deleteUserManagedToken(DeleteServiceAccountTokenRequest request, ActionListener<Boolean> listener) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        if (Validation.UserManagedServiceAccounts.validatePrincipal(accountId.asPrincipal()) != null) {
            listener.onResponse(false);
            return;
        }
        deleteToken(request, listener);
    }

    private void deleteToken(DeleteServiceAccountTokenRequest request, ActionListener<Boolean> listener) {
        final IndexState projectSecurityIndex = this.securityIndex.forCurrentProject();
        if (false == projectSecurityIndex.indexExists()) {
            listener.onResponse(false);
        } else if (false == projectSecurityIndex.isAvailable(PRIMARY_SHARDS)) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else {
            final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
            final ServiceAccountTokenId accountTokenId = new ServiceAccountTokenId(accountId, request.getTokenName());
            final String qualifiedTokenName = accountTokenId.getQualifiedName();
            projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final DeleteRequest deleteRequest = client.prepareDelete(SECURITY_MAIN_ALIAS, docIdForToken(qualifiedTokenName)).request();
                deleteRequest.setRefreshPolicy(request.getRefreshPolicy());
                executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    TransportDeleteAction.TYPE,
                    deleteRequest,
                    ActionListener.wrap(deleteResponse -> {
                        final ClearSecurityCacheRequest clearSecurityCacheRequest = new ClearSecurityCacheRequest().cacheName(
                            "index_service_account_token"
                        ).keys(qualifiedTokenName);
                        executeAsyncWithOrigin(
                            client,
                            SECURITY_ORIGIN,
                            ClearSecurityCacheAction.INSTANCE,
                            clearSecurityCacheRequest,
                            ActionListener.wrap(clearSecurityCacheResponse -> {
                                listener.onResponse(deleteResponse.getResult() == DocWriteResponse.Result.DELETED);
                            }, e -> {
                                final String message = org.elasticsearch.core.Strings.format(
                                    "clearing the cache for service token [%s] failed. please clear the cache manually",
                                    qualifiedTokenName
                                );
                                logger.error(message, e);
                                listener.onFailure(new ElasticsearchException(message, e));
                            })
                        );
                    }, listener::onFailure)
                );
            });
        }
    }

    private static String docIdForToken(String qualifiedTokenName) {
        return SERVICE_ACCOUNT_TOKEN_DOC_TYPE + "-" + qualifiedTokenName;
    }

    /**
     * Matches the token documents of a single service account. The {@code doc_type} clause is what keeps this from also
     * matching the {@code service_account} documents that {@link UserManagedServiceAccountStore} stores under the same
     * {@code username}.
     */
    private static BoolQueryBuilder tokensForAccountQuery(ServiceAccountId accountId) {
        return QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_TOKEN_DOC_TYPE))
            .must(QueryBuilders.termQuery("username", accountId.asPrincipal()));
    }

    private XContentBuilder newDocument(Authentication authentication, ServiceAccountToken serviceAccountToken) throws IOException {
        final Version version = clusterService.state().nodes().getMinNodeVersion();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
            .field("doc_type", SERVICE_ACCOUNT_TOKEN_DOC_TYPE)
            .field("version", version.id)
            .field("username", serviceAccountToken.getAccountId().asPrincipal())
            .field("name", serviceAccountToken.getTokenName())
            .field("creation_time", clock.instant().toEpochMilli())
            .field("enabled", true);
        {
            final Subject effectiveSubject = authentication.getEffectiveSubject();
            builder.startObject("creator")
                .field("principal", effectiveSubject.getUser().principal())
                .field("full_name", effectiveSubject.getUser().fullName())
                .field("email", effectiveSubject.getUser().email())
                .field("metadata", effectiveSubject.getUser().metadata())
                .field("realm", effectiveSubject.getRealm().getName())
                .field("realm_type", effectiveSubject.getRealm().getType());
            if (effectiveSubject.getRealm().getDomain() != null) {
                builder.field("realm_domain", effectiveSubject.getRealm().getDomain());
            }
            builder.endObject();
        }
        byte[] utf8Bytes = null;
        final char[] tokenHash = hasher.hash(serviceAccountToken.getSecret());
        try {
            utf8Bytes = CharArrays.toUtf8Bytes(tokenHash);
            builder.field("password").utf8Value(utf8Bytes, 0, utf8Bytes.length);
        } finally {
            if (utf8Bytes != null) {
                Arrays.fill(utf8Bytes, (byte) 0);
            }
            Arrays.fill(tokenHash, (char) 0);
        }
        builder.endObject();
        return builder;
    }

    private static TokenInfo extractTokenInfo(String docId, ServiceAccountId accountId) {
        // Prefix is SERVICE_ACCOUNT_TOKEN_DOC_TYPE + "-" + accountId.asPrincipal() + "/"
        final int prefixLength = SERVICE_ACCOUNT_TOKEN_DOC_TYPE.length() + accountId.asPrincipal().length() + 2;
        return TokenInfo.indexToken(Strings.substring(docId, prefixLength, docId.length()));
    }
}
