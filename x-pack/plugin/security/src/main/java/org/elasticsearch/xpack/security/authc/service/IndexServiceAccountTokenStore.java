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
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
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
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken.ServiceAccountTokenId;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

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
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class IndexServiceAccountTokenStore extends CachingServiceAccountTokenStore {

    private static final Logger logger = LogManager.getLogger(IndexServiceAccountTokenStore.class);
    static final String SERVICE_ACCOUNT_TOKEN_DOC_TYPE = "service_account_token";

    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final Hasher hasher;

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
        securityIndex.checkIndexVersionThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                GetAction.INSTANCE,
                getRequest,
                ActionListener.<GetResponse>wrap(response -> {
                    if (response.isExists()) {
                        final String tokenHash = (String) response.getSource().get("password");
                        assert tokenHash != null : "service account token hash cannot be null";
                        listener.onResponse(
                            new StoreAuthenticationResult(Hasher.verifyHash(token.getSecret(), tokenHash.toCharArray()), getTokenSource())
                        );
                    } else {
                        logger.trace("service account token [{}] not found in index", token.getQualifiedName());
                        listener.onResponse(new StoreAuthenticationResult(false, getTokenSource()));
                    }
                }, listener::onFailure)
            )
        );
    }

    @Override
    public TokenSource getTokenSource() {
        return TokenSource.INDEX;
    }

    void createToken(
        Authentication authentication,
        CreateServiceAccountTokenRequest request,
        ActionListener<CreateServiceAccountTokenResponse> listener
    ) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        if (false == ServiceAccountService.isServiceAccountPrincipal(accountId.asPrincipal())) {
            listener.onFailure(new IllegalArgumentException("service account [" + accountId + "] does not exist"));
            return;
        }
        final ServiceAccountToken token = ServiceAccountToken.newToken(accountId, request.getTokenName());
        try (XContentBuilder builder = newDocument(authentication, token)) {
            final IndexRequest indexRequest = client.prepareIndex(SECURITY_MAIN_ALIAS)
                .setId(docIdForToken(token.getQualifiedName()))
                .setSource(builder)
                .setOpType(OpType.CREATE)
                .setRefreshPolicy(request.getRefreshPolicy())
                .request();
            final BulkRequest bulkRequest = toSingleItemBulkRequest(indexRequest);

            securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
                executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    BulkAction.INSTANCE,
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
        final SecurityIndexManager frozenSecurityIndex = this.securityIndex.freeze();
        if (false == frozenSecurityIndex.indexExists()) {
            listener.onResponse(List.of());
        } else if (false == frozenSecurityIndex.isAvailable()) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            securityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final Supplier<ThreadContext.StoredContext> contextSupplier = client.threadPool()
                    .getThreadContext()
                    .newRestorableContext(false);
                try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
                    // TODO: wildcard support?
                    final BoolQueryBuilder query = QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_TOKEN_DOC_TYPE))
                        .must(QueryBuilders.termQuery("username", accountId.asPrincipal()));
                    final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                        .setScroll(DEFAULT_KEEPALIVE_SETTING.get(getSettings()))
                        .setQuery(query)
                        .setSize(1000)
                        .setFetchSource(false)
                        .request();
                    request.indicesOptions().ignoreUnavailable();

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

    void deleteToken(DeleteServiceAccountTokenRequest request, ActionListener<Boolean> listener) {
        final SecurityIndexManager frozenSecurityIndex = this.securityIndex.freeze();
        if (false == frozenSecurityIndex.indexExists()) {
            listener.onResponse(false);
        } else if (false == frozenSecurityIndex.isAvailable()) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
            if (false == ServiceAccountService.isServiceAccountPrincipal(accountId.asPrincipal())) {
                listener.onResponse(false);
                return;
            }
            final ServiceAccountTokenId accountTokenId = new ServiceAccountTokenId(accountId, request.getTokenName());
            final String qualifiedTokenName = accountTokenId.getQualifiedName();
            securityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final DeleteRequest deleteRequest = client.prepareDelete(SECURITY_MAIN_ALIAS, docIdForToken(qualifiedTokenName)).request();
                deleteRequest.setRefreshPolicy(request.getRefreshPolicy());
                executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    DeleteAction.INSTANCE,
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

    private String docIdForToken(String qualifiedTokenName) {
        return SERVICE_ACCOUNT_TOKEN_DOC_TYPE + "-" + qualifiedTokenName;
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

    private TokenInfo extractTokenInfo(String docId, ServiceAccountId accountId) {
        // Prefix is SERVICE_ACCOUNT_TOKEN_DOC_TYPE + "-" + accountId.asPrincipal() + "/"
        final int prefixLength = SERVICE_ACCOUNT_TOKEN_DOC_TYPE.length() + accountId.asPrincipal().length() + 2;
        return TokenInfo.indexToken(Strings.substring(docId, prefixLength, docId.length()));
    }
}
