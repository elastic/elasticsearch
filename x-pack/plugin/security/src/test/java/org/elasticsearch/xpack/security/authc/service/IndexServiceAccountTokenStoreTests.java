/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo.TokenSource;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore.StoreAuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.xpack.security.authc.service.IndexServiceAccountTokenStore.SERVICE_ACCOUNT_TOKEN_DOC_TYPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexServiceAccountTokenStoreTests extends ESTestCase {

    private Client client;
    private ClusterService clusterService;
    private CacheInvalidatorRegistry cacheInvalidatorRegistry;
    private IndexServiceAccountTokenStore store;
    private final AtomicReference<ActionRequest> requestHolder = new AtomicReference<>();
    private final AtomicReference<BiConsumer<ActionRequest, ActionListener<ActionResponse>>> responseProviderHolder =
        new AtomicReference<>();
    private SecurityIndexManager securityIndex;

    @Before
    public void init() {
        Client mockClient = mock(Client.class);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = new FilterClient(mockClient) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requestHolder.set(request);
                responseProviderHolder.get().accept(request, (ActionListener<ActionResponse>) listener);
            }
        };
        clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(Version.CURRENT);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(clusterService.state()).thenReturn(clusterState);
        cacheInvalidatorRegistry = mock(CacheInvalidatorRegistry.class);

        securityIndex = mock(SecurityIndexManager.class);
        SecurityIndexManager.IndexState projectIndex = Mockito.mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isIndexUpToDate()).thenReturn(true);
        doAnswer((i) -> {
            Runnable action = (Runnable) i.getArguments()[1];
            action.run();
            return null;
        }).when(projectIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));
        doAnswer((i) -> {
            Runnable action = (Runnable) i.getArguments()[1];
            action.run();
            return null;
        }).when(projectIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        store = new IndexServiceAccountTokenStore(
            Settings.EMPTY,
            threadPool,
            Clock.systemUTC(),
            client,
            securityIndex,
            clusterService,
            cacheInvalidatorRegistry
        );
    }

    public void testDoAuthenticate() throws IOException, ExecutionException, InterruptedException, IllegalAccessException {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final ServiceAccountToken serviceAccountToken = ServiceAccountToken.newToken(accountId, randomAlphaOfLengthBetween(3, 8));
        final GetResponse getResponse1 = createGetResponse(serviceAccountToken, true);

        // success
        responseProviderHolder.set((r, l) -> l.onResponse(getResponse1));
        final PlainActionFuture<StoreAuthenticationResult> future1 = new PlainActionFuture<>();
        store.doAuthenticate(serviceAccountToken, future1);
        final GetRequest getRequest = (GetRequest) requestHolder.get();
        assertThat(getRequest.id(), equalTo("service_account_token-" + serviceAccountToken.getQualifiedName()));
        assertThat(future1.get().isSuccess(), is(true));
        assertThat(future1.get().getTokenSource(), is(TokenSource.INDEX));

        // token mismatch
        final GetResponse getResponse2 = createGetResponse(ServiceAccountToken.newToken(accountId, randomAlphaOfLengthBetween(3, 8)), true);
        responseProviderHolder.set((r, l) -> l.onResponse(getResponse2));
        final PlainActionFuture<StoreAuthenticationResult> future2 = new PlainActionFuture<>();
        store.doAuthenticate(serviceAccountToken, future2);
        assertThat(future2.get().isSuccess(), is(false));
        assertThat(future2.get().getTokenSource(), is(TokenSource.INDEX));

        // token document not found
        final GetResponse getResponse3 = createGetResponse(serviceAccountToken, false);
        responseProviderHolder.set((r, l) -> l.onResponse(getResponse3));
        final PlainActionFuture<StoreAuthenticationResult> future3 = new PlainActionFuture<>();
        store.doAuthenticate(serviceAccountToken, future3);
        assertThat(future3.get().isSuccess(), is(false));
        assertThat(future3.get().getTokenSource(), is(TokenSource.INDEX));
    }

    public void testCreateToken() throws ExecutionException, InterruptedException {
        final Authentication authentication = createAuthentication();
        final CreateServiceAccountTokenRequest request = new CreateServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            randomAlphaOfLengthBetween(3, 8)
        );

        // created
        responseProviderHolder.set((r, l) -> l.onResponse(createSingleBulkResponse()));
        final PlainActionFuture<CreateServiceAccountTokenResponse> future1 = new PlainActionFuture<>();
        store.createToken(authentication, request, future1);
        final BulkRequest bulkRequest = (BulkRequest) requestHolder.get();
        assertThat(bulkRequest.requests(), hasSize(1));
        final IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
        final Map<String, Object> sourceMap = indexRequest.sourceAsMap();
        assertThat(sourceMap.get("username"), equalTo("elastic/fleet-server"));
        assertThat(sourceMap.get("name"), equalTo(request.getTokenName()));
        assertThat(sourceMap.get("doc_type"), equalTo("service_account_token"));
        assertThat(sourceMap.get("version"), equalTo(Version.CURRENT.id));
        assertThat(sourceMap.get("password"), notNullValue());
        assertThat(Hasher.resolveFromHash(((String) sourceMap.get("password")).toCharArray()), equalTo(Hasher.PBKDF2_STRETCH));
        assertThat(sourceMap.get("creation_time"), notNullValue());
        @SuppressWarnings("unchecked")
        final Map<String, Object> creatorMap = (Map<String, Object>) sourceMap.get("creator");
        assertThat(creatorMap, notNullValue());
        assertThat(creatorMap.get("principal"), equalTo(authentication.getEffectiveSubject().getUser().principal()));
        assertThat(creatorMap.get("full_name"), equalTo(authentication.getEffectiveSubject().getUser().fullName()));
        assertThat(creatorMap.get("email"), equalTo(authentication.getEffectiveSubject().getUser().email()));
        assertThat(creatorMap.get("metadata"), equalTo(authentication.getEffectiveSubject().getUser().metadata()));
        assertThat(creatorMap.get("realm"), equalTo(authentication.getEffectiveSubject().getRealm().getName()));
        assertThat(creatorMap.get("realm_type"), equalTo(authentication.getEffectiveSubject().getRealm().getType()));

        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse1 = future1.get();
        assertNotNull(createServiceAccountTokenResponse1);
        assertThat(createServiceAccountTokenResponse1.getName(), equalTo(request.getTokenName()));
        assertNotNull(createServiceAccountTokenResponse1.getValue());

        // failure
        final Exception exception = mock(Exception.class);
        responseProviderHolder.set((r, l) -> l.onFailure(exception));
        final PlainActionFuture<CreateServiceAccountTokenResponse> future3 = new PlainActionFuture<>();
        store.createToken(authentication, request, future3);
        final ExecutionException e3 = expectThrows(ExecutionException.class, () -> future3.get());
        assertThat(e3.getCause(), is(exception));
    }

    public void testCreateTokenWillFailForInvalidServiceAccount() {
        final Authentication authentication = createAuthentication();
        final CreateServiceAccountTokenRequest request = randomValueOtherThanMany(
            r -> "elastic".equals(r.getNamespace()) && "fleet-server".equals(r.getServiceName()),
            () -> new CreateServiceAccountTokenRequest(
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8)
            )
        );
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        store.createToken(authentication, request, future);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            containsString("service account [" + request.getNamespace() + "/" + request.getServiceName() + "] does not exist")
        );
    }

    public void testFindTokensFor() {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final int nhits = randomIntBetween(0, 10);
        final String[] tokenNames = randomArray(nhits, nhits, String[]::new, ValidationTests::randomTokenName);

        responseProviderHolder.set((r, l) -> {
            if (r instanceof SearchRequest) {
                final SearchHit[] hits = IntStream.range(0, nhits)
                    .mapToObj(
                        i -> SearchHit.unpooled(
                            randomIntBetween(0, Integer.MAX_VALUE),
                            SERVICE_ACCOUNT_TOKEN_DOC_TYPE + "-" + accountId.asPrincipal() + "/" + tokenNames[i]
                        )
                    )
                    .toArray(SearchHit[]::new);
                ActionListener.respondAndRelease(
                    l,
                    SearchResponseUtils.successfulResponse(
                        SearchHits.unpooled(hits, new TotalHits(nhits, TotalHits.Relation.EQUAL_TO), randomFloat(), null, null, null)
                    )
                );
            } else if (r instanceof ClearScrollRequest) {
                l.onResponse(new ClearScrollResponse(true, 1));
            } else {
                fail("unexpected request " + r);
            }
        });

        final PlainActionFuture<Collection<TokenInfo>> future = new PlainActionFuture<>();
        store.findTokensFor(accountId, future);
        final Collection<TokenInfo> tokenInfos = future.actionGet();
        assertThat(tokenInfos.stream().map(TokenInfo::getSource).allMatch(TokenSource.INDEX::equals), is(true));
        assertThat(tokenInfos.stream().map(TokenInfo::getName).collect(Collectors.toUnmodifiableSet()), equalTo(Set.of(tokenNames)));
    }

    public void testFindTokensForException() {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final RuntimeException e = new RuntimeException("fail");
        responseProviderHolder.set((r, l) -> { l.onFailure(e); });

        final PlainActionFuture<Collection<TokenInfo>> future = new PlainActionFuture<>();
        store.findTokensFor(accountId, future);
        final RuntimeException e1 = expectThrows(RuntimeException.class, future::actionGet);
        assertThat(e1, is(e));
    }

    public void testDeleteToken() {
        final AtomicBoolean cacheCleared = new AtomicBoolean(false);
        responseProviderHolder.set((r, l) -> {
            if (r instanceof final DeleteRequest dr) {
                final boolean found = dr.id().equals(SERVICE_ACCOUNT_TOKEN_DOC_TYPE + "-elastic/fleet-server/token1");
                l.onResponse(
                    new DeleteResponse(
                        mock(ShardId.class),
                        randomAlphaOfLengthBetween(3, 8),
                        randomLong(),
                        randomLong(),
                        randomLong(),
                        found
                    )
                );
            } else if (r instanceof ClearSecurityCacheRequest) {
                cacheCleared.set(true);
                l.onResponse(
                    new ClearSecurityCacheResponse(mock(ClusterName.class), List.of(mock(ClearSecurityCacheResponse.Node.class)), List.of())
                );
            } else {
                fail("unexpected request " + r);
            }
        });

        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest1 = new DeleteServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            "token1"
        );
        final PlainActionFuture<Boolean> future1 = new PlainActionFuture<>();
        store.deleteToken(deleteServiceAccountTokenRequest1, future1);
        assertThat(future1.actionGet(), is(true));
        assertThat(cacheCleared.get(), is(true));

        // non-exist token name
        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest2 = new DeleteServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            randomAlphaOfLengthBetween(3, 8)
        );
        final PlainActionFuture<Boolean> future2 = new PlainActionFuture<>();
        store.deleteToken(deleteServiceAccountTokenRequest2, future2);
        assertThat(future2.actionGet(), is(false));

        // Invalid service account
        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest3 = new DeleteServiceAccountTokenRequest(
            randomValueOtherThan("elastic", () -> randomAlphaOfLengthBetween(3, 8)),
            "fleet-server",
            "token1"
        );
        final PlainActionFuture<Boolean> future3 = new PlainActionFuture<>();
        store.deleteToken(deleteServiceAccountTokenRequest3, future3);
        assertThat(future3.actionGet(), is(false));
    }

    public void testIndexStateIssues() {
        // Index not exists
        Mockito.reset(securityIndex);
        SecurityIndexManager.IndexState projectIndex = Mockito.mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.indexExists()).thenReturn(false);

        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final PlainActionFuture<Collection<TokenInfo>> future1 = new PlainActionFuture<>();
        store.findTokensFor(accountId, future1);
        assertThat(future1.actionGet(), equalTo(List.of()));

        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest = new DeleteServiceAccountTokenRequest(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8)
        );
        final PlainActionFuture<Boolean> future2 = new PlainActionFuture<>();
        store.deleteToken(deleteServiceAccountTokenRequest, future2);
        assertThat(future2.actionGet(), is(false));

        // Index exists but not available
        Mockito.reset(securityIndex);
        Mockito.reset(projectIndex);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(false);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(false);
        final ElasticsearchException e = new ElasticsearchException("fail");
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(e);
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(e);

        final PlainActionFuture<Collection<TokenInfo>> future3 = new PlainActionFuture<>();
        store.findTokensFor(accountId, future3);
        final ElasticsearchException e3 = expectThrows(ElasticsearchException.class, future3::actionGet);
        assertThat(e3, is(e));

        final PlainActionFuture<Boolean> future4 = new PlainActionFuture<>();
        store.deleteToken(deleteServiceAccountTokenRequest, future4);
        final ElasticsearchException e4 = expectThrows(ElasticsearchException.class, future4::actionGet);
        assertThat(e4, is(e));
    }

    private GetResponse createGetResponse(ServiceAccountToken serviceAccountToken, boolean exists) throws IOException {
        final char[] hash = Hasher.PBKDF2_STRETCH.hash(serviceAccountToken.getSecret());
        final Map<String, Object> documentMap = Map.of("password", new String(CharArrays.toUtf8Bytes(hash), StandardCharsets.UTF_8));
        return new GetResponse(
            new GetResult(
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8),
                exists ? randomLongBetween(0, Long.MAX_VALUE) : UNASSIGNED_SEQ_NO,
                exists ? randomLongBetween(1, Long.MAX_VALUE) : UNASSIGNED_PRIMARY_TERM,
                randomLong(),
                exists,
                XContentTestUtils.convertToXContent(documentMap, XContentType.JSON),
                Map.of(),
                Map.of()
            )
        );
    }

    private Authentication createAuthentication() {
        final Authentication.RealmRef lookedUpBy = randomFrom(
            new Authentication.RealmRef(
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8)
            ),
            null
        );

        if (lookedUpBy == null) {
            return AuthenticationTestHelper.builder().realm().build(false);
        } else {
            return AuthenticationTestHelper.builder().realm().runAs().realmRef(lookedUpBy).build();
        }
    }

    private BulkResponse createSingleBulkResponse() {
        return new BulkResponse(
            new BulkItemResponse[] {
                BulkItemResponse.success(
                    randomInt(),
                    OpType.CREATE,
                    new IndexResponse(mock(ShardId.class), randomAlphaOfLengthBetween(3, 8), randomLong(), randomLong(), randomLong(), true)
                ) },
            randomLong()
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
