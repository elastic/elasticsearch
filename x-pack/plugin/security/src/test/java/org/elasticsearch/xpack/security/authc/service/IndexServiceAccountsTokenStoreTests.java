/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexServiceAccountsTokenStoreTests extends ESTestCase {

    private Client client;
    private ClusterService clusterService;
    private CacheInvalidatorRegistry cacheInvalidatorRegistry;
    private IndexServiceAccountsTokenStore store;
    private final AtomicReference<ActionRequest> requestHolder = new AtomicReference<>();
    private final AtomicReference<Consumer<ActionListener<ActionResponse>>> responseProviderHolder = new AtomicReference<>();

    @Before
    public void init() {
        Client mockClient = mock(Client.class);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = new FilterClient(mockClient) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                requestHolder.set(request);
                responseProviderHolder.get().accept((ActionListener<ActionResponse>) listener);
            }
        };
        clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(Version.CURRENT);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(clusterService.state()).thenReturn(clusterState);
        cacheInvalidatorRegistry = mock(CacheInvalidatorRegistry.class);

        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.isIndexUpToDate()).thenReturn(true);
        when(securityIndex.freeze()).thenReturn(securityIndex);
        doAnswer((i) -> {
            Runnable action = (Runnable) i.getArguments()[1];
            action.run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));
        doAnswer((i) -> {
            Runnable action = (Runnable) i.getArguments()[1];
            action.run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        store = new IndexServiceAccountsTokenStore(Settings.EMPTY,
            threadPool,
            Clock.systemUTC(),
            client,
            securityIndex,
            clusterService,
            cacheInvalidatorRegistry);
    }

    public void testDoAuthenticate() throws IOException, ExecutionException, InterruptedException, IllegalAccessException {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final ServiceAccountToken serviceAccountToken = ServiceAccountToken.newToken(accountId, randomAlphaOfLengthBetween(3, 8));
        final GetResponse getResponse1 = createGetResponse(serviceAccountToken, true);

        // success
        responseProviderHolder.set(l -> l.onResponse(getResponse1));
        final PlainActionFuture<Boolean> future1 = new PlainActionFuture<>();
        store.doAuthenticate(serviceAccountToken, future1);
        final GetRequest getRequest = (GetRequest) requestHolder.get();
        assertThat(getRequest.id(), equalTo("service_account_token-" + serviceAccountToken.getQualifiedName()));
        assertThat(future1.get(), is(true));

        // token mismatch
        final GetResponse getResponse2 = createGetResponse(ServiceAccountToken.newToken(accountId, randomAlphaOfLengthBetween(3, 8)), true);
        responseProviderHolder.set(l -> l.onResponse(getResponse2));
        final PlainActionFuture<Boolean> future2 = new PlainActionFuture<>();
        store.doAuthenticate(serviceAccountToken, future2);
        assertThat(future2.get(), is(false));

        // token document not found
        final GetResponse getResponse3 = createGetResponse(serviceAccountToken, false);
        responseProviderHolder.set(l -> l.onResponse(getResponse3));
        final PlainActionFuture<Boolean> future3 = new PlainActionFuture<>();
        store.doAuthenticate(serviceAccountToken, future3);
        assertThat(future3.get(), is(false));
    }

    public void testCreateToken() throws ExecutionException, InterruptedException {
        final Authentication authentication = createAuthentication();
        final CreateServiceAccountTokenRequest request =
            new CreateServiceAccountTokenRequest("elastic", "fleet", randomAlphaOfLengthBetween(3, 8));

        // created
        responseProviderHolder.set(l -> l.onResponse(createSingleBulkResponse(true)));
        final PlainActionFuture<CreateServiceAccountTokenResponse> future1 = new PlainActionFuture<>();
        store.createToken(authentication, request, future1);
        final BulkRequest bulkRequest = (BulkRequest) requestHolder.get();
        assertThat(bulkRequest.requests().size(), equalTo(1));
        final IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
        final Map<String, Object> sourceMap = indexRequest.sourceAsMap();
        assertThat(sourceMap.get("username"), equalTo("elastic/fleet"));
        assertThat(sourceMap.get("name"), equalTo(request.getTokenName()));
        assertThat(sourceMap.get("doc_type"), equalTo("service_account_token"));
        assertThat(sourceMap.get("version"), equalTo(Version.CURRENT.id));
        assertThat(sourceMap.get("password"), notNullValue());
        assertThat(Hasher.resolveFromHash(((String) sourceMap.get("password")).toCharArray()), equalTo(Hasher.PBKDF2_STRETCH));
        assertThat(sourceMap.get("creation_time"), notNullValue());
        @SuppressWarnings("unchecked")
        final Map<String, Object> creatorMap = (Map<String, Object>) sourceMap.get("creator");
        assertThat(creatorMap, notNullValue());
        assertThat(creatorMap.get("principal"), equalTo(authentication.getUser().principal()));
        assertThat(creatorMap.get("full_name"), equalTo(authentication.getUser().fullName()));
        assertThat(creatorMap.get("email"), equalTo(authentication.getUser().email()));
        assertThat(creatorMap.get("metadata"), equalTo(authentication.getUser().metadata()));
        assertThat(creatorMap.get("realm"), equalTo(authentication.getSourceRealm().getName()));
        assertThat(creatorMap.get("realm_type"), equalTo(authentication.getSourceRealm().getType()));

        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse1 = future1.get();
        assertNotNull(createServiceAccountTokenResponse1);
        assertThat(createServiceAccountTokenResponse1.isCreated(), is(true));
        assertThat(createServiceAccountTokenResponse1.getName(), equalTo(request.getTokenName()));
        assertNotNull(createServiceAccountTokenResponse1.getValue());

        // not created
        responseProviderHolder.set(l -> l.onResponse(createSingleBulkResponse(false)));
        final PlainActionFuture<CreateServiceAccountTokenResponse> future2 = new PlainActionFuture<>();
        store.createToken(authentication, request, future2);
        final CreateServiceAccountTokenResponse createServiceAccountTokenResponse2 = future2.get();
        assertNotNull(createServiceAccountTokenResponse2);
        assertThat(createServiceAccountTokenResponse2.isCreated(), is(false));
        assertNull(createServiceAccountTokenResponse2.getName());
        assertNull(createServiceAccountTokenResponse2.getValue());

        // failure
        final Exception exception = mock(Exception.class);
        responseProviderHolder.set(l -> l.onFailure(exception));
        final PlainActionFuture<CreateServiceAccountTokenResponse> future3 = new PlainActionFuture<>();
        store.createToken(authentication, request, future3);
        final ExecutionException e3 = expectThrows(ExecutionException.class, () -> future3.get());
        assertThat(e3.getCause(), is(exception));
    }

    public void testCreateTokenWillFailForInvalidServiceAccount() {
        final Authentication authentication = createAuthentication();
        final CreateServiceAccountTokenRequest request = randomValueOtherThanMany(
            r -> "elastic".equals(r.getNamespace()) && "fleet".equals(r.getServiceName()),
            () -> new CreateServiceAccountTokenRequest(randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)));
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        store.createToken(authentication, request, future);
        final ExecutionException e = expectThrows(ExecutionException.class, () -> future.get());
        assertThat(e.getCause().getClass(), is(IllegalArgumentException.class));
        assertThat(e.getMessage(),
            containsString("service account [" + request.getNamespace() + "/" + request.getServiceName() + "] does not exist"));
    }

    private GetResponse createGetResponse(ServiceAccountToken serviceAccountToken, boolean exists) throws IOException {
        final char[] hash = Hasher.PBKDF2_STRETCH.hash(serviceAccountToken.getSecret());
        final Map<String, Object> documentMap = Map.of("password", new String(CharArrays.toUtf8Bytes(hash), StandardCharsets.UTF_8));
        return new GetResponse(new GetResult(
            randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8),
            exists ? randomLongBetween(0, Long.MAX_VALUE) : UNASSIGNED_SEQ_NO,
            exists ? randomLongBetween(1, Long.MAX_VALUE) : UNASSIGNED_PRIMARY_TERM, randomLong(), exists,
            XContentTestUtils.convertToXContent(documentMap, XContentType.JSON),
            Map.of(), Map.of()));
    }

    private Authentication createAuthentication() {
        return new Authentication(new User(randomAlphaOfLengthBetween(3, 8)),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8)),
            randomFrom(new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8),
                randomAlphaOfLengthBetween(3, 8)), null));
    }

    private BulkResponse createSingleBulkResponse(boolean created) {
        return new BulkResponse(new BulkItemResponse[] {
            new BulkItemResponse(randomInt(), OpType.CREATE, new IndexResponse(
                mock(ShardId.class), randomAlphaOfLengthBetween(3, 8), randomLong(), randomLong(), randomLong(), created
            ))
        }, randomLong());
    }
}
