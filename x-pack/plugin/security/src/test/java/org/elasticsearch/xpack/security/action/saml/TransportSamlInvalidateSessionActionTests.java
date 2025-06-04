/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlInvalidateSessionResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.TokenServiceTests;
import org.elasticsearch.xpack.security.authc.saml.SamlLogoutRequestHandler;
import org.elasticsearch.xpack.security.authc.saml.SamlNameId;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRealmTestHelper;
import org.elasticsearch.xpack.security.authc.saml.SamlRealmTests;
import org.elasticsearch.xpack.security.authc.saml.SamlTestCase;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;
import org.opensaml.saml.saml2.core.NameID;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSamlInvalidateSessionActionTests extends SamlTestCase {

    private static final String REALM_NAME = "saml1";

    private SamlRealm samlRealm;
    private TokenService tokenService;
    private List<IndexRequest> indexRequests;
    private List<BulkRequest> bulkRequests;
    private List<SearchRequest> searchRequests;
    private TransportSamlInvalidateSessionAction action;
    private SamlLogoutRequestHandler.Result logoutRequest;
    private Function<SearchRequest, SearchHit[]> searchFunction = ignore -> SearchHits.EMPTY;
    private ThreadPool threadPool;

    @Before
    public void setup() throws Exception {
        final RealmIdentifier realmId = new RealmIdentifier("saml", REALM_NAME);
        final Path metadata = PathUtils.get(SamlRealm.class.getResource("idp1.xml").toURI());
        final Settings settings = Settings.builder()
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put("path.home", createTempDir())
            .put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_PATH), metadata.toString())
            .put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_ENTITY_ID),
                SamlRealmTests.TEST_IDP_ENTITY_ID
            )
            .put(getFullSettingKey(REALM_NAME, SingleSpSamlRealmSettings.SP_ENTITY_ID), SamlRealmTestHelper.SP_ENTITY_ID)
            .put(getFullSettingKey(REALM_NAME, SingleSpSamlRealmSettings.SP_ACS), SamlRealmTestHelper.SP_ACS_URL)
            .put(getFullSettingKey(REALM_NAME, SingleSpSamlRealmSettings.SP_LOGOUT), SamlRealmTestHelper.SP_LOGOUT_URL)
            .put(
                getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(SingleSpSamlRealmSettings.TYPE).getAttribute()),
                "uid"
            )
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();

        this.threadPool = new TestThreadPool("saml test thread pool", settings);
        final ThreadContext threadContext = threadPool.getThreadContext();
        final var defaultContext = threadContext.newStoredContext();
        AuthenticationTestHelper.builder()
            .user(new User("kibana"))
            .realmRef(new RealmRef("realm", "type", "node"))
            .build(false)
            .writeToContext(threadContext);

        indexRequests = new ArrayList<>();
        searchRequests = new ArrayList<>();
        bulkRequests = new ArrayList<>();
        final Client client = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportIndexAction.NAME.equals(action.name())) {
                    assertThat(request, instanceOf(IndexRequest.class));
                    IndexRequest indexRequest = (IndexRequest) request;
                    indexRequests.add(indexRequest);
                    final IndexResponse response = new IndexResponse(new ShardId("test", "test", 0), indexRequest.id(), 1, 1, 1, true);
                    listener.onResponse((Response) response);
                } else if (TransportBulkAction.NAME.equals(action.name())) {
                    assertThat(request, instanceOf(BulkRequest.class));
                    BulkRequest bulkRequest = (BulkRequest) request;
                    bulkRequests.add(bulkRequest);
                    BulkItemResponse[] bulkItemResponses = new BulkItemResponse[bulkRequest.requests().size()];
                    for (int i = 0; i < bulkItemResponses.length; i++) {
                        UpdateResponse updateResponse = mock(UpdateResponse.class);
                        DocWriteResponse.Result docWriteResponse = randomFrom(
                            DocWriteResponse.Result.UPDATED,
                            DocWriteResponse.Result.NOOP
                        );
                        when(updateResponse.getResult()).thenReturn(docWriteResponse);
                        GetResult getResult = mock(GetResult.class);
                        when(getResult.getId()).thenReturn(bulkRequest.requests().get(i).id());
                        when(updateResponse.getGetResult()).thenReturn(getResult);
                        bulkItemResponses[i] = BulkItemResponse.success(i, DocWriteRequest.OpType.UPDATE, updateResponse);
                    }
                    BulkResponse response = new BulkResponse(bulkItemResponses, 1);
                    listener.onResponse((Response) response);
                } else if (TransportSearchAction.TYPE.name().equals(action.name())) {
                    assertThat(request, instanceOf(SearchRequest.class));
                    SearchRequest searchRequest = (SearchRequest) request;
                    searchRequests.add(searchRequest);
                    final SearchHit[] hits = searchFunction.apply(searchRequest);
                    final var searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f);
                    try {
                        ActionListener.respondAndRelease(
                            listener,
                            (Response) SearchResponseUtils.response(searchHits).scrollId("_scrollId1").build()
                        );
                    } finally {
                        searchHits.decRef();
                    }
                } else if (TransportSearchScrollAction.TYPE.name().equals(action.name())) {
                    assertThat(request, instanceOf(SearchScrollRequest.class));
                    ActionListener.respondAndRelease(
                        listener,
                        (Response) SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS).scrollId("_scrollId1").build()
                    );
                } else if (TransportClearScrollAction.NAME.equals(action.name())) {
                    assertThat(request, instanceOf(ClearScrollRequest.class));
                    ClearScrollRequest scrollRequest = (ClearScrollRequest) request;
                    assertEquals("_scrollId1", scrollRequest.getScrollIds().get(0));
                    ClearScrollResponse response = new ClearScrollResponse(true, 1);
                    listener.onResponse((Response) response);
                } else if (RefreshAction.NAME.equals(action.name())) {
                    assertThat(request, instanceOf(RefreshRequest.class));
                    listener.onResponse((Response) mock(BroadcastResponse.class));
                } else {
                    super.doExecute(action, request, listener);
                }
            }
        };

        final SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        SecurityIndexManager.IndexState projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        doAnswer(inv -> {
            ((Runnable) inv.getArguments()[1]).run();
            return null;
        }).when(projectIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));
        doAnswer(inv -> {
            ((Runnable) inv.getArguments()[1]).run();
            return null;
        }).when(projectIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isIndexUpToDate()).thenReturn(true);
        when(projectIndex.getCreationTime()).thenReturn(Clock.systemUTC().instant());
        when(projectIndex.aliasName()).thenReturn(".security");

        final MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);

        final ClusterService clusterService;
        try (var ignored = threadContext.newStoredContext()) {
            defaultContext.restore();
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
        }
        final SecurityContext securityContext = new SecurityContext(settings, threadContext);
        tokenService = new TokenService(
            settings,
            Clock.systemUTC(),
            client,
            licenseState,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService
        );

        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final Realms realms = mock(Realms.class);
        action = new TransportSamlInvalidateSessionAction(transportService, mock(ActionFilters.class), tokenService, realms);

        final Environment env = TestEnvironment.newEnvironment(settings);

        final RealmConfig realmConfig = new RealmConfig(realmId, settings, env, threadContext);
        samlRealm = SamlRealmTestHelper.buildRealm(realmConfig, null);
        when(realms.realm(realmConfig.name())).thenReturn(samlRealm);
        when(realms.stream()).thenAnswer(i -> Stream.of(samlRealm));

        logoutRequest = new SamlLogoutRequestHandler.Result(
            randomAlphaOfLengthBetween(8, 24),
            new SamlNameId(NameID.TRANSIENT, randomAlphaOfLengthBetween(8, 24), null, null, null),
            randomAlphaOfLengthBetween(12, 16),
            null
        );
        when(samlRealm.getLogoutHandler().parseFromQueryString(anyString())).thenReturn(logoutRequest);
    }

    private SearchHit tokenHit(int idx, BytesReference source) {
        try {
            final Map<String, Object> sourceMap = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source.streamInput())
                .map();
            @SuppressWarnings("unchecked")
            final Map<String, Object> accessToken = (Map<String, Object>) sourceMap.get("access_token");
            @SuppressWarnings("unchecked")
            final Map<String, Object> userToken = (Map<String, Object>) accessToken.get("user_token");
            final SearchHit hit = new SearchHit(idx, "token_" + userToken.get("id"));
            hit.sourceRef(source);
            return hit;
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    @After
    public void cleanup() {
        samlRealm.close();
        threadPool.shutdown();
    }

    public void testInvalidateCorrectTokensFromLogoutRequest() throws Exception {
        Tuple<byte[], byte[]> newTokenBytes1 = tokenService.getRandomTokenBytes(true);
        Tuple<byte[], byte[]> newTokenBytes2 = tokenService.getRandomTokenBytes(true);
        Tuple<byte[], byte[]> newTokenBytes3 = tokenService.getRandomTokenBytes(true);
        Tuple<byte[], byte[]> newTokenBytes4 = tokenService.getRandomTokenBytes(true);
        storeToken(newTokenBytes3.v1(), newTokenBytes3.v2(), logoutRequest.getNameId(), randomAlphaOfLength(10));
        final TokenService.CreateTokenResult tokenToInvalidate1 = storeToken(
            newTokenBytes1.v1(),
            newTokenBytes1.v2(),
            logoutRequest.getNameId(),
            logoutRequest.getSession()
        );
        storeToken(newTokenBytes2.v1(), newTokenBytes2.v2(), logoutRequest.getNameId(), logoutRequest.getSession());
        storeToken(
            newTokenBytes4.v1(),
            newTokenBytes4.v2(),
            new SamlNameId(NameID.PERSISTENT, randomAlphaOfLength(16), null, null, null),
            logoutRequest.getSession()
        );

        assertThat(indexRequests, hasSize(4));

        final AtomicInteger counter = new AtomicInteger();
        final SearchHit[] searchHits = indexRequests.stream()
            .filter(r -> r.id().startsWith("token"))
            .map(r -> tokenHit(counter.incrementAndGet(), r.source()))
            .collect(Collectors.toList())
            .toArray(SearchHits.EMPTY);
        assertThat(searchHits.length, equalTo(4));
        searchFunction = req1 -> {
            searchFunction = findTokenByRefreshToken(searchHits);
            return searchHits;
        };

        indexRequests.clear();

        final SamlInvalidateSessionRequest request = new SamlInvalidateSessionRequest();
        request.setRealmName(samlRealm.name());
        request.setQueryString("SAMLRequest=foo");
        final PlainActionFuture<SamlInvalidateSessionResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        final SamlInvalidateSessionResponse response = future.get();
        assertThat(response, notNullValue());
        assertThat(response.getCount(), equalTo(4));
        assertThat(response.getRealmName(), equalTo(samlRealm.name()));
        assertThat(response.getRedirectUrl(), notNullValue());
        assertThat(response.getRedirectUrl(), startsWith(SamlRealmTestHelper.IDP_LOGOUT_URL));
        assertThat(response.getRedirectUrl(), containsString("SAMLResponse="));

        // only 1 search request to find the tokens for the realm
        assertThat(searchRequests, hasSize(1));

        assertThat(searchRequests.get(0).source().query(), instanceOf(BoolQueryBuilder.class));
        final List<QueryBuilder> filter0 = ((BoolQueryBuilder) searchRequests.get(0).source().query()).filter();
        assertThat(filter0, iterableWithSize(3));

        assertThat(filter0.get(0), instanceOf(TermQueryBuilder.class));
        assertThat(((TermQueryBuilder) filter0.get(0)).fieldName(), equalTo("doc_type"));
        assertThat(((TermQueryBuilder) filter0.get(0)).value(), equalTo("token"));

        assertThat(filter0.get(1), instanceOf(TermQueryBuilder.class));
        assertThat(((TermQueryBuilder) filter0.get(1)).fieldName(), equalTo("access_token.realm"));
        assertThat(((TermQueryBuilder) filter0.get(1)).value(), equalTo(samlRealm.name()));

        assertThat(filter0.get(2), instanceOf(BoolQueryBuilder.class));
        assertThat(((BoolQueryBuilder) filter0.get(2)).should(), iterableWithSize(2));

        assertThat(
            tokenToInvalidate1.getAuthentication(),
            equalTo(
                AuthenticationTestHelper.builder()
                    .realm()
                    .user(new User("bob"))
                    .realmRef(new RealmRef("native", NativeRealmSettings.TYPE, "node01"))
                    .build(false)
            )
        );

        assertThat(bulkRequests, hasSize(2)); // 2 updates (first for all (2) the refresh-tokens, second one for all (2) the access-tokens)
        assertThat(bulkRequests.get(0).requests().size(), equalTo(2));
        assertThat(bulkRequests.get(1).requests().size(), equalTo(2));
        assertThat(bulkRequests.get(0).requests().get(0), instanceOf(UpdateRequest.class));
        assertThat(bulkRequests.get(0).requests().get(1), instanceOf(UpdateRequest.class));
        assertThat(bulkRequests.get(1).requests().get(0), instanceOf(UpdateRequest.class));
        assertThat(bulkRequests.get(1).requests().get(1), instanceOf(UpdateRequest.class));
        UpdateRequest updateRequest1 = (UpdateRequest) bulkRequests.get(0).requests().get(0);
        UpdateRequest updateRequest2 = (UpdateRequest) bulkRequests.get(0).requests().get(1);
        UpdateRequest updateRequest3 = (UpdateRequest) bulkRequests.get(1).requests().get(0);
        UpdateRequest updateRequest4 = (UpdateRequest) bulkRequests.get(1).requests().get(1);
        assertThat(updateRequest1.toString(), containsString("refresh_token"));
        assertThat(updateRequest2.toString(), containsString("refresh_token"));
        assertThat(updateRequest3.toString(), containsString("access_token"));
        assertThat(updateRequest4.toString(), containsString("access_token"));
        assertThat(
            List.of(updateRequest1.id(), updateRequest2.id()),
            containsInAnyOrder(
                "token_"
                    + TokenServiceTests.tokenDocIdFromAccessTokenBytes(newTokenBytes1.v1(), tokenService.getTokenVersionCompatibility()),
                "token_"
                    + TokenServiceTests.tokenDocIdFromAccessTokenBytes(newTokenBytes2.v1(), tokenService.getTokenVersionCompatibility())
            )
        );
        assertThat(
            List.of(updateRequest3.id(), updateRequest4.id()),
            containsInAnyOrder(
                "token_"
                    + TokenServiceTests.tokenDocIdFromAccessTokenBytes(newTokenBytes1.v1(), tokenService.getTokenVersionCompatibility()),
                "token_"
                    + TokenServiceTests.tokenDocIdFromAccessTokenBytes(newTokenBytes2.v1(), tokenService.getTokenVersionCompatibility())
            )
        );
    }

    private Function<SearchRequest, SearchHit[]> findTokenByRefreshToken(SearchHit[] searchHits) {
        return request -> {
            assertThat(request.source().query(), instanceOf(BoolQueryBuilder.class));
            final List<QueryBuilder> filters = ((BoolQueryBuilder) request.source().query()).filter();
            assertThat(filters, iterableWithSize(2));
            assertThat(filters.get(1), instanceOf(TermQueryBuilder.class));
            final TermQueryBuilder termQuery = (TermQueryBuilder) filters.get(1);
            assertThat(termQuery.fieldName(), equalTo("refresh_token.token"));
            for (SearchHit hit : searchHits) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> refreshToken = (Map<String, Object>) hit.getSourceAsMap().get("refresh_token");
                if (termQuery.value().equals(refreshToken.get("token"))) {
                    return new SearchHit[] { hit };
                }
            }
            return SearchHits.EMPTY;
        };
    }

    private TokenService.CreateTokenResult storeToken(byte[] userTokenBytes, byte[] refreshTokenBytes, SamlNameId nameId, String session) {
        Authentication authentication = AuthenticationTestHelper.builder()
            .realm()
            .user(new User("bob"))
            .realmRef(new RealmRef("native", NativeRealmSettings.TYPE, "node01"))
            .build(false);
        final Map<String, Object> metadata = samlRealm.createTokenMetadata(nameId, session);
        final PlainActionFuture<TokenService.CreateTokenResult> future = new PlainActionFuture<>();
        tokenService.createOAuth2Tokens(userTokenBytes, refreshTokenBytes, authentication, authentication, metadata, future);
        return future.actionGet();
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
