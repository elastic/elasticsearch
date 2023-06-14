/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.action.apikey.AbstractCreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyTests;
import org.elasticsearch.xpack.core.security.action.apikey.BaseUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;
import org.elasticsearch.xpack.core.security.authz.RoleRestrictionTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyDoc;
import org.elasticsearch.xpack.security.authc.ApiKeyService.CachedApiKeyHashResult;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.FeatureNotEnabledException;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS;
import static org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequestTests.randomCrossClusterApiKeyAccessField;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ID_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_METADATA_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_TYPE_KEY;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.WORKFLOWS_RESTRICTION_VERSION;
import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.authc.ApiKeyService.LEGACY_SUPERUSER_ROLE_DESCRIPTOR;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApiKeyServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private Client client;
    private SecurityIndexManager securityIndex;
    private CacheInvalidatorRegistry cacheInvalidatorRegistry;
    private Clock clock;

    @Before
    public void createThreadPool() {
        threadPool = Mockito.spy(
            new TestThreadPool(
                "api key service tests",
                new FixedExecutorBuilder(
                    Settings.EMPTY,
                    SECURITY_CRYPTO_THREAD_POOL_NAME,
                    1,
                    1000,
                    "xpack.security.crypto.thread_pool",
                    false
                )
            )
        );
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    @Before
    public void setupMocks() {
        this.client = mock(Client.class);
        this.securityIndex = SecurityMocks.mockSecurityIndexManager();
        this.cacheInvalidatorRegistry = mock(CacheInvalidatorRegistry.class);
        // Mock a clock that returns real clock time by default
        clock = mock(Clock.class);
        doAnswer(invocation -> Instant.now()).when(clock).instant();
    }

    public void testCreateApiKeyUsesBulkIndexAction() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("alice", "superuser"))
            .realmRef(new RealmRef("file", "file", "node-1"))
            .build(false);
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("key-1", null, null);
        when(client.prepareIndex(anyString())).thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.prepareBulk()).thenReturn(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        when(client.threadPool()).thenReturn(threadPool);
        final AtomicBoolean bulkActionInvoked = new AtomicBoolean(false);
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            BulkRequest bulkRequest = (BulkRequest) args[1];
            assertThat(bulkRequest.numberOfActions(), is(1));
            assertThat(bulkRequest.requests().get(0), instanceOf(IndexRequest.class));
            IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
            assertThat(indexRequest.id(), is(createApiKeyRequest.getId()));
            // The index request has opType create so that it will *not* override any existing document
            assertThat(indexRequest.opType(), is(DocWriteRequest.OpType.CREATE));
            bulkActionInvoked.set(true);
            return null;
        }).when(client).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any());
        service.createApiKey(authentication, createApiKeyRequest, Set.of(), new PlainActionFuture<>());
        assertBusy(() -> assertTrue(bulkActionInvoked.get()));
    }

    @SuppressWarnings("unchecked")
    public void testGetApiKeys() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        when(client.threadPool()).thenReturn(threadPool);
        SearchRequestBuilder searchRequestBuilder = Mockito.spy(new SearchRequestBuilder(client, SearchAction.INSTANCE));
        when(client.prepareSearch(eq(SECURITY_MAIN_ALIAS))).thenReturn(searchRequestBuilder);
        final ApiKeyService service = createApiKeyService(settings);
        final AtomicReference<SearchRequest> searchRequest = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            searchRequest.set((SearchRequest) invocationOnMock.getArguments()[0]);
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
            return null;
        }).when(client).search(any(SearchRequest.class), anyActionListener());
        String[] realmNames = generateRandomStringArray(4, 4, true, true);
        String username = randomFrom(randomAlphaOfLengthBetween(3, 8), null);
        String apiKeyName = randomFrom(randomAlphaOfLengthBetween(3, 8), null);
        String[] apiKeyIds = generateRandomStringArray(4, 4, true, true);
        PlainActionFuture<GetApiKeyResponse> getApiKeyResponsePlainActionFuture = new PlainActionFuture<>();
        service.getApiKeys(realmNames, username, apiKeyName, apiKeyIds, randomBoolean(), getApiKeyResponsePlainActionFuture);
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", "api_key"));
        if (realmNames != null && realmNames.length > 0) {
            if (realmNames.length == 1) {
                boolQuery.filter(QueryBuilders.termQuery("creator.realm", realmNames[0]));
            } else {
                final BoolQueryBuilder realmsQuery = QueryBuilders.boolQuery();
                for (String realmName : realmNames) {
                    realmsQuery.should(QueryBuilders.termQuery("creator.realm", realmName));
                }
                realmsQuery.minimumShouldMatch(1);
                boolQuery.filter(realmsQuery);
            }
        }
        if (Strings.hasText(username)) {
            boolQuery.filter(QueryBuilders.termQuery("creator.principal", username));
        }
        if (Strings.hasText(apiKeyName) && "*".equals(apiKeyName) == false) {
            if (apiKeyName.endsWith("*")) {
                boolQuery.filter(QueryBuilders.prefixQuery("name", apiKeyName.substring(0, apiKeyName.length() - 1)));
            } else {
                boolQuery.filter(QueryBuilders.termQuery("name", apiKeyName));
            }
        }
        if (apiKeyIds != null && apiKeyIds.length > 0) {
            boolQuery.filter(QueryBuilders.idsQuery().addIds(apiKeyIds));
        }
        verify(searchRequestBuilder).setQuery(eq(boolQuery));
        verify(searchRequestBuilder).setFetchSource(eq(true));
        assertThat(searchRequest.get().source().query(), is(boolQuery));
        GetApiKeyResponse getApiKeyResponse = getApiKeyResponsePlainActionFuture.get();
        assertThat(getApiKeyResponse.getApiKeyInfos(), emptyArray());
    }

    @SuppressWarnings("unchecked")
    public void testInvalidateApiKeys() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        when(client.threadPool()).thenReturn(threadPool);
        SearchRequestBuilder searchRequestBuilder = Mockito.spy(new SearchRequestBuilder(client, SearchAction.INSTANCE));
        when(client.prepareSearch(eq(SECURITY_MAIN_ALIAS))).thenReturn(searchRequestBuilder);
        final ApiKeyService service = createApiKeyService(settings);
        final AtomicReference<SearchRequest> searchRequest = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            searchRequest.set((SearchRequest) invocationOnMock.getArguments()[0]);
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
            return null;
        }).when(client).search(any(SearchRequest.class), anyActionListener());
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        service.invalidateApiKeys(
            randomFrom(new String[0], null),
            randomFrom("", null),
            randomFrom("", null),
            randomFrom(new String[0], null),
            listener
        );
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), containsString("One of [api key id, api key name, username, realm name] must be specified"));
        String[] realmNames = generateRandomStringArray(4, 4, true, true);
        String username = randomFrom(randomAlphaOfLengthBetween(3, 8), null);
        String apiKeyName = randomFrom(randomAlphaOfLengthBetween(3, 8), null);
        String[] apiKeyIds = generateRandomStringArray(4, 4, true, true);
        if ((realmNames == null || realmNames.length == 0)
            && Strings.hasText(username) == false
            && Strings.hasText(apiKeyName) == false
            && (apiKeyIds == null || apiKeyIds.length == 0)) {
            username = randomAlphaOfLengthBetween(3, 8);
        }
        PlainActionFuture<InvalidateApiKeyResponse> invalidateApiKeyResponseListener = new PlainActionFuture<>();
        service.invalidateApiKeys(realmNames, username, apiKeyName, apiKeyIds, invalidateApiKeyResponseListener);
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", "api_key"));
        if (realmNames != null && realmNames.length > 0) {
            if (realmNames.length == 1) {
                boolQuery.filter(QueryBuilders.termQuery("creator.realm", realmNames[0]));
            } else {
                final BoolQueryBuilder realmsQuery = QueryBuilders.boolQuery();
                for (String realmName : realmNames) {
                    realmsQuery.should(QueryBuilders.termQuery("creator.realm", realmName));
                }
                realmsQuery.minimumShouldMatch(1);
                boolQuery.filter(realmsQuery);
            }
        }
        if (Strings.hasText(username)) {
            boolQuery.filter(QueryBuilders.termQuery("creator.principal", username));
        }
        if (Strings.hasText(apiKeyName) && "*".equals(apiKeyName) == false) {
            if (apiKeyName.endsWith("*")) {
                boolQuery.filter(QueryBuilders.prefixQuery("name", apiKeyName.substring(0, apiKeyName.length() - 1)));
            } else {
                boolQuery.filter(QueryBuilders.termQuery("name", apiKeyName));
            }
        }
        if (apiKeyIds != null && apiKeyIds.length > 0) {
            boolQuery.filter(QueryBuilders.idsQuery().addIds(apiKeyIds));
        }
        boolQuery.filter(QueryBuilders.termQuery("api_key_invalidated", false));
        verify(searchRequestBuilder).setQuery(eq(boolQuery));
        verify(searchRequestBuilder).setFetchSource(eq(true));
        assertThat(searchRequest.get().source().query(), is(boolQuery));
        InvalidateApiKeyResponse invalidateApiKeyResponse = invalidateApiKeyResponseListener.get();
        assertThat(invalidateApiKeyResponse.getInvalidatedApiKeys(), emptyIterable());
    }

    @SuppressWarnings("unchecked")
    public void testInvalidateApiKeysWillSetInvalidatedFlagAndRecordTimestamp() {
        final int docId = randomIntBetween(0, Integer.MAX_VALUE);
        final String apiKeyId = randomAlphaOfLength(20);

        // Mock the search request for keys to invalidate
        when(client.threadPool()).thenReturn(threadPool);
        when(client.prepareSearch(eq(SECURITY_MAIN_ALIAS))).thenReturn(new SearchRequestBuilder(client, SearchAction.INSTANCE));
        doAnswer(invocation -> {
            final var listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            final var searchHit = new SearchHit(docId, apiKeyId);
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.map(buildApiKeySourceDoc("some_hash".toCharArray()));
                searchHit.sourceRef(BytesReference.bytes(builder));
            }
            final var internalSearchResponse = new InternalSearchResponse(
                new SearchHits(
                    new SearchHit[] { searchHit },
                    new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                    randomFloat(),
                    null,
                    null,
                    null
                ),
                null,
                null,
                null,
                false,
                null,
                0
            );
            final var searchResponse = new SearchResponse(
                internalSearchResponse,
                randomAlphaOfLengthBetween(3, 8),
                1,
                1,
                0,
                10,
                null,
                null
            );
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), anyActionListener());

        // Capture the Update request so that we can verify it is configured as expected
        when(client.prepareBulk()).thenReturn(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        final var updateRequestBuilder = Mockito.spy(new UpdateRequestBuilder(client, UpdateAction.INSTANCE));
        when(client.prepareUpdate(eq(SECURITY_MAIN_ALIAS), eq(apiKeyId))).thenReturn(updateRequestBuilder);

        // Stub bulk and cache clearing calls so that the entire action flow can complete (not strictly necessary but nice to have)
        doAnswer(invocation -> {
            final var listener = (ActionListener<BulkResponse>) invocation.getArguments()[1];
            listener.onResponse(
                new BulkResponse(
                    new BulkItemResponse[] {
                        BulkItemResponse.success(
                            docId,
                            DocWriteRequest.OpType.UPDATE,
                            new UpdateResponse(
                                mock(ShardId.class),
                                apiKeyId,
                                randomLong(),
                                randomLong(),
                                randomLong(),
                                DocWriteResponse.Result.UPDATED
                            )
                        ) },
                    randomLongBetween(1, 100)
                )
            );
            return null;
        }).when(client).bulk(any(BulkRequest.class), anyActionListener());
        doAnswer(invocation -> {
            final var listener = (ActionListener<ClearSecurityCacheResponse>) invocation.getArguments()[2];
            listener.onResponse(mock(ClearSecurityCacheResponse.class));
            return null;
        }).when(client).execute(eq(ClearSecurityCacheAction.INSTANCE), any(ClearSecurityCacheRequest.class), anyActionListener());

        final long invalidationTime = randomMillisUpToYear9999();
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(invalidationTime));
        final ApiKeyService service = createApiKeyService();
        PlainActionFuture<InvalidateApiKeyResponse> future = new PlainActionFuture<>();
        service.invalidateApiKeys(null, null, null, new String[] { apiKeyId }, future);
        final InvalidateApiKeyResponse invalidateApiKeyResponse = future.actionGet();

        assertThat(invalidateApiKeyResponse.getInvalidatedApiKeys(), equalTo(List.of(apiKeyId)));
        verify(updateRequestBuilder).setDoc(
            argThat(
                (ArgumentMatcher<Map<String, Object>>) argument -> Map.of(
                    "api_key_invalidated",
                    true,
                    "invalidation_time",
                    invalidationTime
                ).equals(argument)
            )
        );
    }

    public void testCreateApiKeyWillCacheOnCreation() {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User(randomAlphaOfLengthBetween(8, 16), "superuser"))
            .realmRef(new RealmRef(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)))
            .build(false);
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(randomAlphaOfLengthBetween(3, 8), null, null);
        when(client.prepareIndex(anyString())).thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.prepareBulk()).thenReturn(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            @SuppressWarnings("unchecked")
            final ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) args[2];
            final IndexResponse indexResponse = new IndexResponse(
                new ShardId(INTERNAL_SECURITY_MAIN_INDEX_7, randomAlphaOfLength(22), randomIntBetween(0, 1)),
                createApiKeyRequest.getId(),
                randomLongBetween(1, 99),
                randomLongBetween(1, 99),
                randomIntBetween(1, 99),
                true
            );
            listener.onResponse(
                new BulkResponse(
                    new BulkItemResponse[] { BulkItemResponse.success(randomInt(), DocWriteRequest.OpType.INDEX, indexResponse) },
                    randomLongBetween(0, 100)
                )
            );
            return null;
        }).when(client).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any());

        final Cache<String, ListenableFuture<CachedApiKeyHashResult>> apiKeyAuthCache = service.getApiKeyAuthCache();
        assertNull(apiKeyAuthCache.get(createApiKeyRequest.getId()));
        final PlainActionFuture<CreateApiKeyResponse> listener = new PlainActionFuture<>();
        service.createApiKey(authentication, createApiKeyRequest, Set.of(), listener);
        final CreateApiKeyResponse createApiKeyResponse = listener.actionGet();
        assertThat(createApiKeyResponse.getId(), equalTo(createApiKeyRequest.getId()));
        final CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(createApiKeyResponse.getId());
        assertThat(cachedApiKeyHashResult.success, is(true));
        cachedApiKeyHashResult.verify(createApiKeyResponse.getKey());
    }

    public void testGetCredentialsFromThreadContext() {
        final ApiKeyService apiKeyService = createApiKeyService();
        ThreadContext threadContext = threadPool.getThreadContext();
        assertNull(apiKeyService.getCredentialsFromThreadContext(threadContext));

        final String apiKeyAuthScheme = randomFrom("apikey", "apiKey", "ApiKey", "APikey", "APIKEY");
        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);
        String headerValue = apiKeyAuthScheme + " " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            ApiKeyService.ApiKeyCredentials creds = apiKeyService.getCredentialsFromThreadContext(threadContext);
            assertNotNull(creds);
            assertEquals(id, creds.getId());
            assertEquals(key, creds.getKey().toString());
        }

        // missing space
        headerValue = apiKeyAuthScheme + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            ApiKeyService.ApiKeyCredentials creds = apiKeyService.getCredentialsFromThreadContext(threadContext);
            assertNull(creds);
        }

        // missing colon
        headerValue = apiKeyAuthScheme + " " + Base64.getEncoder().encodeToString((id + key).getBytes(StandardCharsets.UTF_8));
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> apiKeyService.getCredentialsFromThreadContext(threadContext)
            );
            assertEquals("invalid ApiKey value", e.getMessage());
        }
    }

    public void testAuthenticateWithApiKey() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);

        final User user, authUser;
        if (randomBoolean()) {
            user = new User("hulk", new String[] { "superuser" }, "Bruce Banner", "hulk@test.com", Map.of(), true);
            authUser = new User("authenticated_user", "other");
        } else {
            user = new User("hulk", new String[] { "superuser" }, "Bruce Banner", "hulk@test.com", Map.of(), true);
            authUser = null;
        }
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        final Map<String, Object> metadata = mockKeyDocument(id, key, user, authUser, false, Duration.ofSeconds(3600), null, type);

        final AuthenticationResult<User> auth = tryAuthenticate(service, id, key, type);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        assertThat(auth.getValue(), notNullValue());
        assertThat(auth.getValue().principal(), is("hulk"));
        assertThat(auth.getValue().fullName(), is("Bruce Banner"));
        assertThat(auth.getValue().email(), is("hulk@test.com"));
        assertThat(auth.getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_NAME), is("realm1"));
        assertThat(auth.getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_TYPE), is("native"));
        assertThat(auth.getMetadata().get(AuthenticationField.API_KEY_ID_KEY), is(id));
        assertThat(auth.getMetadata().get(AuthenticationField.API_KEY_NAME_KEY), is("test"));
        assertThat(auth.getMetadata().get(API_KEY_TYPE_KEY), is(type.value()));
        checkAuthApiKeyMetadata(metadata, auth);
    }

    public void testAuthenticationFailureWithInvalidatedApiKey() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());

        mockKeyDocument(id, key, new User("hulk", "superuser"), null, true, Duration.ofSeconds(3600), null, type);

        final AuthenticationResult<User> auth = tryAuthenticate(service, id, key, type);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(auth.getValue(), nullValue());
        assertThat(auth.getMessage(), containsString("invalidated"));
    }

    public void testAuthenticationFailureWithInvalidCredentials() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String realKey = randomAlphaOfLength(16);
        final String wrongKey = "#" + realKey.substring(1);

        final User user, authUser;
        if (randomBoolean()) {
            user = new User("hulk", "superuser");
            authUser = new User("authenticated_user", "other");
        } else {
            user = new User("hulk", "superuser");
            authUser = null;
        }
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        mockKeyDocument(id, realKey, user, authUser, false, Duration.ofSeconds(3600), null, type);

        final AuthenticationResult<User> auth = tryAuthenticate(service, id, wrongKey, type);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(auth.getValue(), nullValue());
        assertThat(auth.getMessage(), containsString("invalid credentials for API key [" + id + "]"));
    }

    public void testAuthenticationFailureWithExpiredKey() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);

        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        mockKeyDocument(id, key, new User("hulk", "superuser"), null, false, Duration.ofSeconds(-1), null, type);

        final AuthenticationResult<User> auth = tryAuthenticate(service, id, key, type);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(auth.getValue(), nullValue());
        assertThat(auth.getMessage(), containsString("expired"));
    }

    /**
     * We cache valid and invalid responses. This test verifies that we handle these correctly.
     */
    public void testMixingValidAndInvalidCredentials() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final String id = randomAlphaOfLength(12);
        final String realKey = randomAlphaOfLength(16);

        final User user, authUser;
        if (randomBoolean()) {
            user = new User("hulk", "superuser");
            authUser = new User("authenticated_user", "other");
        } else {
            user = new User("hulk", "superuser");
            authUser = null;
        }
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        final Map<String, Object> metadata = mockKeyDocument(id, realKey, user, authUser, false, Duration.ofSeconds(3600), null, type);

        for (int i = 0; i < 3; i++) {
            final String wrongKey = "=" + randomAlphaOfLength(14) + "@";
            AuthenticationResult<User> auth = tryAuthenticate(service, id, wrongKey, type);
            assertThat(auth.getStatus(), is(AuthenticationResult.Status.CONTINUE));
            assertThat(auth.getValue(), nullValue());
            assertThat(auth.getMessage(), containsString("invalid credentials for API key [" + id + "]"));

            auth = tryAuthenticate(service, id, realKey, type);
            assertThat(auth.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            assertThat(auth.getValue(), notNullValue());
            assertThat(auth.getValue().principal(), is("hulk"));
            assertThat(auth.getMetadata().get(API_KEY_TYPE_KEY), is(type.value()));
            checkAuthApiKeyMetadata(metadata, auth);
        }
    }

    public void testBulkUpdateWithApiKeyCredentialNotSupported() {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = createApiKeyService(settings);

        final PlainActionFuture<BulkUpdateApiKeyResponse> listener = new PlainActionFuture<>();
        service.updateApiKeys(
            AuthenticationTestHelper.builder().apiKey().build(false),
            BulkUpdateApiKeyRequest.usingApiKeyIds("id"),
            Set.of(),
            listener
        );

        final var ex = expectThrows(ExecutionException.class, listener::get);
        assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(ex.getMessage(), containsString("authentication via API key not supported: only the owner user can update an API key"));
    }

    public void testCrossClusterApiKeyUsageStats() {
        final Instant now = Instant.now();
        when(clock.instant()).thenReturn(now);
        when(client.threadPool()).thenReturn(threadPool);
        SearchRequestBuilder searchRequestBuilder = Mockito.spy(new SearchRequestBuilder(client, SearchAction.INSTANCE));
        when(client.prepareSearch(eq(SECURITY_MAIN_ALIAS))).thenReturn(searchRequestBuilder);

        final List<SearchHit> searchHits = new ArrayList<>();
        final int ccsKeys = randomIntBetween(0, 2);
        for (int i = 0; i < ccsKeys; i++) {
            searchHits.add(searchHitForCrossClusterApiKey(0));
        }
        final int ccrKeys = randomIntBetween(0, 2);
        for (int i = 0; i < ccrKeys; i++) {
            searchHits.add(searchHitForCrossClusterApiKey(1));
        }
        final int ccsCcrKeys = randomIntBetween(0, 2);
        for (int i = 0; i < ccsCcrKeys; i++) {
            searchHits.add(searchHitForCrossClusterApiKey(2));
        }

        final AtomicReference<SearchRequest> searchRequest = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            searchRequest.set(invocationOnMock.getArgument(0));
            final var searchResponse = new SearchResponse(
                new InternalSearchResponse(
                    new SearchHits(
                        searchHits.toArray(SearchHit[]::new),
                        new TotalHits(searchHits.size(), TotalHits.Relation.EQUAL_TO),
                        randomFloat(),
                        null,
                        null,
                        null
                    ),
                    null,
                    null,
                    null,
                    false,
                    null,
                    0
                ),
                randomAlphaOfLengthBetween(3, 8),
                1,
                1,
                0,
                10,
                null,
                null
            );
            final ActionListener<SearchResponse> listener = invocationOnMock.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), anyActionListener());

        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("doc_type", "api_key"))
            .filter(QueryBuilders.termQuery("type", ApiKey.Type.CROSS_CLUSTER.value()))
            .filter(QueryBuilders.termQuery("api_key_invalidated", false))
            .filter(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.rangeQuery("expiration_time").gt(now.toEpochMilli()))
                    .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("expiration_time")))
            );

        final ApiKeyService apiKeyService = createApiKeyService();
        final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        apiKeyService.crossClusterApiKeyUsageStats(future);

        verify(searchRequestBuilder).setQuery(eq(boolQuery));
        assertThat(searchRequest.get().source().query(), is(boolQuery));

        assertThat(
            future.actionGet(),
            equalTo(Map.of("total", ccsKeys + ccrKeys + ccsCcrKeys, "ccs", ccsKeys, "ccr", ccrKeys, "ccs_ccr", ccsCcrKeys))
        );
    }

    private SearchHit searchHitForCrossClusterApiKey(int crossClusterAccessLevel) {
        assert crossClusterAccessLevel >= 0 && crossClusterAccessLevel <= 2;
        final String roleDescriptor = switch (crossClusterAccessLevel) {
            case 0 -> """
                {
                  "cluster": ["cross_cluster_search"]
                }""";
            case 1 -> """
                {
                  "cluster": ["cross_cluster_replication"]
                }""";
            default -> """
                {
                  "cluster": ["cross_cluster_search", "cross_cluster_replication"]
                }""";
        };
        final int docId = randomIntBetween(0, Integer.MAX_VALUE);
        final String apiKeyId = randomAlphaOfLength(20);
        final var searchHit = new SearchHit(docId, apiKeyId);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.format("""
                {
                  "doc_type": "api_key",
                  "type": "cross_cluster",
                  "creation_time": 1591919944598,
                  "expiration_time": null,
                  "api_key_invalidated": false,
                  "api_key_hash": "{PBKDF2}10000$abc",
                  "role_descriptors": { "cross_cluster": %s },
                  "limited_by_role_descriptors": { },
                  "name": null,
                  "version": 8090099,
                  "creator": {
                    "principal": "admin",
                    "metadata": {},
                    "realm": "file1"
                  }
                }""", roleDescriptor), randomBoolean()));
            searchHit.sourceRef(BytesReference.bytes(builder));
            return searchHit;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testCrossClusterApiKeyUsageStatsAreZerosWhenServiceNotEnabled() {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), false).build();
        final ApiKeyService service = createApiKeyService(settings);
        final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        service.crossClusterApiKeyUsageStats(future);
        assertThat(future.actionGet(), anEmptyMap());
    }

    public void testCrossClusterApiKeyUsageStatsAreZerosWhenIndexDoesNotExist() {
        securityIndex = SecurityMocks.mockSecurityIndexManager(".security", false, false);
        final ApiKeyService apiKeyService = createApiKeyService();

        final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        apiKeyService.crossClusterApiKeyUsageStats(future);
        assertThat(future.actionGet(), equalTo(Map.of("total", 0, "ccs", 0, "ccr", 0, "ccs_ccr", 0)));
    }

    public void testCrossClusterApiKeyUsageFailsWhenIndexNotAvailable() {
        securityIndex = SecurityMocks.mockSecurityIndexManager(".security", true, false);
        final ElasticsearchException expectedException = new ElasticsearchException("not available");
        when(securityIndex.getUnavailableReason()).thenReturn(expectedException);
        final ApiKeyService apiKeyService = createApiKeyService();

        final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        apiKeyService.crossClusterApiKeyUsageStats(future);
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e, sameInstance(expectedException));
    }

    private Map<String, Object> mockKeyDocument(
        String id,
        String key,
        User user,
        @Nullable User authUser,
        boolean invalidated,
        Duration expiry,
        @Nullable List<RoleDescriptor> keyRoles,
        ApiKey.Type type
    ) throws IOException {
        final Authentication authentication;
        if (authUser != null) {
            authentication = AuthenticationTestHelper.builder()
                .user(authUser)
                .realmRef(new RealmRef("authRealm", "test", "foo"))
                .runAs()
                .user(user)
                .realmRef(new RealmRef("realm1", "native", "node01"))
                .build();
        } else {
            authentication = AuthenticationTestHelper.builder()
                .user(user)
                .realmRef(new RealmRef("realm1", "native", "node01"))
                .build(false);
        }
        final Map<String, Object> metadata = ApiKeyTests.randomMetadata();
        XContentBuilder docSource = ApiKeyService.newDocument(
            getFastStoredHashAlgoForTests().hash(new SecureString(key.toCharArray())),
            "test",
            authentication,
            type == ApiKey.Type.CROSS_CLUSTER ? Set.of() : Collections.singleton(SUPERUSER_ROLE_DESCRIPTOR),
            Instant.now(),
            Instant.now().plus(expiry),
            keyRoles,
            type,
            Version.CURRENT,
            metadata
        );
        if (invalidated) {
            Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(docSource), true, XContentType.JSON).v2();
            map.put("api_key_invalidated", true);
            docSource = XContentBuilder.builder(XContentType.JSON.xContent()).map(map);
        }
        SecurityMocks.mockGetRequest(client, id, BytesReference.bytes(docSource));
        return metadata;
    }

    private AuthenticationResult<User> tryAuthenticate(ApiKeyService service, String id, String key, ApiKey.Type type) throws Exception {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final String header = "ApiKey " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
            threadContext.putHeader("Authorization", header);

            final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            service.tryAuthenticate(threadContext, getApiKeyCredentials(id, key, type), future);

            final AuthenticationResult<User> auth = future.get();
            assertThat(auth, notNullValue());
            return auth;
        }
    }

    public void testValidateApiKey() throws Exception {
        final String apiKeyId = randomAlphaOfLength(12);
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        ApiKeyDoc apiKeyDoc = buildApiKeyDoc(hash, -1, false);

        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(
            apiKeyId,
            apiKeyDoc,
            getApiKeyCredentials(apiKeyId, apiKey, apiKeyDoc.type),
            Clock.systemUTC(),
            future
        );
        AuthenticationResult<User> result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getValue().principal(), is("test_user"));
        assertThat(result.getValue().fullName(), is("test user"));
        assertThat(result.getValue().email(), is("test@user.com"));
        assertThat(result.getValue().roles(), is(emptyArray()));
        assertThat(result.getValue().metadata(), is(Collections.emptyMap()));
        assertThat(result.getMetadata().get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY), equalTo(apiKeyDoc.roleDescriptorsBytes));
        assertThat(
            result.getMetadata().get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
            equalTo(apiKeyDoc.limitedByRoleDescriptorsBytes)
        );
        assertThat(result.getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_NAME), is("realm1"));
        assertThat(result.getMetadata().get(API_KEY_TYPE_KEY), is(apiKeyDoc.type.value()));

        apiKeyDoc = buildApiKeyDoc(hash, Clock.systemUTC().instant().plus(1L, ChronoUnit.HOURS).toEpochMilli(), false);
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(
            apiKeyId,
            apiKeyDoc,
            getApiKeyCredentials(apiKeyId, apiKey, apiKeyDoc.type),
            Clock.systemUTC(),
            future
        );
        result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getValue().principal(), is("test_user"));
        assertThat(result.getValue().fullName(), is("test user"));
        assertThat(result.getValue().email(), is("test@user.com"));
        assertThat(result.getValue().roles(), is(emptyArray()));
        assertThat(result.getValue().metadata(), is(Collections.emptyMap()));
        assertThat(result.getMetadata().get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY), equalTo(apiKeyDoc.roleDescriptorsBytes));
        assertThat(
            result.getMetadata().get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
            equalTo(apiKeyDoc.limitedByRoleDescriptorsBytes)
        );
        assertThat(result.getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_NAME), is("realm1"));
        assertThat(result.getMetadata().get(API_KEY_TYPE_KEY), is(apiKeyDoc.type.value()));

        apiKeyDoc = buildApiKeyDoc(hash, Clock.systemUTC().instant().minus(1L, ChronoUnit.HOURS).toEpochMilli(), false);
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(
            apiKeyId,
            apiKeyDoc,
            getApiKeyCredentials(apiKeyId, apiKey, apiKeyDoc.type),
            Clock.systemUTC(),
            future
        );
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());

        // key is invalidated
        apiKeyDoc = buildApiKeyDoc(hash, -1, true);
        service.getApiKeyAuthCache().put(apiKeyId, new ListenableFuture<>());
        assertNotNull(service.getApiKeyAuthCache().get(apiKeyId));
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(
            apiKeyId,
            apiKeyDoc,
            getApiKeyCredentials(apiKeyId, randomAlphaOfLength(15), apiKeyDoc.type),
            Clock.systemUTC(),
            future
        );
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());
        // make sure the cache is cleared
        assertNull(service.getApiKeyAuthCache().get(apiKeyId));
    }

    @SuppressWarnings("unchecked")
    public void testParseRoleDescriptorsMap() throws Exception {
        final String apiKeyId = randomAlphaOfLength(12);

        final NativePrivilegeStore privilegesStore = mock(NativePrivilegeStore.class);
        doAnswer(i -> {
            assertThat(i.getArguments().length, equalTo(3));
            final Object arg2 = i.getArguments()[2];
            assertThat(arg2, instanceOf(ActionListener.class));
            ActionListener<Collection<ApplicationPrivilege>> listener = (ActionListener<Collection<ApplicationPrivilege>>) arg2;
            listener.onResponse(Collections.emptyList());
            return null;
        }).when(privilegesStore).getPrivileges(any(Collection.class), any(Collection.class), anyActionListener());
        ApiKeyService service = createApiKeyService(Settings.EMPTY);

        assertThat(service.parseRoleDescriptors(apiKeyId, null, randomApiKeyRoleType()), nullValue());
        assertThat(service.parseRoleDescriptors(apiKeyId, Collections.emptyMap(), randomApiKeyRoleType()), emptyIterable());

        final RoleDescriptor roleARoleDescriptor = new RoleDescriptor(
            "a role",
            new String[] { "monitor" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("monitor").build() },
            null
        );
        Map<String, Object> roleARDMap;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            roleARDMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                BytesReference.bytes(roleARoleDescriptor.toXContent(builder, ToXContent.EMPTY_PARAMS, true)).streamInput(),
                false
            );
        }

        List<RoleDescriptor> roleDescriptors = service.parseRoleDescriptors(apiKeyId, Map.of("a role", roleARDMap), randomApiKeyRoleType());
        assertThat(roleDescriptors, hasSize(1));
        assertThat(roleDescriptors.get(0), equalTo(roleARoleDescriptor));

        Map<String, Object> superUserRdMap;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            superUserRdMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                BytesReference.bytes(SUPERUSER_ROLE_DESCRIPTOR.toXContent(builder, ToXContent.EMPTY_PARAMS, true)).streamInput(),
                false
            );
        }
        roleDescriptors = service.parseRoleDescriptors(
            apiKeyId,
            Map.of(SUPERUSER_ROLE_DESCRIPTOR.getName(), superUserRdMap),
            randomApiKeyRoleType()
        );
        assertThat(roleDescriptors, hasSize(1));
        assertThat(roleDescriptors.get(0), equalTo(SUPERUSER_ROLE_DESCRIPTOR));

        final Map<String, Object> legacySuperUserRdMap;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            legacySuperUserRdMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                BytesReference.bytes(LEGACY_SUPERUSER_ROLE_DESCRIPTOR.toXContent(builder, ToXContent.EMPTY_PARAMS, true)).streamInput(),
                false
            );
        }
        final RoleReference.ApiKeyRoleType apiKeyRoleType = randomApiKeyRoleType();
        roleDescriptors = service.parseRoleDescriptors(
            apiKeyId,
            Map.of(LEGACY_SUPERUSER_ROLE_DESCRIPTOR.getName(), legacySuperUserRdMap),
            apiKeyRoleType
        );
        assertThat(roleDescriptors, hasSize(1));
        assertThat(
            roleDescriptors.get(0),
            equalTo(
                apiKeyRoleType == RoleReference.ApiKeyRoleType.LIMITED_BY ? SUPERUSER_ROLE_DESCRIPTOR : LEGACY_SUPERUSER_ROLE_DESCRIPTOR
            )
        );
    }

    public void testParseRoleDescriptors() {
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final String apiKeyId = randomAlphaOfLength(12);
        List<RoleDescriptor> roleDescriptors = service.parseRoleDescriptorsBytes(apiKeyId, null, randomApiKeyRoleType());
        assertTrue(roleDescriptors.isEmpty());

        BytesReference roleBytes = new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}");
        roleDescriptors = service.parseRoleDescriptorsBytes(apiKeyId, roleBytes, randomApiKeyRoleType());
        assertEquals(1, roleDescriptors.size());
        assertEquals("a role", roleDescriptors.get(0).getName());
        assertArrayEquals(new String[] { "all" }, roleDescriptors.get(0).getClusterPrivileges());
        assertEquals(0, roleDescriptors.get(0).getIndicesPrivileges().length);
        assertEquals(0, roleDescriptors.get(0).getApplicationPrivileges().length);

        roleBytes = new BytesArray(
            "{\"reporting_user\":{\"cluster\":[],\"indices\":[],\"applications\":[],\"run_as\":[],\"metadata\":{\"_reserved\":true},"
                + "\"transient_metadata\":{\"enabled\":true}},\"superuser\":{\"cluster\":[\"all\"],\"indices\":[{\"names\":[\"*\"],"
                + "\"privileges\":[\"all\"],\"allow_restricted_indices\":true}],\"applications\":[{\"application\":\"*\","
                + "\"privileges\":[\"*\"],\"resources\":[\"*\"]}],\"run_as\":[\"*\"],\"metadata\":{\"_reserved\":true},"
                + "\"transient_metadata\":{}}}\n"
        );
        final RoleReference.ApiKeyRoleType apiKeyRoleType = randomApiKeyRoleType();
        roleDescriptors = service.parseRoleDescriptorsBytes(apiKeyId, roleBytes, apiKeyRoleType);
        assertEquals(2, roleDescriptors.size());
        assertEquals(
            Set.of("reporting_user", "superuser"),
            roleDescriptors.stream().map(RoleDescriptor::getName).collect(Collectors.toSet())
        );
        assertThat(
            roleDescriptors.get(1),
            equalTo(
                apiKeyRoleType == RoleReference.ApiKeyRoleType.LIMITED_BY ? SUPERUSER_ROLE_DESCRIPTOR : LEGACY_SUPERUSER_ROLE_DESCRIPTOR
            )
        );

        // Tests parsing of role descriptor with and without workflows restriction.
        roleBytes = new BytesArray("""
                {
                    "role_with_restriction":{
                        "indices":[{"names":["books"],"privileges":["read"]}],
                        "restriction":{"workflows":["search_application"]}
                    },
                    "role_without_restriction":{
                        "indices":[{"names":["movies"],"privileges":["read"]}]
                    }
                }
            """);
        roleDescriptors = service.parseRoleDescriptorsBytes(apiKeyId, roleBytes, apiKeyRoleType);
        assertEquals(2, roleDescriptors.size());
        Map<String, RoleDescriptor> roleDescriptorsByName = roleDescriptors.stream()
            .collect(Collectors.toMap(RoleDescriptor::getName, Function.identity()));
        assertEquals(Set.of("role_with_restriction", "role_without_restriction"), roleDescriptorsByName.keySet());

        RoleDescriptor roleWithRestriction = roleDescriptorsByName.get("role_with_restriction");
        assertThat(roleWithRestriction.hasRestriction(), equalTo(true));
        assertThat(roleWithRestriction.getRestriction().isEmpty(), equalTo(false));
        assertThat(roleWithRestriction.getRestriction().hasWorkflows(), equalTo(true));
        assertThat(roleWithRestriction.getRestriction().getWorkflows(), arrayContaining("search_application"));

        RoleDescriptor roleWithoutRestriction = roleDescriptorsByName.get("role_without_restriction");
        assertThat(roleWithoutRestriction.hasRestriction(), equalTo(false));
        assertThat(roleWithoutRestriction.getRestriction().isEmpty(), equalTo(true));
        assertThat(roleWithoutRestriction.getRestriction().hasWorkflows(), equalTo(false));
        assertThat(roleWithoutRestriction.getRestriction().getWorkflows(), nullValue());
    }

    public void testApiKeyServiceDisabled() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), false).build();
        final ApiKeyService service = createApiKeyService(settings);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> service.getApiKeys(
                new String[] { randomAlphaOfLength(6) },
                randomAlphaOfLength(8),
                null,
                null,
                randomBoolean(),
                new PlainActionFuture<>()
            )
        );

        assertThat(e, instanceOf(FeatureNotEnabledException.class));
        // Older Kibana version looked for this exact text:
        assertThat(e, throwableWithMessage("api keys are not enabled"));
        // Newer Kibana versions will check the metadata for this string literal:
        assertThat(e.getMetadata(FeatureNotEnabledException.DISABLED_FEATURE_METADATA), contains("api_keys"));
    }

    public void testApiKeyCache() throws IOException {
        final String apiKeyId = randomAlphaOfLength(12);
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        ApiKeyDoc apiKeyDoc = buildApiKeyDoc(hash, -1, false);

        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        ApiKeyCredentials creds = getApiKeyCredentials(apiKeyId, apiKey, apiKeyDoc.type);
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        AuthenticationResult<User> result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNotNull(cachedApiKeyHashResult);
        assertThat(cachedApiKeyHashResult.success, is(true));

        creds = getApiKeyCredentials(creds.getId(), "somelongenoughrandomstring", apiKeyDoc.type);
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        final CachedApiKeyHashResult shouldBeSame = service.getFromCache(creds.getId());
        assertNotNull(shouldBeSame);
        assertThat(shouldBeSame, sameInstance(cachedApiKeyHashResult));

        apiKeyDoc = buildApiKeyDoc(hasher.hash(new SecureString("somelongenoughrandomstring".toCharArray())), -1, false);
        creds = getApiKeyCredentials(randomAlphaOfLength(12), "otherlongenoughrandomstring", apiKeyDoc.type);
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNotNull(cachedApiKeyHashResult);
        assertThat(cachedApiKeyHashResult.success, is(false));

        creds = getApiKeyCredentials(creds.getId(), "otherlongenoughrandomstring2", apiKeyDoc.type);
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(false));
        assertThat(service.getFromCache(creds.getId()), not(sameInstance(cachedApiKeyHashResult)));
        assertThat(service.getFromCache(creds.getId()).success, is(false));

        creds = getApiKeyCredentials(creds.getId(), "somelongenoughrandomstring", apiKeyDoc.type);
        future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        assertThat(service.getFromCache(creds.getId()), not(sameInstance(cachedApiKeyHashResult)));
        assertThat(service.getFromCache(creds.getId()).success, is(true));
    }

    public void testApiKeyAuthCacheWillTraceLogOnEvictionDueToCacheSize() throws IllegalAccessException {
        final int cacheSize = randomIntBetween(2, 8);
        ApiKeyService service = createApiKeyService(
            Settings.builder().put("xpack.security.authc.api_key.cache.max_keys", cacheSize).build()
        );
        final Cache<String, ListenableFuture<CachedApiKeyHashResult>> apiKeyAuthCache = service.getApiKeyAuthCache();

        // Fill the cache
        final String idPrefix = randomAlphaOfLength(20);
        final AtomicInteger count = new AtomicInteger(0);
        IntStream.range(0, cacheSize).forEach(i -> apiKeyAuthCache.put(idPrefix + count.incrementAndGet(), new ListenableFuture<>()));
        final Logger logger = LogManager.getLogger(ApiKeyService.class);
        Loggers.setLevel(logger, Level.TRACE);
        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(logger, appender);
        appender.start();

        try {
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "evict",
                    ApiKeyService.class.getName(),
                    Level.TRACE,
                    "API key with ID \\[" + idPrefix + "[0-9]+\\] was evicted from the authentication cache.*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no-thrashing",
                    ApiKeyService.class.getName(),
                    Level.WARN,
                    "Possible thrashing for API key authentication cache,*"
                )
            );
            apiKeyAuthCache.put(idPrefix + count.incrementAndGet(), new ListenableFuture<>());
            appender.assertAllExpectationsMatched();

            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "replace",
                    ApiKeyService.class.getName(),
                    Level.TRACE,
                    "API key with ID [" + idPrefix + "*] was evicted from the authentication cache*"
                )
            );
            apiKeyAuthCache.put(idPrefix + count.get(), new ListenableFuture<>());
            appender.assertAllExpectationsMatched();

            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "invalidate",
                    ApiKeyService.class.getName(),
                    Level.TRACE,
                    "API key with ID [" + idPrefix + "*] was evicted from the authentication cache*"
                )
            );
            apiKeyAuthCache.invalidate(idPrefix + count.get(), new ListenableFuture<>());
            apiKeyAuthCache.invalidateAll();
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.setLevel(logger, Level.INFO);
            Loggers.removeAppender(logger, appender);
        }
    }

    public void testApiKeyCacheWillNotTraceLogOnEvictionDueToCacheTtl() throws IllegalAccessException, InterruptedException {
        ApiKeyService service = createApiKeyService(
            Settings.builder()
                .put("xpack.security.authc.api_key.cache.max_keys", 2)
                .put("xpack.security.authc.api_key.cache.ttl", TimeValue.timeValueMillis(100))
                .build()
        );
        final Cache<String, ListenableFuture<CachedApiKeyHashResult>> apiKeyAuthCache = service.getApiKeyAuthCache();
        final String apiKeyId = randomAlphaOfLength(22);

        final Logger logger = LogManager.getLogger(ApiKeyService.class);
        Loggers.setLevel(logger, Level.TRACE);
        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(logger, appender);
        appender.start();

        try {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "evict",
                    ApiKeyService.class.getName(),
                    Level.TRACE,
                    "API key with ID [" + apiKeyId + "] was evicted from the authentication cache*"
                )
            );
            apiKeyAuthCache.put(apiKeyId, new ListenableFuture<>());
            // Wait for the entry to expire
            Thread.sleep(200);
            assertNull(apiKeyAuthCache.get(apiKeyId));
            // Cache a new entry
            apiKeyAuthCache.put(randomValueOtherThan(apiKeyId, () -> randomAlphaOfLength(22)), new ListenableFuture<>());
            assertEquals(1, apiKeyAuthCache.count());
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.setLevel(logger, Level.INFO);
            Loggers.removeAppender(logger, appender);
        }
    }

    public void testApiKeyAuthCacheWillLogWarningOnPossibleThrashing() throws Exception {
        ApiKeyService service = createApiKeyService(Settings.builder().put("xpack.security.authc.api_key.cache.max_keys", 2).build());
        final Cache<String, ListenableFuture<CachedApiKeyHashResult>> apiKeyAuthCache = service.getApiKeyAuthCache();

        // Fill the cache
        apiKeyAuthCache.put(randomAlphaOfLength(20), new ListenableFuture<>());
        apiKeyAuthCache.put(randomAlphaOfLength(21), new ListenableFuture<>());
        final Logger logger = LogManager.getLogger(ApiKeyService.class);
        Loggers.setLevel(logger, Level.TRACE);
        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(logger, appender);
        appender.start();

        try {
            // Prepare the warning logging to trigger
            service.getEvictionCounter().add(4500);
            final long thrashingCheckIntervalInSeconds = 300L;
            final long secondsToNanoSeconds = 1_000_000_000L;
            // Calculate the last thrashing check time to ensure that the elapsed time is longer than the
            // thrashing checking interval (300 seconds). Also add another 10 seconds to counter any
            // test flakiness.
            final long lastCheckedAt = System.nanoTime() - (thrashingCheckIntervalInSeconds + 10L) * secondsToNanoSeconds;
            service.getLastEvictionCheckedAt().set(lastCheckedAt);
            // Ensure the counter is updated
            assertBusy(() -> assertThat(service.getEvictionCounter().longValue() >= 4500, is(true)));
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "evict",
                    ApiKeyService.class.getName(),
                    Level.TRACE,
                    "API key with ID [*] was evicted from the authentication cache*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "thrashing",
                    ApiKeyService.class.getName(),
                    Level.WARN,
                    "Possible thrashing for API key authentication cache,*"
                )
            );
            apiKeyAuthCache.put(randomAlphaOfLength(22), new ListenableFuture<>());
            appender.assertAllExpectationsMatched();

            // Counter and timer should be reset
            assertThat(service.getLastEvictionCheckedAt().get(), lessThanOrEqualTo(System.nanoTime()));
            assertBusy(() -> assertThat(service.getEvictionCounter().longValue(), equalTo(0L)));

            // Will not log warning again for the next eviction because of throttling
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "evict-again",
                    ApiKeyService.class.getName(),
                    Level.TRACE,
                    "API key with ID [*] was evicted from the authentication cache*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "throttling",
                    ApiKeyService.class.getName(),
                    Level.WARN,
                    "Possible thrashing for API key authentication cache,*"
                )
            );
            apiKeyAuthCache.put(randomAlphaOfLength(23), new ListenableFuture<>());
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.setLevel(logger, Level.INFO);
            Loggers.removeAppender(logger, appender);
        }
    }

    public void testAuthenticateWhileCacheBeingPopulated() throws Exception {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        Map<String, Object> sourceMap = buildApiKeySourceDoc(hash);
        final Object metadata = sourceMap.get("metadata_flattened");
        final ApiKey.Type type = parseTypeFromSourceMap(sourceMap);

        ApiKeyService realService = createApiKeyService(Settings.EMPTY);
        ApiKeyService service = Mockito.spy(realService);

        // Used to block the hashing of the first api-key secret so that we can guarantee
        // that a second api key authentication takes place while hashing is "in progress".
        final Semaphore hashWait = new Semaphore(0);
        final AtomicInteger hashCounter = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            hashCounter.incrementAndGet();
            hashWait.acquire();
            return invocationOnMock.callRealMethod();
        }).when(service).verifyKeyAgainstHash(any(String.class), any(ApiKeyCredentials.class), anyActionListener());

        final String apiKeyId = randomAlphaOfLength(12);
        final PlainActionFuture<AuthenticationResult<User>> future1 = new PlainActionFuture<>();

        // Call the top level authenticate... method because it has been known to be buggy in async situations
        mockSourceDocument(apiKeyId, sourceMap);

        // This needs to be done in another thread, because we need it to not complete until we say so, but it should not block this test
        this.threadPool.generic()
            .execute(() -> service.tryAuthenticate(threadPool.getThreadContext(), getApiKeyCredentials(apiKeyId, apiKey, type), future1));

        // Wait for the first credential validation to get to the blocked state
        assertBusy(() -> assertThat(hashCounter.get(), equalTo(1)));
        if (future1.isDone()) {
            // We do this [ rather than assertFalse(isDone) ] so we can get a reasonable failure message
            fail("Expected authentication to be blocked, but was " + future1.actionGet());
        }

        // The second authentication should pass (but not immediately, but will not block)
        PlainActionFuture<AuthenticationResult<User>> future2 = new PlainActionFuture<>();

        service.tryAuthenticate(threadPool.getThreadContext(), getApiKeyCredentials(apiKeyId, apiKey, type), future2);

        assertThat(hashCounter.get(), equalTo(1));
        if (future2.isDone()) {
            // We do this [ rather than assertFalse(isDone) ] so we can get a reasonable failure message
            fail("Expected authentication to be blocked, but was " + future2.actionGet());
        }

        hashWait.release();

        final AuthenticationResult<User> authResult1 = future1.actionGet(TimeValue.timeValueSeconds(2));
        assertThat(authResult1.isAuthenticated(), is(true));
        checkAuthApiKeyMetadata(metadata, authResult1);

        final AuthenticationResult<User> authResult2 = future2.actionGet(TimeValue.timeValueMillis(200));
        assertThat(authResult2.isAuthenticated(), is(true));
        checkAuthApiKeyMetadata(metadata, authResult2);

        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(apiKeyId);
        assertNotNull(cachedApiKeyHashResult);
        assertThat(cachedApiKeyHashResult.success, is(true));
    }

    public void testApiKeyCacheDisabled() throws IOException {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));
        final Settings settings = Settings.builder().put(ApiKeyService.CACHE_TTL_SETTING.getKey(), "0s").build();

        ApiKeyDoc apiKeyDoc = buildApiKeyDoc(hash, -1, false);

        ApiKeyService service = createApiKeyService(settings);
        ApiKeyCredentials creds = getApiKeyCredentials(randomAlphaOfLength(12), apiKey, apiKeyDoc.type);
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        AuthenticationResult<User> result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNull(cachedApiKeyHashResult);
        assertNull(service.getDocCache());
        assertNull(service.getRoleDescriptorsBytesCache());
    }

    public void testApiKeyDocCacheCanBeDisabledSeparately() throws IOException {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));
        final Settings settings = Settings.builder().put(ApiKeyService.DOC_CACHE_TTL_SETTING.getKey(), "0s").build();

        ApiKeyDoc apiKeyDoc = buildApiKeyDoc(hash, -1, false);

        ApiKeyService service = createApiKeyService(settings);

        ApiKeyCredentials creds = getApiKeyCredentials(randomAlphaOfLength(12), apiKey, apiKeyDoc.type);
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.validateApiKeyCredentials(creds.getId(), apiKeyDoc, creds, Clock.systemUTC(), future);
        AuthenticationResult<User> result = future.actionGet();
        assertThat(result.isAuthenticated(), is(true));
        CachedApiKeyHashResult cachedApiKeyHashResult = service.getFromCache(creds.getId());
        assertNotNull(cachedApiKeyHashResult);
        assertNull(service.getDocCache());
        assertNull(service.getRoleDescriptorsBytesCache());
    }

    public void testApiKeyDocCache() throws IOException, ExecutionException, InterruptedException {
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        assertNotNull(service.getDocCache());
        assertNotNull(service.getRoleDescriptorsBytesCache());
        final ThreadContext threadContext = threadPool.getThreadContext();
        final ApiKey.Type type = ApiKey.Type.REST;

        // 1. A new API key document will be cached after its authentication
        final String docId = randomAlphaOfLength(16);
        final String apiKey = randomAlphaOfLength(16);

        ApiKeyCredentials apiKeyCredentials = getApiKeyCredentials(docId, apiKey, type);
        final Map<String, Object> metadata = mockKeyDocument(
            docId,
            apiKey,
            new User("hulk", "superuser"),
            null,
            false,
            Duration.ofSeconds(3600),
            null,
            type
        );
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.loadApiKeyAndValidateCredentials(threadContext, apiKeyCredentials, future);
        final ApiKeyService.CachedApiKeyDoc cachedApiKeyDoc = service.getDocCache().get(docId);
        assertNotNull(cachedApiKeyDoc);
        assertEquals("hulk", cachedApiKeyDoc.creator.get("principal"));
        final BytesReference roleDescriptorsBytes = service.getRoleDescriptorsBytesCache().get(cachedApiKeyDoc.roleDescriptorsHash);
        assertNotNull(roleDescriptorsBytes);
        assertEquals("{}", roleDescriptorsBytes.utf8ToString());
        final BytesReference limitedByRoleDescriptorsBytes = service.getRoleDescriptorsBytesCache()
            .get(cachedApiKeyDoc.limitedByRoleDescriptorsHash);
        assertNotNull(limitedByRoleDescriptorsBytes);
        final List<RoleDescriptor> limitedByRoleDescriptors = service.parseRoleDescriptorsBytes(
            docId,
            limitedByRoleDescriptorsBytes,
            RoleReference.ApiKeyRoleType.LIMITED_BY
        );
        assertEquals(1, limitedByRoleDescriptors.size());
        assertEquals(SUPERUSER_ROLE_DESCRIPTOR, limitedByRoleDescriptors.get(0));
        if (metadata == null) {
            assertNull(cachedApiKeyDoc.metadataFlattened);
        } else {
            assertThat(cachedApiKeyDoc.metadataFlattened, equalTo(XContentTestUtils.convertToXContent(metadata, XContentType.JSON)));
        }
        assertThat(cachedApiKeyDoc.type, is(type));

        // 2. A different API Key with the same role descriptors will share the entries in the role descriptor cache
        final String docId2 = randomAlphaOfLength(16);
        final String apiKey2 = randomAlphaOfLength(16);
        ApiKeyCredentials apiKeyCredentials2 = getApiKeyCredentials(docId2, apiKey2, type);
        final Map<String, Object> metadata2 = mockKeyDocument(
            docId2,
            apiKey2,
            new User("thor", "superuser"),
            null,
            false,
            Duration.ofSeconds(3600),
            null,
            type
        );
        PlainActionFuture<AuthenticationResult<User>> future2 = new PlainActionFuture<>();
        service.loadApiKeyAndValidateCredentials(threadContext, apiKeyCredentials2, future2);
        final ApiKeyService.CachedApiKeyDoc cachedApiKeyDoc2 = service.getDocCache().get(docId2);
        assertNotNull(cachedApiKeyDoc2);
        assertEquals("thor", cachedApiKeyDoc2.creator.get("principal"));
        final BytesReference roleDescriptorsBytes2 = service.getRoleDescriptorsBytesCache().get(cachedApiKeyDoc2.roleDescriptorsHash);
        assertSame(roleDescriptorsBytes, roleDescriptorsBytes2);
        final BytesReference limitedByRoleDescriptorsBytes2 = service.getRoleDescriptorsBytesCache()
            .get(cachedApiKeyDoc2.limitedByRoleDescriptorsHash);
        assertSame(limitedByRoleDescriptorsBytes, limitedByRoleDescriptorsBytes2);
        if (metadata2 == null) {
            assertNull(cachedApiKeyDoc2.metadataFlattened);
        } else {
            assertThat(cachedApiKeyDoc2.metadataFlattened, equalTo(XContentTestUtils.convertToXContent(metadata2, XContentType.JSON)));
        }
        assertThat(cachedApiKeyDoc2.type, is(type));

        // 3. Different role descriptors will be cached into a separate entry
        final String docId3 = randomAlphaOfLength(16);
        final String apiKey3 = randomAlphaOfLength(16);
        ApiKeyCredentials apiKeyCredentials3 = getApiKeyCredentials(docId3, apiKey3, type);
        final List<RoleDescriptor> keyRoles = List.of(
            RoleDescriptor.parse("key-role", new BytesArray("{\"cluster\":[\"monitor\"]}"), true, XContentType.JSON)
        );
        final Map<String, Object> metadata3 = mockKeyDocument(
            docId3,
            apiKey3,
            new User("banner", "superuser"),
            null,
            false,
            Duration.ofSeconds(3600),
            keyRoles,
            type
        );
        PlainActionFuture<AuthenticationResult<User>> future3 = new PlainActionFuture<>();
        service.loadApiKeyAndValidateCredentials(threadContext, apiKeyCredentials3, future3);
        final ApiKeyService.CachedApiKeyDoc cachedApiKeyDoc3 = service.getDocCache().get(docId3);
        assertNotNull(cachedApiKeyDoc3);
        assertEquals("banner", cachedApiKeyDoc3.creator.get("principal"));
        // Shared bytes for limitedBy role since it is the same
        assertSame(
            limitedByRoleDescriptorsBytes,
            service.getRoleDescriptorsBytesCache().get(cachedApiKeyDoc3.limitedByRoleDescriptorsHash)
        );
        // But role descriptors bytes are different
        final BytesReference roleDescriptorsBytes3 = service.getRoleDescriptorsBytesCache().get(cachedApiKeyDoc3.roleDescriptorsHash);
        assertNotSame(roleDescriptorsBytes, roleDescriptorsBytes3);
        assertEquals(3, service.getRoleDescriptorsBytesCache().count());
        if (metadata3 == null) {
            assertNull(cachedApiKeyDoc3.metadataFlattened);
        } else {
            assertThat(cachedApiKeyDoc3.metadataFlattened, equalTo(XContentTestUtils.convertToXContent(metadata3, XContentType.JSON)));
        }
        assertThat(cachedApiKeyDoc3.type, is(type));

        // 3.1. Cross-cluster API keys can share the cache entry
        final String docId31 = randomAlphaOfLength(16);
        final String apiKey31 = randomAlphaOfLength(16);
        ApiKeyCredentials apiKeyCredentials31 = getApiKeyCredentials(docId31, apiKey31, ApiKey.Type.CROSS_CLUSTER);
        final Map<String, Object> metadata31 = mockKeyDocument(
            docId31,
            apiKey31,
            new User("stark", "superuser"),
            null,
            false,
            Duration.ofSeconds(3600),
            keyRoles,
            ApiKey.Type.CROSS_CLUSTER
        );
        service.loadApiKeyAndValidateCredentials(threadContext, apiKeyCredentials31, new PlainActionFuture<>());
        final ApiKeyService.CachedApiKeyDoc cachedApiKeyDoc31 = service.getDocCache().get(docId31);
        assertNotNull(cachedApiKeyDoc31);
        assertEquals("stark", cachedApiKeyDoc31.creator.get("principal"));
        // Both role descriptor and limited-by role descriptor share cache entries.
        assertEquals(3, service.getRoleDescriptorsBytesCache().count());
        // Cross cluster API keys have empty limited-by
        assertThat(
            service.getRoleDescriptorsBytesCache().get(cachedApiKeyDoc31.limitedByRoleDescriptorsHash).utf8ToString(),
            equalTo("{}")
        );
        if (metadata31 == null) {
            assertNull(cachedApiKeyDoc31.metadataFlattened);
        } else {
            assertThat(cachedApiKeyDoc31.metadataFlattened, equalTo(XContentTestUtils.convertToXContent(metadata31, XContentType.JSON)));
        }
        assertThat(service.getRoleDescriptorsBytesCache().get(cachedApiKeyDoc31.roleDescriptorsHash), sameInstance(roleDescriptorsBytes3));
        assertThat(cachedApiKeyDoc31.type, is(ApiKey.Type.CROSS_CLUSTER));

        // 4. Will fetch document from security index if role descriptors are not found even when
        // cachedApiKeyDoc is available
        service.getRoleDescriptorsBytesCache().invalidateAll();
        Mockito.clearInvocations(client);
        final Map<String, Object> metadata4 = mockKeyDocument(
            docId,
            apiKey,
            new User("hulk", "superuser"),
            null,
            false,
            Duration.ofSeconds(3600),
            null,
            type
        );
        PlainActionFuture<AuthenticationResult<User>> future4 = new PlainActionFuture<>();
        service.loadApiKeyAndValidateCredentials(threadContext, getApiKeyCredentials(docId, apiKey, type), future4);
        verify(client, times(1)).get(any(GetRequest.class), anyActionListener());
        assertEquals(2, service.getRoleDescriptorsBytesCache().count());
        final AuthenticationResult<User> authResult4 = future4.get();
        assertSame(AuthenticationResult.Status.SUCCESS, authResult4.getStatus());
        assertThat(authResult4.getMetadata().get(API_KEY_TYPE_KEY), is(type.value()));
        checkAuthApiKeyMetadata(metadata4, authResult4);

        // 5. Cached entries will be used for the same API key doc
        SecurityMocks.mockGetRequestException(client, new EsRejectedExecutionException("rejected"));
        PlainActionFuture<AuthenticationResult<User>> future5 = new PlainActionFuture<>();
        service.loadApiKeyAndValidateCredentials(threadContext, getApiKeyCredentials(docId, apiKey, type), future5);
        final AuthenticationResult<User> authResult5 = future5.get();
        assertSame(AuthenticationResult.Status.SUCCESS, authResult5.getStatus());
        assertThat(authResult5.getMetadata().get(API_KEY_TYPE_KEY), is(type.value()));
        checkAuthApiKeyMetadata(metadata4, authResult5);
    }

    public void testWillInvalidateAuthCacheWhenDocNotFound() {
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final ThreadContext threadContext = threadPool.getThreadContext();
        final String docId = randomAlphaOfLength(16);
        final String apiKey = randomAlphaOfLength(16);
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        ApiKeyCredentials apiKeyCredentials = getApiKeyCredentials(docId, apiKey, type);
        service.getApiKeyAuthCache().put(docId, new ListenableFuture<>());
        assertNotNull(service.getApiKeyAuthCache().get(docId));
        SecurityMocks.mockGetRequest(
            client,
            SECURITY_MAIN_ALIAS,
            docId,
            new GetResult(
                INTERNAL_SECURITY_MAIN_INDEX_7,
                docId,
                UNASSIGNED_SEQ_NO,
                UNASSIGNED_PRIMARY_TERM,
                randomLongBetween(0, 9),
                false,
                null,
                null,
                null
            )
        );
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.loadApiKeyAndValidateCredentials(threadContext, apiKeyCredentials, future);
        assertNull(service.getApiKeyAuthCache().get(docId));
    }

    public void testGetCreatorRealm() {
        final User user = AuthenticationTests.randomUser();

        // API key authentication
        final String apiKeyId = randomAlphaOfLength(20);
        final Authentication authentication1 = AuthenticationTests.randomApiKeyAuthentication(user, apiKeyId);
        assertThat(ApiKeyService.getCreatorRealmName(authentication1), equalTo(AuthenticationField.API_KEY_CREATOR_REALM_NAME));
        assertThat(ApiKeyService.getCreatorRealmType(authentication1), equalTo(AuthenticationField.API_KEY_CREATOR_REALM_TYPE));

        // API key run-as
        final RealmRef lookupRealmRef = AuthenticationTests.randomRealmRef(false);
        final Authentication authentication2 = authentication1.runAs(AuthenticationTests.randomUser(), lookupRealmRef);
        assertThat(ApiKeyService.getCreatorRealmName(authentication2), equalTo(lookupRealmRef.getName()));
        assertThat(ApiKeyService.getCreatorRealmType(authentication2), equalTo(lookupRealmRef.getType()));

        // Realm
        final Authentication authentication3 = AuthenticationTests.randomRealmAuthentication(randomBoolean());
        assertThat(ApiKeyService.getCreatorRealmName(authentication3), equalTo(authentication3.getEffectiveSubject().getRealm().getName()));
        assertThat(ApiKeyService.getCreatorRealmType(authentication3), equalTo(authentication3.getEffectiveSubject().getRealm().getType()));

        // Realm run-as
        final Authentication authentication4 = authentication3.runAs(AuthenticationTests.randomUser(), lookupRealmRef);
        assertThat(ApiKeyService.getCreatorRealmName(authentication4), equalTo(lookupRealmRef.getName()));
        assertThat(ApiKeyService.getCreatorRealmType(authentication4), equalTo(lookupRealmRef.getType()));

        // Others (cannot run-as)
        final Authentication authentication5 = randomFrom(
            AuthenticationTests.randomServiceAccountAuthentication(),
            AuthenticationTests.randomAnonymousAuthentication(),
            AuthenticationTests.randomInternalAuthentication()
        );
        assertThat(ApiKeyService.getCreatorRealmName(authentication5), equalTo(authentication5.getEffectiveSubject().getRealm().getName()));
        assertThat(ApiKeyService.getCreatorRealmType(authentication5), equalTo(authentication5.getEffectiveSubject().getRealm().getType()));

        // Failed run-as returns authenticating subject's realm
        final Authentication authentication6 = authentication3.runAs(AuthenticationTests.randomUser(), null);
        assertThat(
            ApiKeyService.getCreatorRealmName(authentication6),
            equalTo(authentication6.getAuthenticatingSubject().getRealm().getName())
        );
        assertThat(
            ApiKeyService.getCreatorRealmType(authentication6),
            equalTo(authentication6.getAuthenticatingSubject().getRealm().getType())
        );
    }

    public void testGetOwnersRealmNames() {
        // realm, no domain
        RealmRef realmRef = AuthenticationTests.randomRealmRef(false);
        Authentication authentication = Authentication.newRealmAuthentication(AuthenticationTests.randomUser(), realmRef);
        assertThat(Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication)), contains(realmRef.getName()));
        assertThat(Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.token())), contains(realmRef.getName()));
        // realm run-as, no domain
        authentication = Authentication.newRealmAuthentication(
            AuthenticationTests.randomUser(),
            AuthenticationTests.randomRealmRef(randomBoolean())
        );
        assertThat(
            Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.runAs(AuthenticationTests.randomUser(), realmRef))),
            contains(realmRef.getName())
        );
        assertThat(
            Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.runAs(AuthenticationTests.randomUser(), realmRef).token())),
            contains(realmRef.getName())
        );
        // realm under domain
        realmRef = AuthenticationTests.randomRealmRef(true);
        authentication = Authentication.newRealmAuthentication(AuthenticationTests.randomUser(), realmRef);
        assertThat(
            Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication)),
            containsInAnyOrder(
                realmRef.getDomain().realms().stream().map(realmIdentifier -> realmIdentifier.getName()).toArray(String[]::new)
            )
        );
        assertThat(
            Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.token())),
            containsInAnyOrder(
                realmRef.getDomain().realms().stream().map(realmIdentifier -> realmIdentifier.getName()).toArray(String[]::new)
            )
        );
        // realm under domain run-as
        authentication = Authentication.newRealmAuthentication(
            AuthenticationTests.randomUser(),
            AuthenticationTests.randomRealmRef(randomBoolean())
        );
        assertThat(
            Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.runAs(AuthenticationTests.randomUser(), realmRef))),
            containsInAnyOrder(
                realmRef.getDomain().realms().stream().map(realmIdentifier -> realmIdentifier.getName()).toArray(String[]::new)
            )
        );
        assertThat(
            Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.runAs(AuthenticationTests.randomUser(), realmRef).token())),
            containsInAnyOrder(
                realmRef.getDomain().realms().stream().map(realmIdentifier -> realmIdentifier.getName()).toArray(String[]::new)
            )
        );
        // API key authentication
        final String apiKeyCreatorRealm = randomAlphaOfLengthBetween(2, 8);
        authentication = AuthenticationTests.randomApiKeyAuthentication(
            AuthenticationTests.randomUser(),
            randomAlphaOfLength(8),
            apiKeyCreatorRealm,
            "file",
            TransportVersion.current()
        );
        assertThat(Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication)), contains(apiKeyCreatorRealm));
        assertThat(Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.token())), contains(apiKeyCreatorRealm));

        // API key run-as, no domain
        RealmRef lookupRealmRef = AuthenticationTests.randomRealmRef(false);
        assertThat(
            Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.runAs(AuthenticationTests.randomUser(), lookupRealmRef))),
            contains(lookupRealmRef.getName())
        );
        assertThat(
            Arrays.asList(
                ApiKeyService.getOwnersRealmNames(authentication.runAs(AuthenticationTests.randomUser(), lookupRealmRef).token())
            ),
            contains(lookupRealmRef.getName())
        );

        // API key run-as under domain
        lookupRealmRef = AuthenticationTests.randomRealmRef(true);
        assertThat(
            Arrays.asList(ApiKeyService.getOwnersRealmNames(authentication.runAs(AuthenticationTests.randomUser(), lookupRealmRef))),
            containsInAnyOrder(
                lookupRealmRef.getDomain().realms().stream().map(realmIdentifier -> realmIdentifier.getName()).toArray(String[]::new)
            )
        );
        assertThat(
            Arrays.asList(
                ApiKeyService.getOwnersRealmNames(authentication.runAs(AuthenticationTests.randomUser(), lookupRealmRef).token())
            ),
            containsInAnyOrder(
                lookupRealmRef.getDomain().realms().stream().map(realmIdentifier -> realmIdentifier.getName()).toArray(String[]::new)
            )
        );
    }

    public void testAuthWillTerminateIfGetThreadPoolIsSaturated() throws ExecutionException, InterruptedException {
        final String apiKey = randomAlphaOfLength(16);
        final ApiKeyCredentials creds = getApiKeyCredentials(randomAlphaOfLength(12), apiKey, randomFrom(ApiKey.Type.values()));
        SecurityMocks.mockGetRequestException(client, new EsRejectedExecutionException("rejected"));
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.tryAuthenticate(threadPool.getThreadContext(), creds, future);
        final AuthenticationResult<User> authenticationResult = future.get();
        assertEquals(AuthenticationResult.Status.TERMINATE, authenticationResult.getStatus());
        assertThat(authenticationResult.getMessage(), containsString("server is too busy to respond"));
    }

    public void testAuthWillTerminateIfHashingThreadPoolIsSaturated() throws IOException, ExecutionException, InterruptedException {
        final String apiKey = randomAlphaOfLength(16);

        Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));
        Map<String, Object> sourceMap = buildApiKeySourceDoc(hash);
        final ApiKey.Type type = parseTypeFromSourceMap(sourceMap);
        final ApiKeyCredentials creds = getApiKeyCredentials(randomAlphaOfLength(12), apiKey, type);
        mockSourceDocument(creds.getId(), sourceMap);
        final ExecutorService mockExecutorService = mock(ExecutorService.class);
        when(threadPool.executor(SECURITY_CRYPTO_THREAD_POOL_NAME)).thenReturn(mockExecutorService);
        Mockito.doAnswer(invocationOnMock -> {
            final AbstractRunnable actionRunnable = (AbstractRunnable) invocationOnMock.getArguments()[0];
            actionRunnable.onRejection(new EsRejectedExecutionException("rejected"));
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.tryAuthenticate(threadPool.getThreadContext(), creds, future);
        final AuthenticationResult<User> authenticationResult = future.get();
        assertEquals(AuthenticationResult.Status.TERMINATE, authenticationResult.getStatus());
        assertThat(authenticationResult.getMessage(), containsString("server is too busy to respond"));
    }

    public void testCreationWillFailIfHashingThreadPoolIsSaturated() {
        final EsRejectedExecutionException rejectedExecutionException = new EsRejectedExecutionException("rejected");
        final ExecutorService mockExecutorService = mock(ExecutorService.class);
        when(threadPool.executor(SECURITY_CRYPTO_THREAD_POOL_NAME)).thenReturn(mockExecutorService);
        Mockito.doAnswer(invocationOnMock -> {
            final AbstractRunnable actionRunnable = (AbstractRunnable) invocationOnMock.getArguments()[0];
            actionRunnable.onRejection(rejectedExecutionException);
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        final Authentication authentication = AuthenticationTestHelper.builder().build();
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(randomAlphaOfLengthBetween(3, 8), null, null);
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        service.createApiKey(authentication, createApiKeyRequest, Set.of(), future);
        final EsRejectedExecutionException e = expectThrows(EsRejectedExecutionException.class, future::actionGet);
        assertThat(e, is(rejectedExecutionException));
    }

    public void testCachedApiKeyValidationWillNotBeBlockedByUnCachedApiKey() throws IOException, ExecutionException, InterruptedException {
        final String apiKeyId1 = randomAlphaOfLength(12);
        final String apiKey1 = randomAlphaOfLength(16);

        Hasher hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey1.toCharArray()));
        Map<String, Object> sourceMap = buildApiKeySourceDoc(hash);
        final Object metadata = sourceMap.get("metadata_flattened");
        mockSourceDocument(apiKeyId1, sourceMap);

        // Authenticate the key once to cache it
        ApiKeyService service = createApiKeyService(Settings.EMPTY);
        final ApiKeyCredentials creds = getApiKeyCredentials(apiKeyId1, apiKey1, parseTypeFromSourceMap(sourceMap));
        final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        service.tryAuthenticate(threadPool.getThreadContext(), creds, future);
        final AuthenticationResult<User> authenticationResult = future.get();
        assertEquals(AuthenticationResult.Status.SUCCESS, authenticationResult.getStatus());
        checkAuthApiKeyMetadata(metadata, authenticationResult);

        // Now force the hashing thread pool to saturate so that any un-cached keys cannot be validated
        final ExecutorService mockExecutorService = mock(ExecutorService.class);
        when(threadPool.executor(SECURITY_CRYPTO_THREAD_POOL_NAME)).thenReturn(mockExecutorService);
        Mockito.doAnswer(invocationOnMock -> {
            final AbstractRunnable actionRunnable = (AbstractRunnable) invocationOnMock.getArguments()[0];
            actionRunnable.onRejection(new EsRejectedExecutionException("rejected"));
            return null;
        }).when(mockExecutorService).execute(any(Runnable.class));

        // A new API key trying to connect that must go through full hash computation
        final String apiKeyId2 = randomAlphaOfLength(12);
        final String apiKey2 = randomAlphaOfLength(16);
        final Map<String, Object> sourceMap2 = buildApiKeySourceDoc(hasher.hash(new SecureString(apiKey2.toCharArray())));
        mockSourceDocument(apiKeyId2, sourceMap2);
        final ApiKeyCredentials creds2 = getApiKeyCredentials(apiKeyId2, apiKey2, parseTypeFromSourceMap(sourceMap2));
        final PlainActionFuture<AuthenticationResult<User>> future2 = new PlainActionFuture<>();
        service.tryAuthenticate(threadPool.getThreadContext(), creds2, future2);
        final AuthenticationResult<User> authenticationResult2 = future2.get();
        assertEquals(AuthenticationResult.Status.TERMINATE, authenticationResult2.getStatus());
        assertThat(authenticationResult2.getMessage(), containsString("server is too busy to respond"));

        // The cached API key should not be affected
        mockSourceDocument(apiKeyId1, sourceMap);
        final PlainActionFuture<AuthenticationResult<User>> future3 = new PlainActionFuture<>();
        service.tryAuthenticate(
            threadPool.getThreadContext(),
            getApiKeyCredentials(apiKeyId1, apiKey1, parseTypeFromSourceMap(sourceMap)),
            future3
        );
        final AuthenticationResult<User> authenticationResult3 = future3.get();
        assertEquals(AuthenticationResult.Status.SUCCESS, authenticationResult3.getStatus());
        checkAuthApiKeyMetadata(metadata, authenticationResult3);
    }

    @SuppressWarnings("unchecked")
    public void testApiKeyDocDeserialization() throws IOException {
        final String apiKeyDocumentSource = """
            {
              "doc_type": "api_key",
              "creation_time": 1591919944598,
              "expiration_time": 1591919944599,
              "api_key_invalidated": false,
              "api_key_hash": "{PBKDF2}10000$abc",
              "role_descriptors": {
                "a": {
                  "cluster": [ "all" ]
                }
              },
              "limited_by_role_descriptors": {
                "limited_by": {
                  "cluster": [ "all" ],
                  "metadata": {
                    "_reserved": true
                  },
                  "type": "role"
                }
              },
              "name": "key-1",
              "version": 7000099,
              "creator": {
                "principal": "admin",
                "metadata": {
                  "foo": "bar"
                },
                "realm": "file1",
                "realm_type": "file"
              }
            }""";
        final ApiKeyDoc apiKeyDoc = ApiKeyDoc.fromXContent(
            XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                new BytesArray(apiKeyDocumentSource),
                XContentType.JSON
            )
        );
        assertEquals("api_key", apiKeyDoc.docType);
        assertEquals(1591919944598L, apiKeyDoc.creationTime);
        assertEquals(1591919944599L, apiKeyDoc.expirationTime);
        assertFalse(apiKeyDoc.invalidated);
        assertEquals("{PBKDF2}10000$abc", apiKeyDoc.hash);
        assertEquals("key-1", apiKeyDoc.name);
        assertEquals(7000099, apiKeyDoc.version);
        assertEquals(new BytesArray("""
            {"a":{"cluster":["all"]}}"""), apiKeyDoc.roleDescriptorsBytes);
        assertEquals(new BytesArray("""
            {"limited_by":{"cluster":["all"],"metadata":{"_reserved":true},"type":"role"}}"""), apiKeyDoc.limitedByRoleDescriptorsBytes);

        final Map<String, Object> creator = apiKeyDoc.creator;
        assertEquals("admin", creator.get("principal"));
        assertEquals("file1", creator.get("realm"));
        assertEquals("file", creator.get("realm_type"));
        assertEquals("bar", ((Map<String, Object>) creator.get("metadata")).get("foo"));
    }

    public void testValidateApiKeyDocBeforeUpdate() throws IOException {
        final var apiKeyId = randomAlphaOfLength(12);
        final var apiKey = randomAlphaOfLength(16);
        final var hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        final var apiKeyService = createApiKeyService();
        final var apiKeyDocWithNullName = buildApiKeyDoc(hash, -1, false, null, Version.V_8_2_0.id);
        final var auth = Authentication.newRealmAuthentication(
            new User("test_user", "role"),
            new Authentication.RealmRef("realm1", "realm_type1", "node")
        );

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> apiKeyService.validateForUpdate(apiKeyId, apiKeyDocWithNullName.type, auth, apiKeyDocWithNullName)
        );
        assertThat(ex.getMessage(), containsString("cannot update legacy API key [" + apiKeyId + "] without name"));

        final var apiKeyDocWithEmptyName = buildApiKeyDoc(hash, -1, false, "", Version.V_8_2_0.id);
        ex = expectThrows(
            IllegalArgumentException.class,
            () -> apiKeyService.validateForUpdate(apiKeyId, apiKeyDocWithEmptyName.type, auth, apiKeyDocWithEmptyName)
        );
        assertThat(ex.getMessage(), containsString("cannot update legacy API key [" + apiKeyId + "] without name"));

        final ApiKeyDoc apiKeyDoc = buildApiKeyDoc(hash, -1, false, randomAlphaOfLengthBetween(3, 8), Version.CURRENT.id);
        final ApiKey.Type expectedType = randomValueOtherThan(apiKeyDoc.type, () -> randomFrom(ApiKey.Type.values()));
        ex = expectThrows(IllegalArgumentException.class, () -> apiKeyService.validateForUpdate(apiKeyId, expectedType, auth, apiKeyDoc));
        assertThat(
            ex.getMessage(),
            containsString(
                "cannot update API key of type [" + apiKeyDoc.type.value() + "] while expected type is [" + expectedType.value() + "]"
            )
        );
    }

    public void testMaybeBuildUpdatedDocument() throws IOException {
        final var apiKey = randomAlphaOfLength(16);
        final var hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));
        final var oldAuthentication = randomValueOtherThanMany(
            Authentication::isApiKey,
            () -> AuthenticationTestHelper.builder()
                .user(AuthenticationTestHelper.userWithRandomMetadataAndDetails("user", "role"))
                .build(false)
        );
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        final Set<RoleDescriptor> oldUserRoles = type == ApiKey.Type.CROSS_CLUSTER
            ? Set.of()
            : randomSet(0, 3, RoleDescriptorTests::randomRoleDescriptor);
        final List<RoleDescriptor> oldKeyRoles;
        if (type == ApiKey.Type.CROSS_CLUSTER) {
            oldKeyRoles = List.of(CrossClusterApiKeyRoleDescriptorBuilder.parse(randomCrossClusterApiKeyAccessField()).build());
        } else {
            oldKeyRoles = randomList(3, RoleDescriptorTests::randomRoleDescriptor);
        }
        final Map<String, Object> oldMetadata = ApiKeyTests.randomMetadata();
        final Version oldVersion = VersionUtils.randomVersion(random());
        final ApiKeyDoc oldApiKeyDoc = ApiKeyDoc.fromXContent(
            XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                BytesReference.bytes(
                    ApiKeyService.newDocument(
                        hash,
                        randomAlphaOfLength(10),
                        oldAuthentication,
                        oldUserRoles,
                        Instant.now(),
                        randomBoolean() ? null : Instant.now(),
                        oldKeyRoles,
                        type,
                        oldVersion,
                        oldMetadata
                    )
                ),
                XContentType.JSON
            )
        );

        final boolean changeUserRoles = type != ApiKey.Type.CROSS_CLUSTER && randomBoolean();
        final boolean changeKeyRoles = randomBoolean();
        final boolean changeMetadata = randomBoolean();
        final boolean changeVersion = randomBoolean();
        final boolean changeCreator = randomBoolean();
        final Set<RoleDescriptor> newUserRoles = changeUserRoles
            ? randomValueOtherThan(oldUserRoles, () -> randomSet(0, 3, RoleDescriptorTests::randomRoleDescriptor))
            : oldUserRoles;
        final List<RoleDescriptor> newKeyRoles;
        if (changeKeyRoles) {
            if (type == ApiKey.Type.CROSS_CLUSTER) {
                newKeyRoles = randomValueOtherThan(oldKeyRoles, () -> {
                    try {
                        return List.of(CrossClusterApiKeyRoleDescriptorBuilder.parse(randomCrossClusterApiKeyAccessField()).build());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            } else {
                newKeyRoles = randomValueOtherThan(oldKeyRoles, () -> randomList(0, 3, RoleDescriptorTests::randomRoleDescriptor));
            }
        } else {
            newKeyRoles = randomBoolean() ? oldKeyRoles : null;
        }
        final Map<String, Object> newMetadata = changeMetadata
            ? randomValueOtherThanMany(md -> md == null || md.equals(oldMetadata), ApiKeyTests::randomMetadata)
            : (randomBoolean() ? oldMetadata : null);
        final Version newVersion = changeVersion
            ? randomValueOtherThan(oldVersion, () -> VersionUtils.randomVersion(random()))
            : oldVersion;
        final Authentication newAuthentication = changeCreator
            ? randomValueOtherThanMany(
                (auth -> auth.isApiKey() || auth.getEffectiveSubject().getUser().equals(oldAuthentication.getEffectiveSubject().getUser())),
                () -> AuthenticationTestHelper.builder()
                    .user(AuthenticationTestHelper.userWithRandomMetadataAndDetails("user", "role"))
                    .build(false)
            )
            : oldAuthentication;
        final String apiKeyId = randomAlphaOfLength(10);
        final BaseUpdateApiKeyRequest request = mock(BaseUpdateApiKeyRequest.class);
        when(request.getType()).thenReturn(type);
        when(request.getRoleDescriptors()).thenReturn(newKeyRoles);
        when(request.getMetadata()).thenReturn(newMetadata);
        final var service = createApiKeyService();

        final XContentBuilder builder = service.maybeBuildUpdatedDocument(
            apiKeyId,
            oldApiKeyDoc,
            newVersion,
            newAuthentication,
            request,
            newUserRoles
        );

        final boolean noop = (changeCreator || changeMetadata || changeKeyRoles || changeUserRoles || changeVersion) == false;
        if (noop) {
            assertNull(builder);
        } else {
            final ApiKeyDoc updatedApiKeyDoc = ApiKeyDoc.fromXContent(
                XContentHelper.createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder), XContentType.JSON)
            );
            assertEquals(oldApiKeyDoc.docType, updatedApiKeyDoc.docType);
            assertEquals(oldApiKeyDoc.type, updatedApiKeyDoc.type);
            assertEquals(oldApiKeyDoc.name, updatedApiKeyDoc.name);
            assertEquals(oldApiKeyDoc.hash, updatedApiKeyDoc.hash);
            assertEquals(oldApiKeyDoc.expirationTime, updatedApiKeyDoc.expirationTime);
            assertEquals(oldApiKeyDoc.creationTime, updatedApiKeyDoc.creationTime);
            assertEquals(oldApiKeyDoc.invalidated, updatedApiKeyDoc.invalidated);
            assertEquals(newVersion.id, updatedApiKeyDoc.version);
            final var actualUserRoles = service.parseRoleDescriptorsBytes(
                "",
                updatedApiKeyDoc.limitedByRoleDescriptorsBytes,
                RoleReference.ApiKeyRoleType.LIMITED_BY
            );
            assertEquals(newUserRoles.size(), actualUserRoles.size());
            assertEquals(new HashSet<>(newUserRoles), new HashSet<>(actualUserRoles));
            final var actualKeyRoles = service.parseRoleDescriptorsBytes(
                "",
                updatedApiKeyDoc.roleDescriptorsBytes,
                RoleReference.ApiKeyRoleType.ASSIGNED
            );
            if (changeKeyRoles == false) {
                assertEquals(
                    service.parseRoleDescriptorsBytes("", oldApiKeyDoc.roleDescriptorsBytes, RoleReference.ApiKeyRoleType.ASSIGNED),
                    actualKeyRoles
                );
            } else {
                assertEquals(newKeyRoles.size(), actualKeyRoles.size());
                assertEquals(new HashSet<>(newKeyRoles), new HashSet<>(actualKeyRoles));
            }
            if (changeMetadata == false) {
                assertEquals(oldApiKeyDoc.metadataFlattened, updatedApiKeyDoc.metadataFlattened);
            } else {
                assertEquals(newMetadata, XContentHelper.convertToMap(updatedApiKeyDoc.metadataFlattened, true, XContentType.JSON).v2());
            }
            assertEquals(newAuthentication.getEffectiveSubject().getUser().principal(), updatedApiKeyDoc.creator.get("principal"));
            assertEquals(newAuthentication.getEffectiveSubject().getUser().fullName(), updatedApiKeyDoc.creator.get("full_name"));
            assertEquals(newAuthentication.getEffectiveSubject().getUser().email(), updatedApiKeyDoc.creator.get("email"));
            assertEquals(newAuthentication.getEffectiveSubject().getUser().metadata(), updatedApiKeyDoc.creator.get("metadata"));
            final RealmRef realm = newAuthentication.getEffectiveSubject().getRealm();
            assertEquals(realm.getName(), updatedApiKeyDoc.creator.get("realm"));
            assertEquals(realm.getType(), updatedApiKeyDoc.creator.get("realm_type"));
            if (realm.getDomain() != null) {
                @SuppressWarnings("unchecked")
                final var actualRealmDomain = RealmDomain.fromXContent(
                    XContentHelper.mapToXContentParser(
                        XContentParserConfiguration.EMPTY,
                        (Map<String, Object>) updatedApiKeyDoc.creator.get("realm_domain")
                    )
                );
                assertEquals(realm.getDomain(), actualRealmDomain);
            } else {
                assertFalse(updatedApiKeyDoc.creator.containsKey("realm_domain"));
            }
        }
    }

    public void testApiKeyDocDeserializationWithNullValues() throws IOException {
        final String apiKeyDocumentSource = """
            {
              "doc_type": "api_key",
              "creation_time": 1591919944598,
              "expiration_time": null,
              "api_key_invalidated": false,
              "api_key_hash": "{PBKDF2}10000$abc",
              "role_descriptors": {},
              "limited_by_role_descriptors": {
                "limited_by": {
                  "cluster": [ "all" ]
                }
              },
              "name": null,
              "version": 7000099,
              "creator": {
                "principal": "admin",
                "metadata": {},
                "realm": "file1"
              }
            }""";
        final ApiKeyDoc apiKeyDoc = ApiKeyDoc.fromXContent(
            XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                new BytesArray(apiKeyDocumentSource),
                XContentType.JSON
            )
        );
        assertEquals(-1L, apiKeyDoc.expirationTime);
        assertNull(apiKeyDoc.name);
        assertEquals(new BytesArray("{}"), apiKeyDoc.roleDescriptorsBytes);
    }

    public void testGetApiKeyMetadata() throws IOException {
        final Map<String, Object> metadata;
        final Map<String, Object> apiKeyMetadata = ApiKeyTests.randomMetadata();
        if (apiKeyMetadata == null) {
            metadata = Map.of(API_KEY_ID_KEY, randomAlphaOfLength(20));
        } else {
            final BytesReference metadataBytes = XContentTestUtils.convertToXContent(apiKeyMetadata, XContentType.JSON);
            metadata = Map.of(API_KEY_ID_KEY, randomAlphaOfLength(20), API_KEY_METADATA_KEY, metadataBytes);
        }

        final Authentication apiKeyAuthentication = Authentication.newApiKeyAuthentication(
            AuthenticationResult.success(new User(ESTestCase.randomAlphaOfLengthBetween(3, 8)), metadata),
            randomAlphaOfLengthBetween(3, 8)
        );

        final Map<String, Object> restoredApiKeyMetadata = ApiKeyService.getApiKeyMetadata(apiKeyAuthentication);
        if (apiKeyMetadata == null) {
            assertThat(restoredApiKeyMetadata, anEmptyMap());
        } else {
            assertThat(restoredApiKeyMetadata, equalTo(apiKeyMetadata));
        }

        final Authentication authentication = AuthenticationTests.randomAuthentication(
            AuthenticationTests.randomUser(),
            AuthenticationTests.randomRealmRef(randomBoolean()),
            false
        );
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ApiKeyService.getApiKeyMetadata(authentication)
        );
        assertThat(e.getMessage(), containsString("authentication realm must be [_es_api_key]"));
    }

    public void testMaybeRemoveRemoteIndicesPrivilegesWithUnsupportedVersion() {
        final String apiKeyId = randomAlphaOfLengthBetween(5, 8);
        final Set<RoleDescriptor> userRoleDescriptors = Set.copyOf(
            randomList(2, 5, () -> RoleDescriptorTests.randomRoleDescriptor(randomBoolean(), randomBoolean(), randomBoolean()))
        );

        // Selecting random unsupported version.
        final TransportVersion minTransportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.MINIMUM_COMPATIBLE,
            TransportVersionUtils.getPreviousVersion(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS)
        );

        final Set<RoleDescriptor> result = ApiKeyService.maybeRemoveRemoteIndicesPrivileges(
            userRoleDescriptors,
            minTransportVersion,
            apiKeyId
        );
        assertThat(result.stream().anyMatch(RoleDescriptor::hasRemoteIndicesPrivileges), equalTo(false));
        assertThat(result.size(), equalTo(userRoleDescriptors.size()));

        // Roles for which warning headers are added.
        final List<String> userRoleNamesWithRemoteIndicesPrivileges = userRoleDescriptors.stream()
            .filter(RoleDescriptor::hasRemoteIndicesPrivileges)
            .map(RoleDescriptor::getName)
            .sorted()
            .toList();

        if (false == userRoleNamesWithRemoteIndicesPrivileges.isEmpty()) {
            assertWarnings(
                "Removed API key's remote indices privileges from role(s) "
                    + userRoleNamesWithRemoteIndicesPrivileges
                    + ". Remote indices are not supported by all nodes in the cluster. "
                    + "Use the update API Key API to re-assign remote indices to the API key(s), after the cluster upgrade is complete."
            );
        }
    }

    public void testMaybeRemoveRemoteIndicesPrivilegesWithSupportedVersion() {
        final String apiKeyId = randomAlphaOfLengthBetween(5, 8);
        final Set<RoleDescriptor> userRoleDescriptors = Set.copyOf(
            randomList(1, 3, ApiKeyServiceTests::randomRoleDescriptorWithRemoteIndexPrivileges)
        );

        // Selecting random supported version.
        final TransportVersion minTransportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS,
            TransportVersion.current()
        );

        final Set<RoleDescriptor> result = ApiKeyService.maybeRemoveRemoteIndicesPrivileges(
            userRoleDescriptors,
            minTransportVersion,
            apiKeyId
        );

        // User roles should be unchanged.
        assertThat(result, equalTo(userRoleDescriptors));
    }

    public void testBuildDelimitedStringWithLimit() {
        int limit = 2;
        assertThat(ApiKeyService.buildDelimitedStringWithLimit(limit), equalTo(""));
        assertThat(ApiKeyService.buildDelimitedStringWithLimit(limit, new String[] {}), equalTo(""));
        assertThat(ApiKeyService.buildDelimitedStringWithLimit(limit, "id-1"), equalTo("id-1"));
        assertThat(ApiKeyService.buildDelimitedStringWithLimit(limit, "id-1", "id-2"), equalTo("id-1, id-2"));
        assertThat(
            ApiKeyService.buildDelimitedStringWithLimit(limit, "id-1", "id-2", "id-3"),
            equalTo("id-1, id-2... (3 in total, 1 omitted)")
        );
        assertThat(
            ApiKeyService.buildDelimitedStringWithLimit(limit, "id-1", "id-2", "id-3", "id-4"),
            equalTo("id-1, id-2... (4 in total, 2 omitted)")
        );

        var e = expectThrows(
            IllegalArgumentException.class,
            () -> ApiKeyService.buildDelimitedStringWithLimit(randomIntBetween(-5, 0), "not-relevant-for-this-test")
        );
        assertThat(e.getMessage(), equalTo("limit must be positive number"));
    }

    public void testCreateCrossClusterApiKeyMinVersionConstraint() {
        final Authentication authentication = randomValueOtherThanMany(
            Authentication::isApiKey,
            () -> AuthenticationTestHelper.builder().build()
        );
        final AbstractCreateApiKeyRequest request = mock(AbstractCreateApiKeyRequest.class);
        when(request.getType()).thenReturn(ApiKey.Type.CROSS_CLUSTER);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD))
        );
        final ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        final TransportVersion minTransportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.MINIMUM_COMPATIBLE,
            TransportVersionUtils.getPreviousVersion(TransportVersion.V_8_9_0)
        );
        when(clusterState.getMinTransportVersion()).thenReturn(minTransportVersion);

        final ApiKeyService service = new ApiKeyService(
            Settings.EMPTY,
            clock,
            client,
            securityIndex,
            clusterService,
            cacheInvalidatorRegistry,
            threadPool
        );

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        service.createApiKey(authentication, request, Set.of(), future);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);

        assertThat(
            e.getMessage(),
            containsString("all nodes must have transport version [8090099] or higher to support creating cross cluster API keys")
        );
    }

    public void testAuthenticationFailureWithApiKeyTypeMismatch() throws Exception {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        final ApiKeyService service = spy(createApiKeyService(settings));

        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        mockKeyDocument(id, key, new User("hulk", "superuser"), null, false, Duration.ofSeconds(3600), null, type);

        final ApiKey.Type expectedType = randomValueOtherThan(type, () -> randomFrom(ApiKey.Type.values()));
        final AuthenticationResult<User> auth = tryAuthenticate(service, id, key, expectedType);
        assertThat(auth.getStatus(), is(AuthenticationResult.Status.TERMINATE));
        assertThat(auth.getValue(), nullValue());
        assertThat(
            auth.getMessage(),
            containsString(
                "authentication expected API key type of ["
                    + expectedType.value()
                    + "], but API key ["
                    + id
                    + "] has type ["
                    + type.value()
                    + "]"
            )
        );

        // API key type mismatch should be checked after API key secret is verified
        verify(service).verifyKeyAgainstHash(any(), any(), anyActionListener());
        assertThat(service.getDocCache().keys(), contains(id));
        assertThat(service.getApiKeyAuthCache().keys(), contains(id));
    }

    public void testValidateApiKeyTypeAndExpiration() throws IOException {
        final var apiKeyId = randomAlphaOfLength(12);
        final var apiKey = randomAlphaOfLength(16);
        final var hasher = getFastStoredHashAlgoForTests();
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        final long futureTime = Instant.now().plus(7, ChronoUnit.DAYS).toEpochMilli();
        final long pastTime = Instant.now().plus(-7, ChronoUnit.DAYS).toEpochMilli();

        // Wrong API key type
        final var apiKeyDoc1 = buildApiKeyDoc(
            hash,
            randomFrom(-1L, futureTime),
            false,
            randomAlphaOfLengthBetween(3, 8),
            Version.CURRENT.id
        );
        final ApiKey.Type expectedType1 = randomValueOtherThan(apiKeyDoc1.type, () -> randomFrom(ApiKey.Type.values()));
        final ApiKeyCredentials apiKeyCredentials1 = getApiKeyCredentials(apiKeyId, apiKey, expectedType1);
        final PlainActionFuture<AuthenticationResult<User>> future1 = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyTypeAndExpiration(apiKeyDoc1, apiKeyCredentials1, clock, future1);
        final AuthenticationResult<User> auth1 = future1.actionGet();
        assertThat(auth1.getStatus(), is(AuthenticationResult.Status.TERMINATE));
        assertThat(auth1.getValue(), nullValue());
        assertThat(
            auth1.getMessage(),
            containsString(
                "authentication expected API key type of ["
                    + expectedType1.value()
                    + "], but API key ["
                    + apiKeyId
                    + "] has type ["
                    + apiKeyDoc1.type.value()
                    + "]"
            )
        );

        // Expired API key
        final var apiKeyDoc2 = buildApiKeyDoc(hash, pastTime, false, randomAlphaOfLengthBetween(3, 8), Version.CURRENT.id);
        final ApiKeyCredentials apiKeyCredentials2 = getApiKeyCredentials(apiKeyId, apiKey, apiKeyDoc2.type);
        final PlainActionFuture<AuthenticationResult<User>> future2 = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyTypeAndExpiration(apiKeyDoc2, apiKeyCredentials2, clock, future2);
        final AuthenticationResult<User> auth2 = future2.actionGet();
        assertThat(auth2.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(auth2.getValue(), nullValue());
        assertThat(auth2.getMessage(), containsString("api key is expired"));

        // Good API key
        final var apiKeyDoc3 = buildApiKeyDoc(
            hash,
            randomFrom(-1L, futureTime),
            false,
            randomAlphaOfLengthBetween(3, 8),
            Version.CURRENT.id
        );
        final ApiKeyCredentials apiKeyCredentials3 = getApiKeyCredentials(apiKeyId, apiKey, apiKeyDoc3.type);
        final PlainActionFuture<AuthenticationResult<User>> future3 = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyTypeAndExpiration(apiKeyDoc3, apiKeyCredentials3, clock, future3);
        final AuthenticationResult<User> auth3 = future3.actionGet();
        assertThat(auth3.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        assertThat(auth3.getValue(), notNullValue());
        assertThat(auth3.getMetadata(), hasEntry(API_KEY_TYPE_KEY, apiKeyDoc3.type.value()));
    }

    public void testCreateOrUpdateApiKeyWithWorkflowsRestrictionForUnsupportedVersion() {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD))
        );
        final ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        final TransportVersion minTransportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.MINIMUM_COMPATIBLE,
            TransportVersionUtils.getPreviousVersion(WORKFLOWS_RESTRICTION_VERSION)
        );
        when(clusterState.getMinTransportVersion()).thenReturn(minTransportVersion);

        final ApiKeyService service = new ApiKeyService(
            Settings.EMPTY,
            clock,
            client,
            securityIndex,
            clusterService,
            cacheInvalidatorRegistry,
            threadPool
        );

        final List<RoleDescriptor> roleDescriptorsWithWorkflowsRestriction = randomList(
            1,
            3,
            () -> randomRoleDescriptorWithWorkflowsRestriction()
        );

        final AbstractCreateApiKeyRequest createRequest = mock(AbstractCreateApiKeyRequest.class);
        when(createRequest.getType()).thenReturn(ApiKey.Type.REST);
        when(createRequest.getRoleDescriptors()).thenReturn(roleDescriptorsWithWorkflowsRestriction);

        final PlainActionFuture<CreateApiKeyResponse> createFuture = new PlainActionFuture<>();
        service.createApiKey(authentication, createRequest, Set.of(), createFuture);
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, createFuture::actionGet);
        assertThat(
            e1.getMessage(),
            containsString("all nodes must have transport version [8500005] or higher to support restrictions for API keys")
        );

        final BulkUpdateApiKeyRequest updateRequest = new BulkUpdateApiKeyRequest(
            randomList(1, 3, () -> randomAlphaOfLengthBetween(3, 5)),
            roleDescriptorsWithWorkflowsRestriction,
            Map.of()
        );
        final PlainActionFuture<BulkUpdateApiKeyResponse> updateFuture = new PlainActionFuture<>();
        service.updateApiKeys(authentication, updateRequest, Set.of(), updateFuture);
        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, createFuture::actionGet);
        assertThat(
            e2.getMessage(),
            containsString("all nodes must have transport version [8500005] or higher to support restrictions for API keys")
        );
    }

    public void testValidateOwnerUserRoleDescriptorsWithWorkflowsRestriction() {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD))
        );
        final ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        final TransportVersion minTransportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            WORKFLOWS_RESTRICTION_VERSION,
            TransportVersion.current()
        );
        when(clusterState.getMinTransportVersion()).thenReturn(minTransportVersion);
        final ApiKeyService service = new ApiKeyService(
            Settings.EMPTY,
            clock,
            client,
            securityIndex,
            clusterService,
            cacheInvalidatorRegistry,
            threadPool
        );

        final Set<RoleDescriptor> userRoleDescriptorsWithWorkflowsRestriction = randomSet(
            1,
            2,
            () -> randomRoleDescriptorWithWorkflowsRestriction()
        );
        final List<RoleDescriptor> requestRoleDescriptors = randomList(
            0,
            1,
            () -> RoleDescriptorTests.randomRoleDescriptor(randomBoolean(), randomBoolean(), randomBoolean())
        );

        final AbstractCreateApiKeyRequest createRequest = mock(AbstractCreateApiKeyRequest.class);
        when(createRequest.getType()).thenReturn(ApiKey.Type.REST);
        when(createRequest.getRoleDescriptors()).thenReturn(requestRoleDescriptors);

        final PlainActionFuture<CreateApiKeyResponse> createFuture = new PlainActionFuture<>();
        service.createApiKey(authentication, createRequest, userRoleDescriptorsWithWorkflowsRestriction, createFuture);
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, createFuture::actionGet);
        assertThat(e1.getMessage(), containsString("owner user role descriptors must not include restriction"));

        final BulkUpdateApiKeyRequest updateRequest = new BulkUpdateApiKeyRequest(
            randomList(1, 3, () -> randomAlphaOfLengthBetween(3, 5)),
            requestRoleDescriptors,
            Map.of()
        );
        final PlainActionFuture<BulkUpdateApiKeyResponse> updateFuture = new PlainActionFuture<>();
        service.updateApiKeys(authentication, updateRequest, userRoleDescriptorsWithWorkflowsRestriction, updateFuture);
        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, createFuture::actionGet);
        assertThat(e2.getMessage(), containsString("owner user role descriptors must not include restriction"));
    }

    private static RoleDescriptor randomRoleDescriptorWithRemoteIndexPrivileges() {
        return new RoleDescriptor(
            randomAlphaOfLengthBetween(3, 90),
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            RoleDescriptorTests.randomIndicesPrivileges(0, 3),
            RoleDescriptorTests.randomApplicationPrivileges(),
            RoleDescriptorTests.randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
            RoleDescriptorTests.randomRoleDescriptorMetadata(randomBoolean()),
            Map.of(),
            RoleDescriptorTests.randomRemoteIndicesPrivileges(1, 3),
            RoleRestrictionTests.randomWorkflowsRestriction(1, 3)
        );
    }

    private static RoleDescriptor randomRoleDescriptorWithWorkflowsRestriction() {
        return new RoleDescriptor(
            randomAlphaOfLengthBetween(3, 90),
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            RoleDescriptorTests.randomIndicesPrivileges(0, 3),
            RoleDescriptorTests.randomApplicationPrivileges(),
            RoleDescriptorTests.randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
            RoleDescriptorTests.randomRoleDescriptorMetadata(randomBoolean()),
            Map.of(),
            null,
            RoleRestrictionTests.randomWorkflowsRestriction(1, 3)
        );
    }

    public static class Utils {

        private static final AuthenticationContextSerializer authenticationContextSerializer = new AuthenticationContextSerializer();

        public static Authentication createApiKeyAuthentication(
            ApiKeyService apiKeyService,
            Authentication authentication,
            Set<RoleDescriptor> userRoles,
            List<RoleDescriptor> keyRoles,
            TransportVersion version
        ) throws Exception {
            XContentBuilder keyDocSource = ApiKeyService.newDocument(
                getFastStoredHashAlgoForTests().hash(new SecureString(randomAlphaOfLength(16).toCharArray())),
                "test",
                authentication,
                userRoles,
                Instant.now(),
                Instant.now().plus(Duration.ofSeconds(3600)),
                keyRoles,
                ApiKey.Type.REST,
                Version.CURRENT,
                randomBoolean() ? null : Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8))
            );
            final ApiKeyDoc apiKeyDoc = ApiKeyDoc.fromXContent(
                XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    BytesReference.bytes(keyDocSource),
                    XContentType.JSON
                )
            );
            PlainActionFuture<AuthenticationResult<User>> authenticationResultFuture = PlainActionFuture.newFuture();
            ApiKeyService.validateApiKeyTypeAndExpiration(
                apiKeyDoc,
                new ApiKeyService.ApiKeyCredentials("id", new SecureString(randomAlphaOfLength(16).toCharArray()), ApiKey.Type.REST),
                Clock.systemUTC(),
                authenticationResultFuture
            );

            AuthenticationResult<User> authenticationResult = authenticationResultFuture.get();
            if (randomBoolean()) {
                // maybe remove realm name to simulate old API Key authentication
                assert authenticationResult.getStatus() == AuthenticationResult.Status.SUCCESS;
                Map<String, Object> authenticationResultMetadata = new HashMap<>(authenticationResult.getMetadata());
                authenticationResultMetadata.remove(AuthenticationField.API_KEY_CREATOR_REALM_NAME);
                authenticationResult = AuthenticationResult.success(authenticationResult.getValue(), authenticationResultMetadata);
            }
            if (randomBoolean()) {
                // simulate authentication with nameless API Key, see https://github.com/elastic/elasticsearch/issues/59484
                assert authenticationResult.getStatus() == AuthenticationResult.Status.SUCCESS;
                Map<String, Object> authenticationResultMetadata = new HashMap<>(authenticationResult.getMetadata());
                authenticationResultMetadata.put(AuthenticationField.API_KEY_NAME_KEY, null);
                authenticationResult = AuthenticationResult.success(authenticationResult.getValue(), authenticationResultMetadata);
            }

            final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
            authenticationContextSerializer.writeToContext(
                Authentication.newApiKeyAuthentication(authenticationResult, "node01"),
                threadContext
            );
            final CompletableFuture<Authentication> authFuture = new CompletableFuture<>();
            securityContext.executeAfterRewritingAuthentication((c) -> {
                try {
                    authFuture.complete(authenticationContextSerializer.readFromContext(threadContext));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, version);
            return authFuture.get();
        }

        public static Authentication createApiKeyAuthentication(ApiKeyService apiKeyService, Authentication authentication)
            throws Exception {
            return createApiKeyAuthentication(
                apiKeyService,
                authentication,
                Collections.singleton(new RoleDescriptor("user_role_" + randomAlphaOfLength(4), new String[] { "manage" }, null, null)),
                null,
                TransportVersion.current()
            );
        }
    }

    private ApiKeyService createApiKeyService() {
        final Settings settings = Settings.builder().put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
        return createApiKeyService(settings);
    }

    private ApiKeyService createApiKeyService(Settings baseSettings) {
        final Settings settings = Settings.builder()
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(baseSettings)
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD))
        );
        final ApiKeyService service = new ApiKeyService(
            settings,
            clock,
            client,
            securityIndex,
            ClusterServiceUtils.createClusterService(threadPool, clusterSettings),
            cacheInvalidatorRegistry,
            threadPool
        );
        if ("0s".equals(settings.get(ApiKeyService.CACHE_TTL_SETTING.getKey()))) {
            verify(cacheInvalidatorRegistry, never()).registerCacheInvalidator(eq("api_key"), any());
        } else {
            verify(cacheInvalidatorRegistry).registerCacheInvalidator(eq("api_key"), any());
        }
        return service;
    }

    private Map<String, Object> buildApiKeySourceDoc(char[] hash) {
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("doc_type", "api_key");
        if (randomBoolean()) {
            sourceMap.put("type", randomFrom(ApiKey.Type.values()).value());
        }
        sourceMap.put("creation_time", Clock.systemUTC().instant().toEpochMilli());
        sourceMap.put("expiration_time", -1);
        sourceMap.put("api_key_hash", new String(hash));
        sourceMap.put("name", randomAlphaOfLength(12));
        sourceMap.put("version", 0);
        sourceMap.put("role_descriptors", Collections.singletonMap("a role", Collections.singletonMap("cluster", List.of("all"))));
        sourceMap.put(
            "limited_by_role_descriptors",
            Collections.singletonMap("limited role", Collections.singletonMap("cluster", List.of("all")))
        );
        Map<String, Object> creatorMap = new HashMap<>();
        creatorMap.put("principal", "test_user");
        creatorMap.put("full_name", "test user");
        creatorMap.put("email", "test@user.com");
        creatorMap.put("metadata", Collections.emptyMap());
        sourceMap.put("creator", creatorMap);
        sourceMap.put("api_key_invalidated", false);
        // noinspection unchecked
        sourceMap.put("metadata_flattened", ApiKeyTests.randomMetadata());
        return sourceMap;
    }

    private void mockSourceDocument(String id, Map<String, Object> sourceMap) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(sourceMap);
            SecurityMocks.mockGetRequest(client, id, BytesReference.bytes(builder));
        }
    }

    private ApiKeyDoc buildApiKeyDoc(char[] hash, long expirationTime, boolean invalidated) throws IOException {
        return buildApiKeyDoc(hash, expirationTime, invalidated, randomAlphaOfLength(12));
    }

    private ApiKeyDoc buildApiKeyDoc(char[] hash, long expirationTime, boolean invalidated, String name) throws IOException {
        return buildApiKeyDoc(hash, expirationTime, invalidated, name, 0);
    }

    private ApiKeyDoc buildApiKeyDoc(char[] hash, long expirationTime, boolean invalidated, String name, int version) throws IOException {
        final BytesReference metadataBytes = XContentTestUtils.convertToXContent(ApiKeyTests.randomMetadata(), XContentType.JSON);
        return new ApiKeyDoc(
            "api_key",
            randomBoolean() ? randomFrom(ApiKey.Type.values()) : null,
            Clock.systemUTC().instant().toEpochMilli(),
            expirationTime,
            invalidated,
            new String(hash),
            name,
            version,
            new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}"),
            new BytesArray("{\"limited role\": {\"cluster\": [\"all\"]}}"),
            Map.of(
                "principal",
                "test_user",
                "full_name",
                "test user",
                "email",
                "test@user.com",
                "realm",
                "realm1",
                "realm_type",
                "realm_type1",
                "metadata",
                Map.of()
            ),
            metadataBytes
        );
    }

    @SuppressWarnings("unchecked")
    private void checkAuthApiKeyMetadata(Object metadata, AuthenticationResult<User> authResult1) throws IOException {
        if (metadata == null) {
            assertThat(authResult1.getMetadata().containsKey(API_KEY_METADATA_KEY), is(false));
        } else {
            assertThat(
                authResult1.getMetadata().get(API_KEY_METADATA_KEY),
                equalTo(XContentTestUtils.convertToXContent((Map<String, Object>) metadata, XContentType.JSON))
            );
        }
    }

    private RoleReference.ApiKeyRoleType randomApiKeyRoleType() {
        return randomFrom(RoleReference.ApiKeyRoleType.values());
    }

    private ApiKeyCredentials getApiKeyCredentials(String id, String key, ApiKey.Type type) {
        return new ApiKeyCredentials(id, new SecureString(key.toCharArray()), type);
    }

    private ApiKey.Type parseTypeFromSourceMap(Map<String, Object> sourceMap) {
        if (sourceMap.containsKey("type")) {
            return ApiKey.Type.parse((String) sourceMap.get("type"));
        } else {
            return ApiKey.Type.REST;
        }
    }
}
