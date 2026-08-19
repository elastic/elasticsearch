/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.xpack.security.authc.service.UserManagedServiceAccountStore.SERVICE_ACCOUNT_DOC_TYPE;
import static org.elasticsearch.xpack.security.authc.service.UserManagedServiceAccountStore.USER_MANAGED_SERVICE_ACCOUNTS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserManagedServiceAccountStoreTests extends ESTestCase {

    private static final String PRINCIPAL = "engineering/deploy_bot";
    private static final ServiceAccountId ACCOUNT_ID = ServiceAccountId.fromPrincipal(PRINCIPAL);
    private static final String DOC_ID = SERVICE_ACCOUNT_DOC_TYPE + "-" + PRINCIPAL;
    private static final String ROLE_A = "deploy_bot_role_a";
    private static final String ROLE_B = "deploy_bot_role_b";

    private Client client;
    private ClusterService clusterService;
    private ClusterState clusterState;
    private SecurityIndexManager securityIndex;
    private SecurityIndexManager.IndexState projectIndex;
    private UserManagedServiceAccountStore store;

    private final List<ActionRequest> requests = new ArrayList<>();
    private final AtomicInteger getRequestCount = new AtomicInteger();
    private final List<String> clearedCacheKeys = new ArrayList<>();
    private final AtomicReference<BiConsumer<ActionRequest, ActionListener<ActionResponse>>> responseProvider = new AtomicReference<>();

    /**
     * The store is driven through a real {@link FilterClient} that records every request and answers it from
     * {@link #responseProvider}, so the tests assert on the requests the store actually builds rather than on mock
     * interactions. The collaborators below are mocked because neither can be constructed without a running node: a
     * {@link Client} needs a transport, and a {@link SecurityIndexManager} needs cluster state, index mappings and a
     * project resolver. {@link CacheInvalidatorRegistry} is cheap to construct, so the real one is used.
     */
    @Before
    public void init() {
        responseProvider.set((request, listener) -> fail("unexpected request " + request));

        final Client mockClient = mock(Client.class);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);
        final ThreadPool threadPool = mock(ThreadPool.class);
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
                requests.add(request);
                if (request instanceof GetRequest) {
                    getRequestCount.incrementAndGet();
                }
                responseProvider.get().accept(request, (ActionListener<ActionResponse>) listener);
            }
        };

        // The store reads only two values from cluster state, so a stub is enough here: the minimum transport version,
        // which gates writes, and the minimum node version, which is stamped into the document it writes.
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(Version.CURRENT);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(clusterState.getMinTransportVersion()).thenReturn(TransportVersion.current());
        when(clusterService.state()).thenReturn(clusterState);

        securityIndex = mock(SecurityIndexManager.class);
        projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        // Running the action inline bypasses the index version check and the index creation a real
        // SecurityIndexManager would perform first; the tests that need those to fail stub indexExists and
        // isAvailable instead.
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[1]).run();
            return null;
        }).when(projectIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[1]).run();
            return null;
        }).when(projectIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));

        store = new UserManagedServiceAccountStore(Settings.EMPTY, client, securityIndex, clusterService, new CacheInvalidatorRegistry());
    }

    public void testLoadedAccountIsAuthorizedByItsNamedRoles() {
        final boolean enabled = randomBoolean();
        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_A, ROLE_B), enabled));

        final UserManagedServiceAccount account = getByPrincipal(PRINCIPAL);
        assertThat(account.id(), equalTo(ACCOUNT_ID));
        assertThat(account.roles(), contains(ROLE_A, ROLE_B));
        assertThat(account.enabled(), is(enabled));
        assertThat(account.asUser().principal(), equalTo(PRINCIPAL));
        assertThat(account.asUser().roles(), arrayContaining(ROLE_A, ROLE_B));
        assertThat(account.asUser().enabled(), is(enabled));
        // The marker is what routes authorization to the named roles above rather than to a built-in account of
        // the same name, so an account without it would silently authorize as something else.
        assertThat(account.asUser().metadata(), equalTo(Map.of(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD, true)));
    }

    public void testGetByPrincipalCachesTheAccount() {
        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_A), true));

        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_A));
        assertThat(getRequestCount.get(), equalTo(1));

        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_A));
        assertThat(getRequestCount.get(), equalTo(1));
    }

    public void testGetByPrincipalCachesTheAbsenceOfAnAccount() {
        respondToGetWith(null);

        assertThat(getByPrincipal(PRINCIPAL), nullValue());
        assertThat(getRequestCount.get(), equalTo(1));

        assertThat(getByPrincipal(PRINCIPAL), nullValue());
        assertThat(getRequestCount.get(), equalTo(1));
    }

    public void testGetByPrincipalFindsNothingForPrincipalsNoAccountCouldHold() {
        final String principal = randomFrom(
            "elastic/fleet-server",       // the reserved namespace
            "engineering",                // not a {namespace}/{service-name} pair
            "engineering/deploy bot",     // outside the permitted character set
            "_engineering/deploy_bot"     // does not start with a letter or digit
        );
        assertThat(getByPrincipal(principal), nullValue());
        assertThat(getRequestCount.get(), equalTo(0));
    }

    public void testMalformedDocumentsAreTreatedAsAbsentAccounts() {
        final Map<String, Consumer<Map<String, Object>>> corruptions = new LinkedHashMap<>();
        corruptions.put("doc_type of another document type", source -> source.put("doc_type", "user"));
        corruptions.put("missing doc_type", source -> source.remove("doc_type"));
        corruptions.put("username of another account", source -> source.put("username", "engineering/other_bot"));
        corruptions.put("missing username", source -> source.remove("username"));
        corruptions.put("missing roles", source -> source.remove("roles"));
        corruptions.put("roles that is not a list", source -> source.put("roles", ROLE_A));
        corruptions.put("a role that is not a string", source -> source.put("roles", List.of(ROLE_A, 42)));
        corruptions.put("a role name that is not valid", source -> source.put("roles", List.of(" leading space")));
        corruptions.put("missing enabled", source -> source.remove("enabled"));
        corruptions.put("enabled that is not a boolean", source -> source.put("enabled", "true"));

        corruptions.forEach((description, corruption) -> {
            final Map<String, Object> source = accountDocument(PRINCIPAL, List.of(ROLE_A), true);
            corruption.accept(source);
            respondToGetWith(source);
            store.invalidateAll();
            assertThat("document with " + description, getByPrincipal(PRINCIPAL), nullValue());
        });
    }

    public void testAReadThatRacedAnInvalidationDoesNotPopulateTheCache() throws Exception {
        final CountDownLatch releaseGet = new CountDownLatch(1);
        responseProvider.set((request, listener) -> {
            try {
                releaseGet.await();
                respondToGet(request, accountDocument(PRINCIPAL, List.of(ROLE_A), true), listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });

        final PlainActionFuture<UserManagedServiceAccount> racingRead = new PlainActionFuture<>();
        final Thread readingThread = new Thread(() -> store.getByPrincipal(PRINCIPAL, racingRead));
        readingThread.start();
        assertBusy(() -> assertThat(getRequestCount.get(), equalTo(1)));

        store.invalidate(List.of(PRINCIPAL));
        releaseGet.countDown();
        readingThread.join();

        // The in-flight read still answers its own caller, but its result is too old to be shared.
        assertThat(racingRead.actionGet().roles(), contains(ROLE_A));
        assertThat(store.getAccountCache().get(PRINCIPAL), nullValue());

        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_B), true));
        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_B));
        assertThat(getRequestCount.get(), equalTo(2));
    }

    public void testPutAccountWritesTheDocumentAndClearsTheCacheClusterWide() {
        respondWithBulkResult(true);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_B, ROLE_A, ROLE_B), false, RefreshPolicy.WAIT_UNTIL, future);
        assertThat(future.actionGet(), is(UserManagedServiceAccountStore.PutResult.CREATED));

        assertThat(onlyRequestOfType(BulkRequest.class).getRefreshPolicy(), is(RefreshPolicy.WAIT_UNTIL));
        final IndexRequest indexRequest = indexedDocument();
        assertThat(indexRequest.id(), equalTo(DOC_ID));
        assertThat(indexRequest.opType(), is(OpType.INDEX));
        final Map<String, Object> source = indexRequest.sourceAsMap();
        assertThat(source.get("doc_type"), equalTo(SERVICE_ACCOUNT_DOC_TYPE));
        assertThat(source.get("username"), equalTo(PRINCIPAL));
        assertThat(source.get("version"), equalTo(Version.CURRENT.id));
        assertThat(source.get("enabled"), is(false));
        // Sorted and de-duplicated, so that the document does not depend on how the caller ordered the roles.
        assertThat(source.get("roles"), equalTo(List.of(ROLE_A, ROLE_B)));

        assertThat(clearedCacheKeys, contains(PRINCIPAL));
    }

    public void testPutAccountReportsAnUpdateOfAnExistingAccount() {
        respondWithBulkResult(false);

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(UserManagedServiceAccountStore.PutResult.UPDATED));
    }

    public void testPutAccountReportsEveryValidationErrorAtOnce() {
        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(
            new ServiceAccountId("elastic", "deploy bot"),
            List.of("a role name that is far too long".repeat(32)),
            true,
            RefreshPolicy.NONE,
            future
        );

        final ValidationException e = expectThrows(ValidationException.class, future::actionGet);
        assertThat(e.validationErrors(), hasSize(3));
        assertThat(e.getMessage(), containsString("the [elastic] namespace is reserved for built-in service accounts"));
        assertThat(e.getMessage(), containsString("service account service name [deploy bot]"));
        assertThat(e.getMessage(), containsString("Role names must be at least"));
    }

    public void testPutAccountRequiresEveryNodeToSupportUserManagedServiceAccounts() {
        when(clusterState.getMinTransportVersion()).thenReturn(
            TransportVersionUtils.randomVersionNotSupporting(USER_MANAGED_SERVICE_ACCOUNTS)
        );

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        store.putAccount(ACCOUNT_ID, List.of(ROLE_A), true, RefreshPolicy.NONE, future);

        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            equalTo(
                "all nodes must have version ["
                    + USER_MANAGED_SERVICE_ACCOUNTS.toReleaseVersion()
                    + "] or higher to support user-managed service accounts"
            )
        );
    }

    public void testDeleteAccountClearsTheCacheClusterWide() {
        respondWithDeleteResult(true);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.IMMEDIATE, future);
        assertThat(future.actionGet(), is(true));

        assertThat(clearedCacheKeys, contains(PRINCIPAL));
    }

    public void testDeleteAccountReportsWhenThereWasNothingToDelete() {
        respondWithDeleteResult(false);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.IMMEDIATE, future);
        assertThat(future.actionGet(), is(false));

        assertThat(clearedCacheKeys, empty());
    }

    public void testDeleteAccountFailsWhenTheCacheCannotBeCleared() {
        final ElasticsearchException failure = new ElasticsearchException("node unreachable");
        responseProvider.set((request, listener) -> {
            if (request instanceof DeleteRequest) {
                listener.onResponse(deleteResponse(true));
            } else if (request instanceof ClearSecurityCacheRequest) {
                listener.onFailure(failure);
            } else {
                fail("unexpected request " + request);
            }
        });

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.IMMEDIATE, future);

        final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("clearing the cache for service account [" + PRINCIPAL + "] failed"));
        assertThat(e.getCause(), is(failure));
    }

    public void testDeleteAccountRejectsAnIdNoAccountCouldHold() {
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(new ServiceAccountId("elastic", "fleet-server"), RefreshPolicy.NONE, future);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("the [elastic] namespace is reserved for built-in service accounts"));
    }

    public void testDeleteAccountIsNotGatedOnTheClusterTransportVersion() {
        // Deleting is how an operator resolves a cluster that holds accounts an older node cannot authorize, so
        // unlike creating it stays available while a rolling upgrade is in progress.
        when(clusterState.getMinTransportVersion()).thenReturn(
            TransportVersionUtils.randomVersionNotSupporting(USER_MANAGED_SERVICE_ACCOUNTS)
        );
        respondWithDeleteResult(true);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(true));
    }

    public void testListAccountsSelectsASingleAccountByPrincipal() {
        respondToSearchWith(List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true)));

        final List<UserManagedServiceAccount> accounts = listAccounts("engineering", "deploy_bot");
        assertThat(accounts, hasSize(1));
        assertThat(accounts.get(0).id(), equalTo(ACCOUNT_ID));

        assertThat(
            searchedQuery(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE))
                    .filter(QueryBuilders.termQuery("username", PRINCIPAL))
            )
        );
    }

    public void testListAccountsSelectsANamespaceByPrefix() {
        respondToSearchWith(
            List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true), accountDocument("engineering/other_bot", List.of(ROLE_B), false))
        );

        assertThat(listAccounts("engineering", null), hasSize(2));
        assertThat(
            searchedQuery(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE))
                    .filter(QueryBuilders.prefixQuery("username", "engineering/"))
            )
        );
    }

    public void testListAccountsNarrowsToAServiceNameGivenWithoutANamespace() {
        respondToSearchWith(
            List.of(accountDocument(PRINCIPAL, List.of(ROLE_A), true), accountDocument("operations/other_bot", List.of(ROLE_B), false))
        );

        final List<UserManagedServiceAccount> accounts = listAccounts(null, "deploy_bot");
        assertThat(accounts, hasSize(1));
        assertThat(accounts.get(0).id(), equalTo(ACCOUNT_ID));

        // A leading wildcard would be the only way to express this in the query, so it is applied after parsing.
        assertThat(
            searchedQuery(),
            equalTo(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)))
        );
    }

    public void testAStoredDocumentCannotClaimTheReservedNamespace() {
        // Principals are re-validated on read, so a document written by hand cannot shadow a built-in account.
        respondToSearchWith(List.of(accountDocument("elastic/fleet-server", List.of(ROLE_A), true)));
        assertThat(listAccounts(null, null), empty());
    }

    public void testAccountsAreReadFromTheIndexEveryTimeWhenCachingIsDisabled() {
        store = new UserManagedServiceAccountStore(
            Settings.builder().put(UserManagedServiceAccountStore.CACHE_TTL_SETTING.getKey(), TimeValue.ZERO).build(),
            client,
            securityIndex,
            clusterService,
            new CacheInvalidatorRegistry()
        );
        respondToGetWith(accountDocument(PRINCIPAL, List.of(ROLE_A), true));

        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_A));
        assertThat(getByPrincipal(PRINCIPAL).roles(), contains(ROLE_A));
        assertThat(getRequestCount.get(), equalTo(2));
        assertThat(store.getAccountCache(), nullValue());
    }

    public void testListAccountsFindsNothingForIdsNoAccountCouldHold() {
        assertThat(listAccounts("elastic", null), empty());
        assertThat(listAccounts("engineering*", null), empty());
        assertThat(listAccounts("engineering", "deploy*"), empty());
        assertThat(requests, empty());
    }

    public void testAnAbsentSecurityIndexHoldsNoAccounts() {
        when(projectIndex.indexExists()).thenReturn(false);

        assertThat(getByPrincipal(PRINCIPAL), nullValue());
        assertThat(listAccounts(null, null), empty());

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(false));
    }

    public void testAnUnavailableSecurityIndexFailsTheRequest() {
        final ElasticsearchException unavailable = new ElasticsearchException("index unavailable");
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(false);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(false);
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(unavailable);
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(unavailable);

        final PlainActionFuture<UserManagedServiceAccount> get = new PlainActionFuture<>();
        store.getByPrincipal(PRINCIPAL, get);
        assertThat(expectThrows(ElasticsearchException.class, get::actionGet), is(unavailable));

        final PlainActionFuture<List<UserManagedServiceAccount>> list = new PlainActionFuture<>();
        store.listAccounts(null, null, list);
        assertThat(expectThrows(ElasticsearchException.class, list::actionGet), is(unavailable));

        final PlainActionFuture<Boolean> delete = new PlainActionFuture<>();
        store.deleteAccount(ACCOUNT_ID, RefreshPolicy.NONE, delete);
        assertThat(expectThrows(ElasticsearchException.class, delete::actionGet), is(unavailable));
    }

    private UserManagedServiceAccount getByPrincipal(String principal) {
        final PlainActionFuture<UserManagedServiceAccount> future = new PlainActionFuture<>();
        store.getByPrincipal(principal, future);
        return future.actionGet();
    }

    private List<UserManagedServiceAccount> listAccounts(String namespace, String serviceName) {
        final PlainActionFuture<List<UserManagedServiceAccount>> future = new PlainActionFuture<>();
        store.listAccounts(namespace, serviceName, future);
        return future.actionGet();
    }

    private void respondToGetWith(Map<String, Object> source) {
        responseProvider.set((request, listener) -> {
            try {
                respondToGet(request, source, listener);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

    private static void respondToGet(ActionRequest request, Map<String, Object> source, ActionListener<?> listener) throws IOException {
        assertThat(request, instanceOf(GetRequest.class));
        final GetRequest getRequest = (GetRequest) request;
        assertThat(getRequest.id(), equalTo(DOC_ID));
        final GetResult getResult = new GetResult(
            getRequest.index(),
            getRequest.id(),
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            1L,
            source != null,
            source == null ? null : BytesReference.bytes(XContentFactory.jsonBuilder().map(source)),
            Map.of(),
            Map.of()
        );
        @SuppressWarnings("unchecked")
        final ActionListener<GetResponse> getListener = (ActionListener<GetResponse>) listener;
        getListener.onResponse(new GetResponse(getResult));
    }

    private void respondWithBulkResult(boolean created) {
        responseProvider.set((request, listener) -> {
            if (request instanceof BulkRequest) {
                listener.onResponse(
                    new BulkResponse(
                        new BulkItemResponse[] {
                            BulkItemResponse.success(
                                0,
                                OpType.INDEX,
                                new IndexResponse(mock(ShardId.class), DOC_ID, randomLong(), randomLong(), randomLong(), created)
                            ) },
                        randomLong()
                    )
                );
            } else if (recordClearedCache(request, listener) == false) {
                fail("unexpected request " + request);
            }
        });
    }

    private void respondWithDeleteResult(boolean found) {
        responseProvider.set((request, listener) -> {
            if (request instanceof DeleteRequest) {
                listener.onResponse(deleteResponse(found));
            } else if (recordClearedCache(request, listener) == false) {
                fail("unexpected request " + request);
            }
        });
    }

    private static DeleteResponse deleteResponse(boolean found) {
        return new DeleteResponse(mock(ShardId.class), DOC_ID, randomLong(), randomLong(), randomLong(), found);
    }

    private boolean recordClearedCache(ActionRequest request, ActionListener<ActionResponse> listener) {
        if (request instanceof ClearSecurityCacheRequest clearSecurityCacheRequest) {
            assertThat(clearSecurityCacheRequest.cacheName(), equalTo(UserManagedServiceAccountStore.CACHE_NAME));
            clearedCacheKeys.addAll(List.of(clearSecurityCacheRequest.keys()));
            listener.onResponse(new ClearSecurityCacheResponse(mock(ClusterName.class), List.of(), List.of()));
            return true;
        }
        return false;
    }

    private void respondToSearchWith(List<Map<String, Object>> sources) {
        responseProvider.set((request, listener) -> {
            if (request instanceof SearchRequest) {
                ActionListener.respondAndRelease(listener, searchResponse(sources));
            } else if (request instanceof SearchScrollRequest) {
                // Reached only when a hit did not parse, since the scroll runs until as many results as hits
                // have been collected. An empty page ends it.
                ActionListener.respondAndRelease(listener, searchResponse(List.of()));
            } else if (request instanceof ClearScrollRequest) {
                listener.onResponse(new ClearScrollResponse(true, 1));
            } else {
                fail("unexpected request " + request);
            }
        });
    }

    private static SearchResponse searchResponse(List<Map<String, Object>> sources) {
        final SearchHit[] hits = new SearchHit[sources.size()];
        for (int i = 0; i < hits.length; i++) {
            final Map<String, Object> source = sources.get(i);
            hits[i] = SearchHit.unpooled(i, SERVICE_ACCOUNT_DOC_TYPE + "-" + source.get("username"));
            try {
                hits[i].sourceRef(BytesReference.bytes(XContentFactory.jsonBuilder().map(source)));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        final SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f);
        try {
            return SearchResponseUtils.successfulResponse(searchHits);
        } finally {
            searchHits.decRef();
        }
    }

    private IndexRequest indexedDocument() {
        final BulkRequest bulkRequest = onlyRequestOfType(BulkRequest.class);
        assertThat(bulkRequest.requests(), hasSize(1));
        return (IndexRequest) bulkRequest.requests().get(0);
    }

    private QueryBuilder searchedQuery() {
        return onlyRequestOfType(SearchRequest.class).source().query();
    }

    private <T extends ActionRequest> T onlyRequestOfType(Class<T> requestClass) {
        final List<T> matching = requests.stream().filter(requestClass::isInstance).map(requestClass::cast).toList();
        assertThat(matching, hasSize(1));
        return matching.get(0);
    }

    private static Map<String, Object> accountDocument(String principal, List<String> roles, boolean enabled) {
        final Map<String, Object> source = new HashMap<>();
        source.put("doc_type", SERVICE_ACCOUNT_DOC_TYPE);
        source.put("version", Version.CURRENT.id);
        source.put("username", principal);
        source.put("roles", roles);
        source.put("enabled", enabled);
        return source;
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
