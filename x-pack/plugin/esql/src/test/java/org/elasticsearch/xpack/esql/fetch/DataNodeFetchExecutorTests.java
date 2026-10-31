/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.fetch.FetchResponse.ShardResult;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.FetchSourceProvider;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerServices;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * The data node half of a fetch, on a real {@code SearchService} with the reader contexts the query phase keeps open.
 * {@code MockSearchService} fails a test that leaves a context open, and every test ends with an empty request breaker.
 */
public class DataNodeFetchExecutorTests extends FetchContextsTestCase {
    private static final String FETCHED = "fetched";
    /**
     * An index with enough documents in a shard to split it.
     */
    private static final String SPLIT = "split";
    private static final int DOCS = 40;
    private static final Attribute ID = field("id", DataType.KEYWORD);
    private static final Attribute N = field("n", DataType.LONG);

    private BlockFactory blockFactory;

    /**
     * The open context of one shard and where each document of the shard is, by its {@code n}.
     */
    private record OpenShard(ShardId shardId, ShardSearchContextId contextId, Map<Long, int[]> segmentAndDocByN) {}

    private record Fetched(List<ShardResult> results, List<Page> pages, DriverCompletionInfo info) implements Releasable {
        @Override
        public void close() {
            FetchResponse.releasePages(pages);
        }
    }

    @Before
    public void createIndexAndBlockFactory() {
        CircuitBreaker breaker = getInstanceFromNode(CircuitBreakerService.class).getBreaker(CircuitBreaker.REQUEST);
        blockFactory = BlockFactory.builder(getInstanceFromNode(BigArrays.class)).breaker(breaker).build();
        assertAcked(
            indicesAdmin().prepareCreate(FETCHED)
                .setSettings(Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0))
                .setMapping("id", "type=keyword", "n", "type=long")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS; i++) {
            bulk.add(prepareIndex(FETCHED).setSource("id", "doc-" + i, "n", i));
        }
        assertFalse(bulk.get().hasFailures());
    }

    @After
    public void assertTheBreakerIsEmpty() throws Exception {
        // the executor drops its references on the generic pool, after the response
        assertBusy(() -> assertThat(blockFactory.breaker().getUsed(), equalTo(0L)));
    }

    public void testFetchesTheRowsOfEachShardInRequestOrder() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, randomSubsetOf(between(1, DOCS), allNs()));

        try (Fetched fetched = fetch(request)) {
            assertThat(fetched.results().stream().map(ShardResult::failure).toList(), everyItem(nullValue()));
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, request)));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * A context that is gone, for example because its keep-alive passed, fails its shard. The other shards still load.
     */
    public void testAMissingContextFailsOnlyItsShard() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        int missing = between(0, request.shards().size() - 1);
        List<FetchRequest.ShardDocs> withMissing = new ArrayList<>(request.shards());
        FetchRequest.ShardDocs shard = withMissing.get(missing);
        ShardSearchContextId gone = new ShardSearchContextId(shard.contextId().getSessionId(), Long.MAX_VALUE);
        withMissing.set(missing, new FetchRequest.ShardDocs(shard.shardId(), gone, shard.segments(), shard.docs()));
        FetchRequest partial = withShards(request, withMissing);

        try (Fetched fetched = fetch(partial)) {
            for (int s = 0; s < withMissing.size(); s++) {
                ShardResult result = fetched.results().get(s);
                if (s == missing) {
                    assertThat(result.failure(), instanceOf(SearchContextMissingException.class));
                    assertThat(result.rows(), equalTo(0));
                } else {
                    assertThat(result.failure(), nullValue());
                    assertThat(result.rows(), equalTo(withMissing.get(s).docCount()));
                }
            }
            List<FetchRequest.ShardDocs> loaded = new ArrayList<>(withMissing);
            loaded.remove(missing);
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, withShards(request, loaded))));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * Authorization drops the indices the user lost since the query phase. Their shards fail instead of reading
     * without document and field level security.
     */
    public void testAShardOfAnIndexAuthorizationDroppedFails() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();

        Fetched fetched;
        try (var ignored = threadContext.stashContext()) {
            AuthorizationServiceField.INDICES_PERMISSIONS_VALUE.set(threadContext, IndicesAccessControl.ALLOW_NO_INDICES);
            fetched = fetch(request);
        }
        try (fetched) {
            for (ShardResult result : fetched.results()) {
                assertThat(result.failure(), instanceOf(ElasticsearchSecurityException.class));
                assertThat(result.failure().getMessage(), containsString("is unauthorized for index [" + FETCHED + "]"));
            }
            assertThat(fetched.pages(), hasSize(0));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * Like the fetch phase of a search, a fetch request reads only single-session contexts, not a scroll or a point in
     * time whose id it learned.
     */
    public void testAPointInTimeIsMissing() throws Exception {
        ShardId shardId = new ShardId(resolveIndex(FETCHED), 0);
        PlainActionFuture<ShardSearchContextId> opened = new PlainActionFuture<>();
        searchService().openReaderContext(shardId, TimeValue.timeValueMinutes(5), SplitShardCountSummary.IRRELEVANT, opened);
        ShardSearchContextId pointInTime = opened.actionGet(TimeValue.timeValueSeconds(10));
        try {
            FetchRequest.ShardDocs docs = new FetchRequest.ShardDocs(shardId, pointInTime, new int[] { 0 }, new int[] { 0 });
            FetchRequest request = request(List.of(), List.of());
            try (Fetched fetched = fetch(withShards(request, List.of(docs)))) {
                assertThat(fetched.results().get(0).failure(), instanceOf(SearchContextMissingException.class));
                assertThat(fetched.pages(), hasSize(0));
            }
            assertThat("the point in time stays open", searchService().getActiveContexts(), equalTo(1));
        } finally {
            searchService().freeReaderContext(pointInTime);
        }
        assertAllFreed();
    }

    /**
     * A document outside the reader of its context would fail deep in the loaders or load another document.
     */
    public void testADocumentOutsideTheReaderFailsItsShard() throws Exception {
        List<OpenShard> shards = openEveryShard();
        OpenShard shard = randomFrom(shards);
        FetchRequest.ShardDocs outside = new FetchRequest.ShardDocs(
            shard.shardId(),
            shard.contextId(),
            new int[] { 0, 10_000 },
            new int[] { 0, 0 }
        );
        FetchRequest request = withShards(request(shards, List.of()), List.of(outside));

        try (Fetched fetched = fetch(request)) {
            assertThat(fetched.results().get(0).failure(), instanceOf(IllegalArgumentException.class));
            assertThat(fetched.results().get(0).failure().getMessage(), containsString("isn't in the reader of"));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * A cancelled request opens no search context and loads nothing. The contexts stay with their coordinator.
     */
    public void testACancelledRequestLoadsNothing() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        CancellableTask task = newTask();
        TaskCancelHelper.cancel(task, "the query was cancelled");

        Exception e = expectThrows(Exception.class, () -> fetch(request, task, between(1, 10)));
        assertThat(e, instanceOf(TaskCancelledException.class));
        assertThat("the contexts stay open", searchService().getActiveContexts(), equalTo(SHARDS));
        freeAndAssertAllFreed(shards);
    }

    /**
     * A {@code _doc} column can't leave the node, so a fetch plan that returns one fails before it loads anything.
     */
    public void testAFetchPlanThatReturnsDocumentsFails() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        FetchRequest returnsDocs = new FetchRequest(
            request.sessionId(),
            request.clusterAlias(),
            new OriginalIndices(request.indices(), request.indicesOptions()),
            request.shards(),
            request.configuration(),
            new ProjectExec(Source.EMPTY, new FetchSourceExec(Source.EMPTY, doc, 64), List.of(doc))
        );

        Exception e = expectThrows(Exception.class, () -> fetch(returnsDocs));
        assertThat(e, instanceOf(IllegalArgumentException.class));
        assertThat(e.getMessage(), containsString("a fetch plan can't return"));
        freeAndAssertAllFreed(shards);
    }

    /**
     * The node loads at most a bounded number of shards at a time, and still loads every shard of the request.
     */
    public void testLoadsEveryShardOneAtATime() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        AtomicInteger loading = new AtomicInteger();
        AtomicInteger mostLoading = new AtomicInteger();
        SourceWrapper observe = (driver, source) -> new DelegatingSource(source) {
            private boolean started;

            @Override
            public Page getOutput() {
                if (started == false) {
                    started = true;
                    mostLoading.accumulateAndGet(loading.incrementAndGet(), Math::max);
                }
                return super.getOutput();
            }

            @Override
            public void close() {
                if (started) {
                    loading.decrementAndGet();
                }
                super.close();
            }
        };

        try (Fetched fetched = fetch(request, newTask(), 1, observe)) {
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, request)));
        }
        assertThat(mostLoading.get(), equalTo(1));
        freeAndAssertAllFreed(shards);
    }

    /**
     * A driver that fails, for example because the breaker tripped while it loaded, fails its shard. The other shards
     * still return their rows, and a profile lists the other drivers only.
     */
    public void testAFailedDriverFailsOnlyItsShard() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        if (randomBoolean()) {
            request = withConfiguration(request, new ConfigurationBuilder(request.configuration()).profile(true).build());
        }
        int maxConcurrentShardTasks = between(1, 10);
        List<Integer> shardOfDriver = shardOfEachDriver(request, maxConcurrentShardTasks);
        int failingDriver = between(0, shardOfDriver.size() - 1);
        int failing = shardOfDriver.get(failingDriver);
        SourceWrapper failOne = (driver, source) -> driver != failingDriver ? source : new DelegatingSource(source) {
            @Override
            public Page getOutput() {
                throw new IllegalStateException("simulated failure");
            }
        };

        try (Fetched fetched = fetch(request, newTask(), maxConcurrentShardTasks, failOne)) {
            for (int s = 0; s < request.shards().size(); s++) {
                ShardResult result = fetched.results().get(s);
                if (s == failing) {
                    assertThat(result.failure(), instanceOf(IllegalStateException.class));
                    assertThat(result.failure().getMessage(), equalTo("simulated failure"));
                    assertThat(result.rows(), equalTo(0));
                } else {
                    assertThat(result.failure(), nullValue());
                    assertThat(result.rows(), equalTo(request.shards().get(s).docCount()));
                }
            }
            List<FetchRequest.ShardDocs> loaded = new ArrayList<>(request.shards());
            loaded.remove(failing);
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, withShards(request, loaded))));
            if (request.configuration().profile()) {
                assertThat(fetched.info().driverProfiles(), hasSize(shardOfDriver.size() - 1));
            }
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * A shard with many documents splits into pieces, one driver each, and the drivers of its pieces load at the same time:
     * the first two drivers wait for each other before they load anything.
     */
    public void testLoadsTheDocumentsOfOneShardOnSeveralDrivers() throws Exception {
        List<OpenShard> shards = openEveryShardOf(SPLIT, 1, 100);
        FetchRequest request = request(shards, ns(100));
        int maxConcurrentShardTasks = between(2, 10);
        assertThat(shardOfEachDriver(request, maxConcurrentShardTasks).size(), greaterThan(1));
        CountDownLatch together = new CountDownLatch(2);
        SourceWrapper meet = (driver, source) -> new DelegatingSource(source) {
            private boolean started;

            @Override
            public Page getOutput() {
                if (started == false) {
                    started = true;
                    together.countDown();
                    safeAwait(together);
                }
                return super.getOutput();
            }
        };

        try (Fetched fetched = fetch(request, newTask(), maxConcurrentShardTasks, meet)) {
            assertThat(fetched.results().getFirst().rows(), equalTo(100));
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, request)));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * The rows of a shard split into pieces come back in the order of the request, piece after piece.
     */
    public void testTheRowsOfASplitShardComeBackInRequestOrder() throws Exception {
        int docs = between(40, 200);
        List<OpenShard> shards = openEveryShardOf(SPLIT, between(1, 2), docs);
        FetchRequest request = request(shards, ns(docs));
        int maxConcurrentShardTasks = between(2, 10);
        assertThat("a shard splits", shardOfEachDriver(request, maxConcurrentShardTasks).size(), greaterThan(request.shards().size()));

        try (Fetched fetched = fetch(request, newTask(), maxConcurrentShardTasks)) {
            assertThat(fetched.results().stream().map(ShardResult::failure).toList(), everyItem(nullValue()));
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, request)));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * A piece that fails fails its whole shard, so the response never holds a part of the rows of a shard. The other
     * shards still return theirs.
     */
    public void testAFailedPieceFailsItsWholeShard() throws Exception {
        List<OpenShard> shards = openEveryShardOf(SPLIT, 2, 100);
        FetchRequest request = request(shards, ns(100));
        int maxConcurrentShardTasks = between(2, 10);
        List<Integer> shardOfDriver = shardOfEachDriver(request, maxConcurrentShardTasks);
        List<Integer> driversOfSplitShards = new ArrayList<>();
        for (int driver = 0; driver < shardOfDriver.size(); driver++) {
            if (Collections.frequency(shardOfDriver, shardOfDriver.get(driver)) > 1) {
                driversOfSplitShards.add(driver);
            }
        }
        int failingDriver = randomFrom(driversOfSplitShards);
        int failing = shardOfDriver.get(failingDriver);
        SourceWrapper failOne = (driver, source) -> driver != failingDriver ? source : new DelegatingSource(source) {
            @Override
            public Page getOutput() {
                throw new IllegalStateException("simulated failure");
            }
        };

        try (Fetched fetched = fetch(request, newTask(), maxConcurrentShardTasks, failOne)) {
            ShardResult failed = fetched.results().get(failing);
            assertThat(failed.failure(), instanceOf(IllegalStateException.class));
            assertThat(failed.rows(), equalTo(0));
            List<FetchRequest.ShardDocs> loaded = new ArrayList<>(request.shards());
            loaded.remove(failing);
            for (FetchRequest.ShardDocs shard : loaded) {
                ShardResult result = fetched.results().get(request.shards().indexOf(shard));
                assertThat(result.failure(), nullValue());
                assertThat(result.rows(), equalTo(shard.docCount()));
            }
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, withShards(request, loaded))));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * A shard splits into at most as many pieces as the request runs drivers at the same time, and a request that runs
     * one driver at a time loads each shard whole.
     */
    public void testAShardSplitsIntoAtMostAsManyPiecesAsTheRequestRunsDrivers() throws Exception {
        List<OpenShard> shards = openEveryShardOf(SPLIT, 1, 200);
        FetchRequest request = withConfiguration(
            request(shards, ns(200)),
            new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).profile(true).build()
        );
        int maxConcurrentShardTasks = randomBoolean() ? 1 : between(2, 10);

        try (Fetched fetched = fetch(request, newTask(), maxConcurrentShardTasks)) {
            // every piece of 200 documents holds at least 20, so no cut is dropped
            assertThat(fetched.info().driverProfiles(), hasSize(maxConcurrentShardTasks));
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, request)));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * A fetch plan the node can't plan fails the request, and the node releases the shards it already opened.
     */
    public void testAPlanningFailureReleasesTheOpenShards() throws Exception {
        List<OpenShard> shards = openEveryShard();
        Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        // without a row size estimate the source can't size its pages
        PhysicalPlan unsized = new ProjectExec(
            Source.EMPTY,
            new FieldExtractExec(Source.EMPTY, new FetchSourceExec(Source.EMPTY, doc, null), List.of(ID, N), NONE),
            List.of(ID, N)
        );
        FetchRequest request = withFetchPlan(request(shards, allNs()), unsized);

        Exception e = expectThrows(Exception.class, () -> fetch(request));
        assertThat(e.getMessage(), containsString("estimated row size hasn't been set"));
        freeAndAssertAllFreed(shards);
    }

    /**
     * Cancelling the query while the node loads stops the drivers and fails the request. The contexts stay with their
     * coordinator.
     */
    public void testCancellationWhileLoadingFailsTheRequest() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        CancellableTask task = newTask();
        SourceWrapper cancelWhileLoading = (driver, source) -> driver != 0 ? source : new DelegatingSource(source) {
            @Override
            public Page getOutput() {
                if (task.isCancelled() == false) {
                    TaskCancelHelper.cancel(task, "the query was cancelled");
                }
                return super.getOutput();
            }
        };

        Exception e = expectThrows(Exception.class, () -> fetch(request, task, between(1, 10), cancelWhileLoading));
        assertThat(e, instanceOf(TaskCancelledException.class));
        assertThat("the contexts stay open", searchService().getActiveContexts(), equalTo(SHARDS));
        freeAndAssertAllFreed(shards);
    }

    /**
     * A profiled query gets a {@code fetch} driver for every piece of every shard the request loaded.
     */
    public void testProfileListsAFetchDriverPerPiece() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        FetchRequest profiledRequest = withConfiguration(request, new ConfigurationBuilder(request.configuration()).profile(true).build());
        int maxConcurrentShardTasks = between(1, 10);

        try (Fetched fetched = fetch(profiledRequest, newTask(), maxConcurrentShardTasks)) {
            List<DriverProfile> profiles = fetched.info().driverProfiles();
            assertThat(profiles, hasSize(shardOfEachDriver(request, maxConcurrentShardTasks).size()));
            for (DriverProfile profile : profiles) {
                assertThat(profile.description(), equalTo(DataNodeFetchExecutor.DESCRIPTION));
            }
            assertThat(fetched.info().planProfiles(), hasSize(1));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * Opens a context on every shard the way a data node request does, finds where each document is, and responds, so
     * the contexts stay open for the fetch.
     */
    private List<OpenShard> openEveryShard() throws IOException {
        NodeFetchContexts contexts = newNodeContexts();
        List<SearchContext> opened = openEveryShard(contexts, FETCHED);
        List<OpenShard> shards = new ArrayList<>(opened.size());
        for (SearchContext searchContext : opened) {
            contexts.originOf(searchContext);
            shards.add(new OpenShard(searchContext.shardTarget().getShardId(), searchContext.readerContext().id(), locate(searchContext)));
        }
        contexts.responded();
        endRequest(contexts);
        return shards;
    }

    /**
     * A new index of {@code docs} documents in {@code shards} shards, indexed in a few refreshes so each shard has several
     * segments, with a context open on every shard like {@link #openEveryShard()}.
     */
    private List<OpenShard> openEveryShardOf(String index, int shards, int docs) throws IOException {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", 0))
                .setMapping("id", "type=keyword", "n", "type=long")
        );
        int refreshes = between(1, 4);
        for (int r = 0; r < refreshes; r++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = r * docs / refreshes; i < (r + 1) * docs / refreshes; i++) {
                bulk.add(prepareIndex(index).setSource("id", "doc-" + i, "n", i));
            }
            assertFalse(bulk.get().hasFailures());
        }
        NodeFetchContexts contexts = newNodeContexts();
        List<OpenShard> open = new ArrayList<>(shards);
        for (int shard = 0; shard < shards; shard++) {
            SearchContext searchContext = open(contexts, index, shard);
            contexts.originOf(searchContext);
            open.add(new OpenShard(searchContext.shardTarget().getShardId(), searchContext.readerContext().id(), locate(searchContext)));
        }
        contexts.responded();
        endRequest(contexts);
        return open;
    }

    private static List<Long> ns(int count) {
        List<Long> ns = new ArrayList<>(count);
        for (long n = 0; n < count; n++) {
            ns.add(n);
        }
        return ns;
    }

    private static Map<Long, int[]> locate(SearchContext searchContext) throws IOException {
        Map<Long, int[]> locations = new HashMap<>();
        List<LeafReaderContext> leaves = searchContext.searcher().getIndexReader().leaves();
        for (int segment = 0; segment < leaves.size(); segment++) {
            LeafReader reader = leaves.get(segment).reader();
            SortedNumericDocValues values = DocValues.getSortedNumeric(reader, "n");
            for (int doc = 0; doc < reader.maxDoc(); doc++) {
                assertTrue(values.advanceExact(doc));
                locations.put(values.nextValue(), new int[] { segment, doc });
            }
        }
        return locations;
    }

    private static List<Long> allNs() {
        List<Long> ns = new ArrayList<>(DOCS);
        for (long n = 0; n < DOCS; n++) {
            ns.add(n);
        }
        return ns;
    }

    /**
     * A request for the documents with the given {@code n}, the way a coordinator builds it: the shards in any order,
     * the documents of each shard sorted by segment and doc.
     */
    private FetchRequest request(List<OpenShard> shards, Collection<Long> ns) {
        List<FetchRequest.ShardDocs> shardDocs = new ArrayList<>();
        for (OpenShard shard : shards) {
            List<int[]> wanted = new ArrayList<>();
            for (long n : ns) {
                int[] location = shard.segmentAndDocByN().get(n);
                if (location != null) {
                    wanted.add(location);
                }
            }
            if (wanted.isEmpty()) {
                continue;
            }
            wanted.sort((a, b) -> a[0] != b[0] ? Integer.compare(a[0], b[0]) : Integer.compare(a[1], b[1]));
            int[] segments = wanted.stream().mapToInt(l -> l[0]).toArray();
            int[] docs = wanted.stream().mapToInt(l -> l[1]).toArray();
            shardDocs.add(new FetchRequest.ShardDocs(shard.shardId(), shard.contextId(), segments, docs));
        }
        shardDocs = shuffledList(shardDocs);
        return new FetchRequest(
            randomAlphaOfLength(8),
            "",
            new OriginalIndices(new String[] { FETCHED }, IndicesOptions.strictExpandOpen()),
            shardDocs,
            EsqlTestUtils.TEST_CFG,
            FetchRequestTests.fetchPlan(List.of(ID, N))
        );
    }

    /**
     * The position in the request of the shard of each driver, in the order the executor builds them: every shard of the
     * request opens and splits into its pieces, at most as many as the request runs drivers at the same time.
     */
    private static List<Integer> shardOfEachDriver(FetchRequest request, int maxConcurrentShardTasks) {
        List<Integer> shardOfDriver = new ArrayList<>();
        for (int position = 0; position < request.shards().size(); position++) {
            FetchRequest.ShardDocs shard = request.shards().get(position);
            FetchDocsSourceOperator.ShardDocs docs = new FetchDocsSourceOperator.ShardDocs(position, shard.segments(), shard.docs());
            int pieces = docs.split(maxConcurrentShardTasks).size();
            for (int piece = 0; piece < pieces; piece++) {
                shardOfDriver.add(position);
            }
        }
        return shardOfDriver;
    }

    private static FetchRequest withShards(FetchRequest request, List<FetchRequest.ShardDocs> shards) {
        return copy(request, shards, request.configuration(), request.fetchPlan());
    }

    private static FetchRequest withConfiguration(FetchRequest request, Configuration configuration) {
        return copy(request, request.shards(), configuration, request.fetchPlan());
    }

    private static FetchRequest withFetchPlan(FetchRequest request, PhysicalPlan fetchPlan) {
        return copy(request, request.shards(), request.configuration(), fetchPlan);
    }

    private static FetchRequest copy(
        FetchRequest request,
        List<FetchRequest.ShardDocs> shards,
        Configuration configuration,
        PhysicalPlan fetchPlan
    ) {
        return new FetchRequest(
            request.sessionId(),
            request.clusterAlias(),
            new OriginalIndices(request.indices(), request.indicesOptions()),
            shards,
            configuration,
            fetchPlan
        );
    }

    /**
     * The rows the request asks for, in its order: the shards as listed, the documents of each shard as listed.
     */
    private static List<Tuple<String, Long>> expectedRows(List<OpenShard> shards, FetchRequest request) {
        List<Tuple<String, Long>> rows = new ArrayList<>();
        for (FetchRequest.ShardDocs shardDocs : request.shards()) {
            OpenShard shard = shards.stream().filter(s -> s.shardId().equals(shardDocs.shardId())).findFirst().orElseThrow();
            for (int i = 0; i < shardDocs.docCount(); i++) {
                int segment = shardDocs.segments()[i];
                int doc = shardDocs.docs()[i];
                long n = shard.segmentAndDocByN()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getValue()[0] == segment && e.getValue()[1] == doc)
                    .findFirst()
                    .orElseThrow()
                    .getKey();
                rows.add(Tuple.tuple("doc-" + n, n));
            }
        }
        return rows;
    }

    private static List<Tuple<String, Long>> rows(List<Page> pages) {
        List<Tuple<String, Long>> rows = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            BytesRefBlock ids = page.getBlock(0);
            LongBlock ns = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                rows.add(
                    Tuple.tuple(ids.getBytesRef(ids.getFirstValueIndex(p), scratch).utf8ToString(), ns.getLong(ns.getFirstValueIndex(p)))
                );
            }
        }
        return rows;
    }

    private Fetched fetch(FetchRequest request) {
        return fetch(request, newTask(), between(1, 10));
    }

    private Fetched fetch(FetchRequest request, CancellableTask task, int maxConcurrentShardTasks) {
        return fetch(request, task, maxConcurrentShardTasks, SourceWrapper.NONE);
    }

    /**
     * Runs the request on the search pool, where the transport layer runs fetch requests.
     */
    private Fetched fetch(FetchRequest request, CancellableTask task, int maxConcurrentShardTasks, SourceWrapper sources) {
        DataNodeFetchExecutor executor = new DataNodeFetchExecutor(
            getInstanceFromNode(TransportService.class),
            getInstanceFromNode(ClusterService.class),
            searchService(),
            blockFactory,
            getInstanceFromNode(ThreadPool.class).executor(ThreadPool.Names.GENERIC),
            () -> PlannerSettings.DEFAULTS,
            plannerFactory(sources),
            () -> maxConcurrentShardTasks
        );
        PlainActionFuture<Fetched> future = new PlainActionFuture<>();
        getInstanceFromNode(ThreadPool.class).executor(ThreadPool.Names.SEARCH)
            .execute(
                () -> executor.execute(request, task, future.map(r -> new Fetched(r.shardResults(), r.takePages(), r.completionInfo())))
            );
        return future.actionGet(TimeValue.timeValueSeconds(30));
    }

    /**
     * Builds planners the way a data node does, with the source of each driver wrapped by {@code sources}.
     */
    private FetchPlannerFactory plannerFactory(SourceWrapper sources) {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        Settings settings = getInstanceFromNode(ClusterService.class).getSettings();
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        return (sessionId, clusterAlias, task, configuration, foldCtx, shardContexts, fetchSources) -> new LocalExecutionPlanner(
            sessionId,
            clusterAlias,
            task,
            bigArrays,
            blockFactory,
            settings,
            configuration,
            () -> {
                throw new AssertionError("a fetch plan reads no exchange");
            },
            () -> { throw new AssertionError("a fetch plan writes no exchange"); },
            null,
            null,
            null,
            null,
            null,
            null,
            ProjectMetadata.builder(randomProjectIdOrDefault()).build(),
            new EsPhysicalOperationProviders(
                foldCtx,
                shardContexts,
                indicesService.getAnalysis(),
                PlannerSettings.DEFAULTS,
                () -> 0L,
                QueryWarnings.EMIT
            ),
            null,
            randomBoolean(),
            PlannerServices.forFetchPlan(wrap(fetchSources, sources)),
            null,
            0,
            MatcherWatchdog.noop(),
            TransportVersion.current()
        );
    }

    /**
     * Wraps the source of each driver in the order the planner builds them, which is the order of the open shards.
     */
    private static FetchSourceProvider wrap(FetchSourceProvider fetchSources, SourceWrapper sources) {
        AtomicInteger built = new AtomicInteger();
        return (exec, maxPageSize) -> {
            FetchSourceProvider.FetchSource fetchSource = fetchSources.fetchSource(exec, maxPageSize);
            SourceOperator.SourceOperatorFactory factory = new SourceOperator.SourceOperatorFactory() {
                @Override
                public SourceOperator get(DriverContext driverContext) {
                    return sources.wrap(built.getAndIncrement(), fetchSource.factory().get(driverContext));
                }

                @Override
                public String describe() {
                    return fetchSource.factory().describe();
                }
            };
            return new FetchSourceProvider.FetchSource(factory, fetchSource.drivers());
        };
    }

    /**
     * Changes the source of a fetch driver, by its position among the drivers of the request.
     */
    @FunctionalInterface
    private interface SourceWrapper {
        SourceWrapper NONE = (driver, source) -> source;

        SourceOperator wrap(int driver, SourceOperator source);
    }

    /**
     * Passes every call to the source it wraps, so a test can change one of them.
     */
    private static class DelegatingSource extends SourceOperator {
        private final SourceOperator delegate;

        DelegatingSource(SourceOperator delegate) {
            this.delegate = delegate;
        }

        @Override
        public void finish() {
            delegate.finish();
        }

        @Override
        public boolean isFinished() {
            return delegate.isFinished();
        }

        @Override
        public Page getOutput() {
            return delegate.getOutput();
        }

        @Override
        public Operator.Status status() {
            return delegate.status();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static CancellableTask newTask() {
        return new CancellableTask(
            randomNonNegativeLong(),
            "transport",
            FetchService.FETCH_ACTION_NAME,
            "",
            new TaskId("coordinator", randomNonNegativeLong()),
            Map.of()
        );
    }

    private void freeAndAssertAllFreed(List<OpenShard> shards) throws Exception {
        for (OpenShard shard : shards) {
            searchService().freeReaderContext(shard.contextId());
        }
        assertAllFreed();
    }

    private static Attribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
