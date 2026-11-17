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
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.ComputeExecutor;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The data node half of a fetch, on a real {@code SearchService} with the reader contexts the query phase keeps open.
 * {@code MockSearchService} fails a test that leaves a context open, and every test ends with an empty request breaker.
 */
public class FetchServiceTests extends FetchContextsTestCase {
    private static final String FETCHED = "fetched";
    private static final int DOCS = 40;
    private static final Attribute ID = field("id", DataType.KEYWORD);
    private static final Attribute N = field("n", DataType.LONG);

    private BlockFactory blockFactory;

    /**
     * The open context of one shard and where each document of the shard is, by its {@code n}.
     */
    private record OpenShard(ShardId shardId, ShardSearchContextId contextId, Map<Long, int[]> segmentAndDocByN) {}

    private record Fetched(List<Page> pages, DriverCompletionInfo info) implements Releasable {
        @Override
        public void close() {
            Releasables.closeExpectNoException(pages);
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
        // a few refreshes, so the shards have several segments
        int refreshes = between(1, 4);
        for (int r = 0; r < refreshes; r++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = r * DOCS / refreshes; i < (r + 1) * DOCS / refreshes; i++) {
                bulk.add(prepareIndex(FETCHED).setSource("id", "doc-" + i, "n", i));
            }
            assertFalse(bulk.get().hasFailures());
        }
    }

    @After
    public void assertTheBreakerIsEmpty() throws Exception {
        // the request drops its references after the response, on the pool that frees search contexts
        assertBusy(() -> assertThat(blockFactory.breaker().getUsed(), equalTo(0L)));
    }

    /**
     * One driver loads the documents of every shard, and the rows follow the request: the shards in request order, and
     * the documents of each shard in the order the request listed them.
     */
    public void testFetchesTheRowsOfEveryShardInRequestOrder() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, randomSubsetOf(between(1, DOCS), allNs()));

        try (Fetched fetched = fetch(request)) {
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, request)));
            assertThat(fetched.info().driverProfiles(), hasSize(0));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * Once it answered, the node frees the contexts the request reads for the last time, and keeps the ones the
     * coordinator reads again.
     */
    public void testFreesTheContextsOfTheirLastFetchAfterTheResponse() throws Exception {
        List<OpenShard> shards = openEveryShard();
        Set<ShardSearchContextId> lastReads = Set.copyOf(randomSubsetOf(between(1, SHARDS - 1), contextIds(shards)));
        FetchRequest request = request(shards, allNs(), lastReads);

        fetch(request).close();

        awaitActiveContexts(SHARDS - lastReads.size());
        for (OpenShard shard : shards) {
            assertThat(
                "read again " + shard.contextId(),
                isOpen(shard.contextId()),
                equalTo(lastReads.contains(shard.contextId()) == false)
            );
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * A context that is gone, for example because its keep-alive passed, fails the whole request, like a fetch request of
     * {@code _search}. The request frees only the contexts of the shards it opened before, so a request frees only
     * contexts it was allowed to read.
     */
    public void testAMissingContextFailsTheRequest() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs(), Set.copyOf(contextIds(shards)));
        int missing = between(0, request.shards().size() - 1);
        List<FetchRequest.ShardDocs> withMissing = new ArrayList<>(request.shards());
        FetchRequest.ShardDocs shard = withMissing.get(missing);
        ShardSearchContextId gone = new ShardSearchContextId(shard.contextId().getSessionId(), Long.MAX_VALUE);
        withMissing.set(missing, new FetchRequest.ShardDocs(shard.shardId(), gone, true, shard.segments(), shard.docs()));

        Exception e = expectThrows(Exception.class, () -> fetch(withShards(request, withMissing)));
        assertThat(e, instanceOf(SearchContextMissingException.class));

        // the request frees the contexts of the shards it opened before the missing one
        awaitActiveContexts(SHARDS - missing);
        for (int position = 0; position < withMissing.size(); position++) {
            if (position != missing) {
                ShardSearchContextId contextId = withMissing.get(position).contextId();
                assertThat("opened after the missing one " + contextId, isOpen(contextId), equalTo(position > missing));
            }
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * A request may name the aliases the query reached its shards through, like the shard requests of {@code _search}.
     */
    public void testFetchesThroughAnAlias() throws Exception {
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(FETCHED, "fetched-alias"));
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = withIndices(request(shards, randomSubsetOf(between(1, DOCS), allNs())), "fetched-alias");

        try (Fetched fetched = fetch(request)) {
            assertThat(rows(fetched.pages()), equalTo(expectedRows(shards, request)));
        }
        freeAndAssertAllFreed(shards);
    }

    /**
     * Security authorizes a request for the indices it names, as they resolve when it arrives. The searcher of a shard of
     * another index would read it without the document and field level security of that index, for example once an alias
     * the query read through moved to other indices. Such a shard fails the request as a whole, before it opens a shard.
     */
    public void testRefusesAShardOutsideTheIndicesItNames() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("moved-to")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("moved-to", "moved"));
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = withIndices(request(shards, allNs(), Set.copyOf(contextIds(shards))), "moved");

        Exception e = expectThrows(Exception.class, () -> fetch(request));
        assertThat(e, instanceOf(IllegalArgumentException.class));
        assertThat(e.getMessage(), containsString("isn't in the indices [moved] of the fetch request"));

        awaitTheRelease();
        assertThat("the request opened no shard, so it freed none", searchService().getActiveContexts(), equalTo(SHARDS));
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
            // a request may not free a point in time it learned the id of either
            FetchRequest.ShardDocs docs = new FetchRequest.ShardDocs(
                shardId,
                pointInTime,
                randomBoolean(),
                new int[] { 0 },
                new int[] { 0 }
            );

            Exception e = expectThrows(Exception.class, () -> fetch(withShards(request(List.of(), List.of()), List.of(docs))));
            assertThat(e, instanceOf(SearchContextMissingException.class));
            awaitTheRelease();
            assertThat("the point in time stays open", searchService().getActiveContexts(), equalTo(1));
        } finally {
            searchService().freeReaderContext(pointInTime);
        }
        assertAllFreed();
    }

    /**
     * A document outside the reader of its context would fail deep in the loaders or load another document.
     */
    public void testADocumentOutsideTheReaderFailsTheRequest() throws Exception {
        List<OpenShard> shards = openEveryShard();
        OpenShard shard = randomFrom(shards);
        FetchRequest.ShardDocs outside = new FetchRequest.ShardDocs(
            shard.shardId(),
            shard.contextId(),
            false,
            new int[] { 0, 10_000 },
            new int[] { 0, 0 }
        );
        FetchRequest request = withShards(request(shards, List.of()), List.of(outside));

        Exception e = expectThrows(Exception.class, () -> fetch(request));
        assertThat(e, instanceOf(IllegalArgumentException.class));
        assertThat(e.getMessage(), containsString("isn't in the reader of"));
        freeAndAssertAllFreed(shards);
    }

    /**
     * A cancelled request opens no search context and loads nothing. The contexts stay with their coordinator.
     */
    public void testACancelledRequestLoadsNothing() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs(), Set.copyOf(contextIds(shards)));
        CancellableTask task = newTask();
        TaskCancelHelper.cancel(task, "the query was cancelled");

        Exception e = expectThrows(Exception.class, () -> fetch(request, task));
        assertThat(e, instanceOf(TaskCancelledException.class));
        awaitTheRelease();
        assertThat("the contexts stay open", searchService().getActiveContexts(), equalTo(SHARDS));
        freeAndAssertAllFreed(shards);
    }

    /**
     * A profiled query gets the profile of the one {@code fetch} driver of the request.
     */
    public void testProfileListsTheFetchDriver() throws Exception {
        List<OpenShard> shards = openEveryShard();
        FetchRequest request = request(shards, allNs());
        FetchRequest profiled = withConfiguration(request, new ConfigurationBuilder(request.configuration()).profile(true).build());

        try (Fetched fetched = fetch(profiled)) {
            List<DriverProfile> profiles = fetched.info().driverProfiles();
            assertThat(profiles, hasSize(1));
            assertThat(profiles.getFirst().description(), equalTo(FetchService.DESCRIPTION));
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
        endRequest(contexts);
        return shards;
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

    /**
     * Waits until the node holds {@code expected} contexts, once the request freed the ones it should. Idle pools don't
     * prove the frees ran: an idle worker can take the free task straight from the queue of its pool, and for a moment
     * the pool shows neither an active nor a queued task.
     */
    private void awaitActiveContexts(int expected) throws Exception {
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(expected)));
    }

    /**
     * Waits until the search and generic pools are idle, before a test checks that a request freed nothing. The thread
     * that answered schedules the release on the generic pool, through the executor that frees search contexts. This is
     * best effort: idle pools don't prove the release ran (see {@link #awaitActiveContexts}), so a wrong free can slip
     * past a check that follows. Only a hook in {@link FetchService} would close that gap.
     */
    private void awaitTheRelease() throws Exception {
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        Set<String> pools = Set.of(ThreadPool.Names.SEARCH, ThreadPool.Names.GENERIC);
        assertBusy(() -> {
            for (ThreadPoolStats.Stats stats : threadPool.stats()) {
                if (pools.contains(stats.name())) {
                    assertThat(stats.name(), stats.active() + stats.queue(), equalTo(0));
                }
            }
        });
    }

    private static List<ShardSearchContextId> contextIds(List<OpenShard> shards) {
        return shards.stream().map(OpenShard::contextId).toList();
    }

    private static List<Long> allNs() {
        List<Long> ns = new ArrayList<>(DOCS);
        for (long n = 0; n < DOCS; n++) {
            ns.add(n);
        }
        return ns;
    }

    private FetchRequest request(List<OpenShard> shards, Collection<Long> ns) {
        return request(shards, ns, Set.of());
    }

    /**
     * A request for the documents with the given {@code n}, the way a coordinator builds it: the shards in any order,
     * the documents of each shard sorted by segment and doc.
     *
     * @param lastReads the contexts the request reads for the last time
     */
    private FetchRequest request(List<OpenShard> shards, Collection<Long> ns, Set<ShardSearchContextId> lastReads) {
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
            boolean freeAfterFetch = lastReads.contains(shard.contextId());
            shardDocs.add(new FetchRequest.ShardDocs(shard.shardId(), shard.contextId(), freeAfterFetch, segments, docs));
        }
        return new FetchRequest(
            randomAlphaOfLength(8),
            "",
            new OriginalIndices(new String[] { FETCHED }, IndicesOptions.strictExpandOpen()),
            shuffledList(shardDocs),
            EsqlTestUtils.TEST_CFG,
            List.of(ID, N)
        );
    }

    private static FetchRequest withShards(FetchRequest request, List<FetchRequest.ShardDocs> shards) {
        return copy(request, request.indices(), shards, request.configuration());
    }

    private static FetchRequest withConfiguration(FetchRequest request, Configuration configuration) {
        return copy(request, request.indices(), request.shards(), configuration);
    }

    private static FetchRequest withIndices(FetchRequest request, String... indices) {
        return copy(request, indices, request.shards(), request.configuration());
    }

    private static FetchRequest copy(
        FetchRequest request,
        String[] indices,
        List<FetchRequest.ShardDocs> shards,
        Configuration configuration
    ) {
        return new FetchRequest(
            request.sessionId(),
            request.clusterAlias(),
            new OriginalIndices(indices, request.indicesOptions()),
            shards,
            configuration,
            request.attributes()
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
        return fetch(request, newTask());
    }

    /**
     * Runs the request on the search pool, where the transport layer runs fetch requests. The node of these tests has no
     * worker pool of ES|QL, so the driver runs on the generic pool.
     */
    private Fetched fetch(FetchRequest request, CancellableTask task) {
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        FetchService service = new FetchService(
            getInstanceFromNode(TransportService.class),
            getInstanceFromNode(ClusterService.class),
            getInstanceFromNode(ProjectResolver.class),
            getInstanceFromNode(IndexNameExpressionResolver.class),
            searchService(),
            getInstanceFromNode(BigArrays.class),
            blockFactory,
            () -> PlannerSettings.DEFAULTS,
            new ComputeExecutor(threadPool.generic(), threadPool.getThreadContext())
        );
        PlainActionFuture<Fetched> future = new PlainActionFuture<>();
        threadPool.executor(ThreadPool.Names.SEARCH)
            .execute(() -> service.execute(request, task, future.map(r -> new Fetched(r.takePages(), r.completionInfo()))));
        return future.actionGet(TimeValue.timeValueSeconds(30));
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
