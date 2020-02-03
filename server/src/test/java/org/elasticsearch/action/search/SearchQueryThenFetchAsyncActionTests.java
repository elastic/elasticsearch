package org.elasticsearch.action.search;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class SearchQueryThenFetchAsyncActionTests extends ESTestCase {
    public void testSearchBefore() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        int numShards = randomIntBetween(10, 20);
        int numConcurrent = randomIntBetween(1, 4);
        AtomicInteger numWithTopDocs = new AtomicInteger();
        AtomicInteger successfulOps = new AtomicInteger();
        SearchTransportService searchTransportService = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteQuery(Transport.Connection connection, ShardSearchRequest request, SearchTask task, SearchActionListener<SearchPhaseResult> listener) {
                int shardId = request.shardId().id();
                if (request.getRawBottomSortValues() != null) {
                    assertNotEquals(shardId, (int) request.getRawBottomSortValues()[0]);
                    numWithTopDocs.incrementAndGet();
                }
                QuerySearchResult queryResult = new QuerySearchResult(123,
                    new SearchShardTarget("node1", new ShardId("test", "na", shardId), null, OriginalIndices.NONE));
                SortField sortField = new SortField("timestamp", SortField.Type.LONG);
                queryResult.topDocs(new TopDocsAndMaxScore(new TopFieldDocs(
                    new TotalHits(1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                    new FieldDoc[] {
                        new FieldDoc(randomInt(1000), Float.NaN, new Object[] { request.shardId().id() })
                    }, new SortField[] { sortField }), Float.NaN),
                    new DocValueFormat[] { DocValueFormat.RAW });
                queryResult.from(0);
                queryResult.size(1);
                successfulOps.incrementAndGet();
                new Thread(() -> listener.onResponse(queryResult)).start();
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards, randomBoolean(), primaryNode, replicaNode);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.setMaxConcurrentShardRequests(numConcurrent);
        searchRequest.setBatchedReduceSize(2);
        searchRequest.source(new SearchSourceBuilder()
            .size(1)
            .trackTotalHitsUpTo(1)
            .sort(SortBuilders.fieldSort("timestamp")));
        searchRequest.allowPartialSearchResults(false);
        SearchPhaseController controller = new SearchPhaseController((b) -> new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, b));
        SearchTask task = new SearchTask(0, "n/a", "n/a", "test", null, Collections.emptyMap());
        SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(logger,
            searchTransportService, (clusterAlias, node) -> lookup.get(node), Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(), Collections.emptyMap(), controller, EsExecutors.newDirectExecutorService(), searchRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        throw new AssertionError(e);
                    } finally {
                        latch.countDown();
                    }
                }
            }, shardsIter, timeProvider, 0, task,
            SearchResponse.Clusters.EMPTY);
        action.start();
        latch.await();
        assertThat(successfulOps.get(), equalTo(numShards));
        assertThat(numWithTopDocs.get(), greaterThanOrEqualTo(1));
        SearchPhaseController.ReducedQueryPhase phase = action.results.reduce();
        assertThat(phase.numReducePhases, greaterThanOrEqualTo(1));
        assertThat(phase.totalHits.value, equalTo(1L));
        assertThat(phase.totalHits.relation, equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
        assertThat(phase.sortedTopDocs.scoreDocs.length, equalTo(1));
        assertThat(phase.sortedTopDocs.scoreDocs[0], instanceOf(FieldDoc.class));
        assertThat(((FieldDoc) phase.sortedTopDocs.scoreDocs[0]).fields.length, equalTo(1));
        assertThat(((FieldDoc) phase.sortedTopDocs.scoreDocs[0]).fields[0], equalTo(0));

    }
}
