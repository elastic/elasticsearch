package org.elasticsearch.index.reindex;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class ReindexResilientSearchIT extends ReindexTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(PainlessPlugin.class)).collect(Collectors.toList());
    }

    public void testDataNodeRestart() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        int shardCount = randomIntBetween(2, 10);
        createIndex("test",
            Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, shardCount)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build());

        int numberOfDocuments = randomIntBetween(10, 50);
        indexRandom(true, IntStream.range(0, numberOfDocuments)
            .mapToObj(i -> client().prepareIndex("test", "doc", String.valueOf(i)).setSource("data", i)).collect(Collectors.toList()));

        ensureGreen("test");
        String reindexNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        NodeClient reindexNodeClient = internalCluster().getInstance(NodeClient.class, reindexNode);


        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices("test").setSourceBatchSize(1);
        request.setDestIndex("dest");
        request.setRequestsPerSecond(0.3f); // 30 seconds minimum
        request.setAbortOnVersionConflict(false);
        PlainListenableActionFuture<BulkByScrollResponse> reindexFuture = PlainListenableActionFuture.newListenableFuture();
        Task reindexTask = reindexNodeClient.executeLocally(ReindexAction.INSTANCE, request, reindexFuture);

        assertBusy(() -> {
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("test").execute().actionGet();
            assertThat(Stream.of(indicesStatsResponse.getIndex("test").getShards()).mapToLong(shardStat -> shardStat.getStats().search.getTotal().getScrollCurrent()).sum(),
                Matchers.equalTo((long) shardCount));
        }, 30, TimeUnit.SECONDS);

        // todo: enable this once search is resilient.
//        for (int i = 0; i < randomIntBetween(1,5); ++i) {
//            internalCluster().restartRandomDataNode();
//        }

        rethrottle().setTaskId(new TaskId(reindexNodeClient.getLocalNodeId(), reindexTask.getId()))
            .setRequestsPerSecond(Float.POSITIVE_INFINITY).execute().get();

        BulkByScrollResponse bulkByScrollResponse = reindexFuture.actionGet(30, TimeUnit.SECONDS);
        assertEquals(0, bulkByScrollResponse.getBulkFailures().size());
        assertEquals(0, bulkByScrollResponse.getSearchFailures().size());

        assertSameDocs(numberOfDocuments, "test", "dest");
    }

    private void assertSameDocs(int numberOfDocuments, String... indices) {
        refresh(indices);
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().size(0)
            .aggregation(new TermsAggregationBuilder("unique_count", ValueType.STRING)
                .script(new Script("doc['_id'].value + '|' + doc['data'].value"))
                .order(BucketOrder.count(true))
                .size(numberOfDocuments + 1)
            );

        SearchResponse searchResponse = client().search(new SearchRequest(indices).source(sourceBuilder)).actionGet();
        Terms termsAggregation = searchResponse.getAggregations().get("unique_count");
        assertEquals("Must have a bucket per doc", termsAggregation.getBuckets().size(), numberOfDocuments);
        assertEquals("First bucket must have a doc per index", termsAggregation.getBuckets().get(0).getDocCount(), indices.length);
        // grouping by id, we are sure that no bucket has more than #indices docs, thus above is enough to check that we have the same docs.
    }
}
