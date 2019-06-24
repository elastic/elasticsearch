/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;


@ESIntegTestCase.ClusterScope(scope = TEST)
public class ReindexResilientSearchIT extends ReindexTestCase {

    public void testDataNodeRestart() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        int shardCount = randomIntBetween(2, 10);
        createIndex("test",
            Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, shardCount)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build());

        // create index and mapping up front to avoid master involvement, since that can result in an item failing with NodeClosedException.
        assertAcked(prepareCreate("dest")
            .addMapping("_doc", jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("data")
                            .field("type", "long")
                        .endObject()
                    .endObject()
                .endObject())
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, shardCount)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build()));

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
        if (randomBoolean()) {
            request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        }
        PlainListenableActionFuture<BulkByScrollResponse> reindexFuture = PlainListenableActionFuture.newListenableFuture();
        Task reindexTask = reindexNodeClient.executeLocally(ReindexAction.INSTANCE, request, reindexFuture);

        assertBusy(() -> {
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("test").execute().actionGet();
            assertThat(Stream.of(indicesStatsResponse.getIndex("test").getShards())
                    .mapToLong(shardStat -> shardStat.getStats().search.getTotal().getScrollCurrent()).sum(),
                Matchers.equalTo((long) shardCount));
        }, 30, TimeUnit.SECONDS);

        for (int i = 0; i < randomIntBetween(1,5); ++i) {
            internalCluster().restartRandomDataNode();
        }

        rethrottle().setTaskId(new TaskId(reindexNodeClient.getLocalNodeId(), reindexTask.getId()))
            .setRequestsPerSecond(Float.POSITIVE_INFINITY).execute().get();

        BulkByScrollResponse bulkByScrollResponse = reindexFuture.actionGet(30, TimeUnit.SECONDS);
        // todo: this assert fails sometimes due to missing retry on transport closed
        assertThat(bulkByScrollResponse.getBulkFailures(), Matchers.empty());
//        assertEquals(0, bulkByScrollResponse.getSearchFailures().size());

        assertSameDocs(numberOfDocuments, "test", "dest");
    }

    private void assertSameDocs(int numberOfDocuments, String... indices) {
        refresh(indices);
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().size(0)
            .aggregation(new TermsAggregationBuilder("unique_count", ValueType.LONG)
                .field("data")
                .order(BucketOrder.count(true))
                .size(numberOfDocuments + 1)
            );

        SearchResponse searchResponse = client().search(new SearchRequest(indices).source(sourceBuilder)).actionGet();
        Terms termsAggregation = searchResponse.getAggregations().get("unique_count");
        assertEquals("Must have a bucket per doc", termsAggregation.getBuckets().size(), numberOfDocuments);
        assertEquals("First bucket must have a doc per index", termsAggregation.getBuckets().get(0).getDocCount(), indices.length);
        // grouping by unique field data, we are sure that no bucket has more than #indices docs, thus above is enough to check that we
        // have the same docs.
    }
}
