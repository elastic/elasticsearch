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
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;


@ESIntegTestCase.ClusterScope(scope = TEST)
public class ReindexResilientSearchIT extends ReindexTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

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
            .setMapping(jsonBuilder()
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
            .mapToObj(i -> client().prepareIndex("test").setId(String.valueOf(i)).setSource("data", i)).collect(Collectors.toList()));

        ensureGreen("test");

        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("test").setSourceBatchSize(1);
        reindexRequest.setDestIndex("dest");
        reindexRequest.setRequestsPerSecond(0.3f); // 30 seconds minimum
        reindexRequest.setAbortOnVersionConflict(false);
        if (randomBoolean()) {
            reindexRequest.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        }
        StartReindexTaskAction.Request request = new StartReindexTaskAction.Request(reindexRequest, false);
        StartReindexTaskAction.Response response = client().execute(StartReindexTaskAction.INSTANCE, request).get();

        TaskId taskId = new TaskId(response.getEphemeralTaskId());
        String persistentTaskId = response.getPersistentTaskId();

        Set<String> reindexNodeNames = Arrays.stream(internalCluster().getNodeNames())
            .map(name -> Tuple.tuple(internalCluster().getInstance(NodeClient.class, name).getLocalNodeId(), name))
            .filter(idAndName -> taskId.getNodeId().equals(idAndName.v1())).map(Tuple::v2).collect(Collectors.toSet());

        assertEquals(1, reindexNodeNames.size());
        String notToRestart = reindexNodeNames.iterator().next();

        internalCluster().getInstances(NodeClient.class);
        assertBusy(() -> {
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("test").execute().actionGet();
            assertThat(Stream.of(indicesStatsResponse.getIndex("test").getShards())
                    .mapToLong(shardStat -> shardStat.getStats().search.getTotal().getScrollCurrent()).sum(),
                Matchers.equalTo((long) shardCount));
            // wait for all initial search to complete, since we do not retry those.
            assertThat(client().admin().cluster().prepareListTasks().setActions(SearchAction.NAME).get().getTasks(), Matchers.empty());
        }, 30, TimeUnit.SECONDS);

        Set<String> restartableNodes = internalCluster().nodesInclude("test")
            .stream().filter(id -> id.equals(notToRestart) == false).collect(Collectors.toSet());
        for (int i = 0; i < randomIntBetween(1,5); ++i) {
            String node1ToRestart = randomFrom(restartableNodes);
            internalCluster().restartNode(node1ToRestart, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    if (restartableNodes.size() > 1) {
                        String node2ToRestart = randomValueOtherThan(node1ToRestart, () -> randomFrom(restartableNodes));
                        logger.info("--> restarting second node: " + node2ToRestart);
                        internalCluster().restartNode(node2ToRestart, new InternalTestCluster.RestartCallback());
                    }
                    return super.onNodeStopped(nodeName);
                }
            });
        }

        if (randomBoolean()) {
            ensureYellow(".reindex");
            client().execute(RethrottlePersistentReindexAction.INSTANCE, new RethrottlePersistentReindexAction.Request(persistentTaskId,
                Float.POSITIVE_INFINITY)).actionGet();
        } else {
            rethrottle().setTaskId(taskId)
                .setRequestsPerSecond(Float.POSITIVE_INFINITY).execute().get();
        }

        Map<String, Object> reindexResponse = client().admin().cluster().prepareGetTask(taskId).setWaitForCompletion(true)
            .get(TimeValue.timeValueSeconds(30)).getTask().getResponseAsMap();
        // todo: this assert fails sometimes due to missing retry on transport closed
//        assertThat(bulkByScrollResponse.getBulkFailures(), Matchers.empty());
        assertEquals(Collections.emptyList(), reindexResponse.get("failures"));

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
        assertEquals("First bucket must have a doc per index", indices.length, termsAggregation.getBuckets().get(0).getDocCount());
        // grouping by unique field data, we are sure that no bucket has more than #indices docs, thus above is enough to check that we
        // have the same docs.
    }
}
