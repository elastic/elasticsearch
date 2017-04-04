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
package org.elasticsearch.index.store;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2,
    supportsDedicatedMasters = false, numClientNodes = 1, transportClientRatio = 0.0)
public class ExceptionRetryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected void beforeIndexDeletion() {
        // a write operation might still be in flight when the test has finished
        // so we should not check the operation counter here
    }

    /**
     * Tests retry mechanism when indexing. If an exception occurs when indexing then the indexing request is tried again before finally
     * failing. If auto generated ids are used this must not lead to duplicate ids
     * see https://github.com/elastic/elasticsearch/issues/8788
     */
    public void testRetryDueToExceptionOnNetworkLayer() throws ExecutionException, InterruptedException, IOException {
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        int numDocs = scaledRandomIntBetween(100, 1000);
        Client client = internalCluster().coordOnlyNodeClient();
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        NodeStats unluckyNode = randomFrom(nodeStats.getNodes().stream().filter((s) -> s.getNode().isDataNode())
            .collect(Collectors.toList()));
        assertAcked(client().admin().indices().prepareCreate("index").setSettings(Settings.builder()
            .put("index.number_of_replicas", 1)
            .put("index.number_of_shards", 5)));
        ensureGreen("index");
        logger.info("unlucky node: {}", unluckyNode.getNode());
        //create a transport service that throws a ConnectTransportException for one bulk request and therefore triggers a retry.
        for (NodeStats dataNode : nodeStats.getNodes()) {
            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class,
                dataNode.getNode().getName()));
            mockTransportService.addDelegate(internalCluster().getInstance(TransportService.class, unluckyNode.getNode().getName()),
                new MockTransportService.DelegateTransport(mockTransportService.original()) {
                @Override
                protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                           TransportRequestOptions options) throws IOException {
                    super.sendRequest(connection, requestId, action, request, options);
                    if (action.equals(TransportShardBulkAction.ACTION_NAME) && exceptionThrown.compareAndSet(false, true)) {
                        logger.debug("Throw ConnectTransportException");
                        throw new ConnectTransportException(connection.getNode(), action);
                    }
                }
            });
        }

        BulkRequestBuilder bulkBuilder = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder doc = null;
            doc = jsonBuilder().startObject().field("foo", "bar").endObject();
            bulkBuilder.add(client.prepareIndex("index", "type").setSource(doc));
        }

        BulkResponse response = bulkBuilder.get();
        if (response.hasFailures()) {
            for (BulkItemResponse singleIndexRespons : response.getItems()) {
                if (singleIndexRespons.isFailed()) {
                    fail("None of the bulk items should fail but got " + singleIndexRespons.getFailureMessage());
                }
            }
        }

        refresh();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(numDocs * 2).addStoredField("_id").get();

        Set<String> uniqueIds = new HashSet();
        long dupCounter = 0;
        boolean found_duplicate_already = false;
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            if (!uniqueIds.add(searchResponse.getHits().getHits()[i].getId())) {
                if (!found_duplicate_already) {
                    SearchResponse dupIdResponse = client().prepareSearch("index").setQuery(termQuery("_id",
                        searchResponse.getHits().getHits()[i].getId())).setExplain(true).get();
                    assertThat(dupIdResponse.getHits().getTotalHits(), greaterThan(1L));
                    logger.info("found a duplicate id:");
                    for (SearchHit hit : dupIdResponse.getHits()) {
                        logger.info("Doc {} was found on shard {}", hit.getId(), hit.getShard().getShardId());
                    }
                    logger.info("will not print anymore in case more duplicates are found.");
                    found_duplicate_already = true;
                }
                dupCounter++;
            }
        }
        assertSearchResponse(searchResponse);
        assertThat(dupCounter, equalTo(0L));
        assertHitCount(searchResponse, numDocs);
        IndicesStatsResponse index = client().admin().indices().prepareStats("index").clear().setSegments(true).get();
        IndexStats indexStats = index.getIndex("index");
        long maxUnsafeAutoIdTimestamp = Long.MIN_VALUE;
        for (IndexShardStats indexShardStats : indexStats) {
            for (ShardStats shardStats : indexShardStats) {
                SegmentsStats segments = shardStats.getStats().getSegments();
                maxUnsafeAutoIdTimestamp = Math.max(maxUnsafeAutoIdTimestamp, segments.getMaxUnsafeAutoIdTimestamp());
            }
        }
        assertTrue("exception must have been thrown otherwise setup is broken", exceptionThrown.get());
        assertTrue("maxUnsafeAutoIdTimestamp must be > than 0 we have at least one retry", maxUnsafeAutoIdTimestamp > -1);
    }
}
