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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class ExceptionRetryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected void beforeIndexDeletion() {
        // a write operation might still be in flight when the test has finished
        // so we should not check the operation counter here 
    }

    /**
     * Tests retry mechanism when indexing. If an exception occurs when indexing then the indexing request is tried again before finally failing.
     * If auto generated ids are used this must not lead to duplicate ids
     * see https://github.com/elasticsearch/elasticsearch/issues/8788
     */
    @Test
    public void testRetryDueToExceptionOnNetworkLayer() throws ExecutionException, InterruptedException, IOException {
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        int numDocs = scaledRandomIntBetween(100, 1000);
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        NodeStats unluckyNode = randomFrom(nodeStats.getNodes());
        assertAcked(client().admin().indices().prepareCreate("index"));
        ensureGreen("index");

        //create a transport service that throws a ConnectTransportException for one bulk request and therefore triggers a retry.
        for (NodeStats dataNode : nodeStats.getNodes()) {
            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class, dataNode.getNode().name()));
            mockTransportService.addDelegate(internalCluster().getInstance(Discovery.class, unluckyNode.getNode().name()).localNode(), new MockTransportService.DelegateTransport(mockTransportService.original()) {

                @Override
                public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                    super.sendRequest(node, requestId, action, request, options);
                    if (action.equals(TransportShardBulkAction.ACTION_NAME) && !exceptionThrown.get()) {
                        logger.debug("Throw ConnectTransportException");
                        exceptionThrown.set(true);
                        throw new ConnectTransportException(node, action);
                    }
                }
            });
        }

        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder doc = null;
            doc = jsonBuilder().startObject().field("foo", "bar").endObject();
            bulkBuilder.add(client().prepareIndex("index", "type").setSource(doc));
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
        SearchResponse searchResponse = client().prepareSearch("index").setSize(numDocs * 2).addField("_id").get();

        Set<String> uniqueIds = new HashSet();
        long dupCounter = 0;
        boolean found_duplicate_already = false;
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            if (!uniqueIds.add(searchResponse.getHits().getHits()[i].getId())) {
                if (!found_duplicate_already) {
                    SearchResponse dupIdResponse = client().prepareSearch("index").setQuery(termQuery("_id", searchResponse.getHits().getHits()[i].getId())).setExplain(true).get();
                    assertThat(dupIdResponse.getHits().totalHits(), greaterThan(1l));
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
        assertThat(dupCounter, equalTo(0l));
        assertHitCount(searchResponse, numDocs);
    }
}
