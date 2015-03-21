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

package org.elasticsearch.bwcompat;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.CompositeTestCluster;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.CoreMatchers.equalTo;

public class TransportClientBackwardsCompatibilityTest extends ElasticsearchBackwardsCompatIntegrationTest {

    @Test
    public void testSniffMode() throws ExecutionException, InterruptedException {

        Settings settings = ImmutableSettings.builder().put(requiredSettings()).put("client.transport.nodes_sampler_interval", "1s")
                .put("name", "transport_client_sniff_mode").put(ClusterName.SETTING, cluster().getClusterName())
                .put("client.transport.sniff", true).build();

        CompositeTestCluster compositeTestCluster = backwardsCluster();
        TransportAddress transportAddress = compositeTestCluster.externalTransportAddress();

        try(TransportClient client = new TransportClient(settings)) {
            client.addTransportAddress(transportAddress);

            assertAcked(client.admin().indices().prepareCreate("test"));
            ensureYellow("test");

            int numDocs = iterations(10, 100);
            IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
            for (int i = 0; i < numDocs; i++) {
                String id = "id" + i;
                indexRequestBuilders[i] = client.prepareIndex("test", "test", id).setSource("field", "value" + i);
            }
            indexRandom(false, indexRequestBuilders);

            String randomId = "id" + randomInt(numDocs-1);
            GetResponse getResponse = client.prepareGet("test", "test", randomId).get();
            assertThat(getResponse.isExists(), equalTo(true));

            refresh();

            SearchResponse searchResponse = client.prepareSearch("test").get();
            assertThat(searchResponse.getHits().totalHits(), equalTo((long)numDocs));

            int randomDocId = randomInt(numDocs-1);
            String fieldValue = "value" + randomDocId;
            String id = "id" + randomDocId;
            searchResponse = client.prepareSearch("test").setQuery(QueryBuilders.termQuery("field", fieldValue)).get();
            assertSearchHits(searchResponse, id);
        }
    }
}
