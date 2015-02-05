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

import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;

import java.io.File;
import java.net.InetSocketAddress;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * These tests are against static indexes, built from versions of ES that cannot be upgraded without
 * a full cluster restart (ie no wire format compatibility).
 */
@LuceneTestCase.SuppressCodecs({"Lucene3x", "MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom", "Lucene40", "Lucene41", "Appending", "Lucene42", "Lucene45", "Lucene46", "Lucene49"})
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, minNumDataNodes = 0, maxNumDataNodes = 0)
public class StaticIndexBackwardCompatibilityTest extends ElasticsearchIntegrationTest {

    public void loadIndex(String index, Object... settings) throws Exception {
        logger.info("Checking static index " + index);
        Settings nodeSettings = prepareBackwardsDataDir(new File(getClass().getResource(index).toURI()), settings);
        internalCluster().startNode(nodeSettings);
        ensureGreen("test");
        assertIndexSanity();
    }
    
    public void unloadIndex() throws Exception {
        ElasticsearchAssertions.assertAcked(client().admin().indices().prepareDelete("test").get());
        while (internalCluster().stopRandomDataNode()) {} // stop all data nodes
        ElasticsearchAssertions.assertAllFilesClosed();
    }

    void assertIndexSanity() {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().get();
        assertEquals(1, getIndexResponse.indices().length);
        assertEquals("test", getIndexResponse.indices()[0]);
        ensureYellow("test");
        SearchResponse test = client().prepareSearch("test").get();
        assertThat(test.getHits().getTotalHits(), greaterThanOrEqualTo(1l));
    }

    protected static HttpRequestBuilder httpClient() {
        NodeInfo info = nodeInfo(client());
        info.getHttp().address().boundAddress();
        TransportAddress publishAddress = info.getHttp().address().publishAddress();
        assertEquals(1, publishAddress.uniqueAddressTypeId());
        InetSocketAddress address = ((InetSocketTransportAddress) publishAddress).address();
        return new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
    }

    static NodeInfo nodeInfo(final Client client) {
        final NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
        final NodeInfo[] nodes = nodeInfos.getNodes();
        assertTrue(nodes.length > 0);
        return nodes[0];
    }
}
