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

package org.elasticsearch.rest.action.admin.indices.upgrade;

import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@SuppressCodecs({"Lucene3x", "MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom", "Lucene40", "Lucene41", "Appending", "Lucene42", "Lucene45", "Lucene46", "Lucene49"})
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, minNumDataNodes = 0, maxNumDataNodes = 0)
public class UpgradeReallyOldIndexTest extends ElasticsearchIntegrationTest {

    // this can maybe go into  ElasticsearchIntegrationTest
    public File prepareBackwardsDataDir(File backwardsIndex) throws IOException {
        File dataDir = new File(newTempDir(), "data");
        TestUtil.unzip(backwardsIndex, dataDir.getParentFile());
        assertTrue(dataDir.exists());
        String[] list = dataDir.list();
        if (list == null || list.length > 1) {
            throw new IllegalStateException("Backwards index must contain exactly one cluster");
        }
        File src = new File(dataDir, list[0]);
        File dest = new File(dataDir, internalCluster().getClusterName());
        assertTrue(src.exists());
        src.renameTo(dest);
        assertFalse(src.exists());
        assertTrue(dest.exists());
        return dataDir;
    }

    public void testUpgrade_0_20() throws Exception {
        // If this assert trips it means we are not suppressing enough codecs up above:
        assertFalse("test infra is broken!", LuceneTestCase.OLD_FORMAT_IMPERSONATION_IS_ACTIVE);
        File dataDir = prepareBackwardsDataDir(new File(getClass().getResource("index-0.20.zip").toURI()));
        internalCluster().startNode(ImmutableSettings.builder()
                .put("path.data", dataDir.getPath())
                .put("gateway.type", "local") // this is important we need to recover from gateway
                .put(InternalNode.HTTP_ENABLED, true)
                .build());
        ensureGreen("test");
       
        assertIndexSanity();
        
        HttpRequestBuilder httpClient = httpClient();

        UpgradeTest.assertNotUpgraded(httpClient, "test");
        UpgradeTest.runUpgrade(httpClient, "test", "wait_for_completion", "true");
        UpgradeTest.assertUpgraded(httpClient, "test");
    }
    
    void assertIndexSanity() {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().get();
        logger.info("Found indices: {}", Arrays.toString(getIndexResponse.indices()));
        assertEquals(1, getIndexResponse.indices().length);
        assertEquals("test", getIndexResponse.indices()[0]);
        ensureYellow("test");
        SearchResponse test = client().prepareSearch("test").get();
        assertThat(test.getHits().getTotalHits(), greaterThanOrEqualTo(1l));
    }

    static HttpRequestBuilder httpClient() {
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
        assertEquals(1, nodes.length);
        return nodes[0];
    }
}
