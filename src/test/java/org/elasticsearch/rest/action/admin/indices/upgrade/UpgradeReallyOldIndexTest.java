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
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, minNumDataNodes = 0, maxNumDataNodes = 0)
public class UpgradeReallyOldIndexTest extends ElasticsearchIntegrationTest {

    // this can maybe go into  ElasticsearchIntegrationTest
    public File prepareBackwardsDataDir(File backwardsIndex) throws IOException {
        File dataDir = new File(newTempDir(), "data");
        TestUtil.unzip(backwardsIndex, dataDir.getParentFile());
        assertThat(dataDir.exists(), is(true));
        String[] list = dataDir.list();
        if (list == null || list.length > 1) {
            throw new IllegalStateException("Backwards index must contain exactly one cluster");
        }
        File src = new File(dataDir, list[0]);
        File dest = new File(dataDir, internalCluster().getClusterName());
        assertThat(src.exists(), is(true));
        src.renameTo(dest);
        assertThat(src.exists(), is(false));
        assertThat(dest.exists(), is(true));
        return dataDir;
    }


    public void test() throws Exception {
        File dataDir = prepareBackwardsDataDir(new File(getClass().getResource("index-0.20.zip").toURI()));
        internalCluster().startNode(ImmutableSettings.builder()
                .put("path.data", dataDir.getPath())
                .put("node.mode", "network")
                .put("gateway.type", "local") // this is important we need to recover from gateway
                .put(InternalNode.HTTP_ENABLED, true)
                .build());
        NodeInfo info = nodeInfo(client());
        info.getHttp().address().boundAddress();
        TransportAddress publishAddress = info.getHttp().address().publishAddress();
        assertEquals(1, publishAddress.uniqueAddressTypeId());
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().get();
        logger.info("Found indices: {}", Arrays.toString(getIndexResponse.indices()));
        assertThat(getIndexResponse.indices().length, equalTo(1));
        assertThat(getIndexResponse.indices()[0], equalTo("test"));
        ensureYellow("test");
        SearchResponse test = client().prepareSearch("test").get();
        assertThat(test.getHits().getTotalHits(), greaterThanOrEqualTo(1l));
        InetSocketAddress address = ((InetSocketTransportAddress) publishAddress).address();
        HttpRequestBuilder httpClient = new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
        HttpResponse rsp = httpClient.method("GET").path("/_upgrade").addParam("pretty", "true").execute();
        logger.info("RESPONSE: \n" + rsp.getBody());
    }



    static NodeInfo nodeInfo(final Client client) {
        final NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
        final NodeInfo[] nodes = nodeInfos.getNodes();
        assertEquals(1, nodes.length);
        return nodes[0];
    }

    /*HttpRequestBuilder httpClient(Node ) {
        InetSocketAddress[] addresses = cluster().httpAddresses();
        InetSocketAddress address = addresses[randomInt(addresses.length - 1)];
        return new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
    }*/
}
