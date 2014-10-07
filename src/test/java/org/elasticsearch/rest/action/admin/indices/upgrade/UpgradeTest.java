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

import com.google.common.base.Predicate;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.rest.client.RestResponse;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.test.rest.json.JsonPath;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class UpgradeTest extends ElasticsearchBackwardsCompatIntegrationTest {

    @Override
    protected int minExternalNodes() {
        return 2;
    }

    public void testUpgrade() throws Exception {
        if (backwardsCluster().numNewDataNodes() == 0) {
            backwardsCluster().startNewNode();
        }
        
        int numIndexes = randomIntBetween(2, 4);
        String[] indexNames = new String[numIndexes];
        for (int i = 0; i < numIndexes; ++i) {
            String indexName = "test" + i;
            indexNames[i] = indexName;
            
            Settings settings = ImmutableSettings.builder()
                .put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern())
                // don't allow any merges so that we can check segments are upgraded
                // by the upgrader, and not just regular merging
                .put("index.merge.policy.segments_per_tier", 1000000f)
                .put(indexSettings())
                .build();

            assertAcked(prepareCreate(indexName).setSettings(settings));
            ensureGreen(indexName);
            assertAllShardsOnNodes(indexName, backwardsCluster().backwardsNodePattern());

            int numDocs = scaledRandomIntBetween(100, 1000);
            List<IndexRequestBuilder> builder = new ArrayList<>();
            for (int j = 0; j < numDocs; ++j) {
                String id = Integer.toString(j);
                builder.add(client().prepareIndex(indexName, "type1", id).setSource("text", "sometext"));
            }
            indexRandom(true, builder);
            ensureGreen(indexName);
            flushAndRefresh();
        }
        backwardsCluster().allowOnAllNodes(indexNames);
        backwardsCluster().upgradeAllNodes();
        ensureGreen();

        checkNotUpgraded("/_upgrade");
        final String indexToUpgrade = "test" + randomInt(numIndexes - 1);
        
        runUpgrade("/" + indexToUpgrade + "/_upgrade");
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                try {
                    return isUpgraded("/" + indexToUpgrade + "/_upgrade");
                } catch (Exception e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        });
        
        runUpgrade("/_upgrade", "wait_for_completion", "true");
        checkUpgraded("/_upgrade");
    }
    
    void checkNotUpgraded(String path) throws Exception {
        for (UpgradeStatus status : getUpgradeStatus(path)) {
            assertTrue("index " + status.indexName + " should not be zero sized", status.totalBytes != 0);
            assertTrue("total bytes must be >= upgrade bytes", status.totalBytes >= status.toUpgradeBytes);
            assertEquals("index " + status.indexName + " should need upgrading",
                         status.totalBytes, status.toUpgradeBytes);
        }
    }

    void checkUpgraded(String path) throws Exception {
        for (UpgradeStatus status : getUpgradeStatus(path)) {
            assertTrue("index " + status.indexName + " should not be zero sized", status.totalBytes != 0);
            assertTrue("total bytes must be >= upgrade bytes", status.totalBytes >= status.toUpgradeBytes);
            assertEquals("index " + status.indexName + " should need upgrading",
                0, status.toUpgradeBytes);
        }
    }
    
    boolean isUpgraded(String path) throws Exception {
        int toUpgrade = 0;
        for (UpgradeStatus status : getUpgradeStatus(path)) {
            logger.info("Index: " + status.indexName + ", total: " + status.totalBytes + ", toUpgrade: " + status.toUpgradeBytes);
            toUpgrade += status.toUpgradeBytes;
        }
        return toUpgrade == 0;
    }

    class UpgradeStatus {
        public final String indexName;
        public final int totalBytes;
        public final int toUpgradeBytes;
        
        public UpgradeStatus(String indexName, int totalBytes, int toUpgradeBytes) {
            this.indexName = indexName;
            this.totalBytes = totalBytes;
            this.toUpgradeBytes = toUpgradeBytes;
        }
    }
    
    void runUpgrade(String path, String... params) throws Exception {
        assert params.length % 2 == 0;
        HttpRequestBuilder builder = httpClient().method("POST").path(path);
        for (int i = 0; i < params.length; i += 2) {
            builder.addParam(params[i], params[i + 1]);
        }
        HttpResponse rsp = builder.execute();
        assertNotNull(rsp);
        assertEquals(200, rsp.getStatusCode());
    }

    List<UpgradeStatus> getUpgradeStatus(String path) throws Exception {
        HttpResponse rsp = httpClient().method("GET").path(path).execute();
        Map<String,Object> data = validateAndParse(rsp);
        List<UpgradeStatus> ret = new ArrayList<>();
        for (String index : data.keySet()) {
            Map<String, Object> status = (Map<String,Object>)data.get(index);
            assertTrue("missing key size_in_bytes for index " + index, status.containsKey("size_in_bytes"));
            Object totalBytes = status.get("size_in_bytes");
            assertTrue("size_in_bytes for index " + index + " is not an integer", totalBytes instanceof Integer);
            assertTrue("missing key size_to_upgrade_in_bytes for index " + index, status.containsKey("size_to_upgrade_in_bytes"));
            Object toUpgradeBytes = status.get("size_to_upgrade_in_bytes");
            assertTrue("size_to_upgrade_in_bytes for index " + index + " is not an integer", toUpgradeBytes instanceof Integer);
            ret.add(new UpgradeStatus(index, ((Integer)totalBytes).intValue(), ((Integer)toUpgradeBytes).intValue()));
        }
        return ret;
    }
    
    Map<String, Object> validateAndParse(HttpResponse rsp) throws Exception {
        assertNotNull(rsp);
        assertEquals(200, rsp.getStatusCode());
        assertTrue(rsp.hasBody());
        return (Map<String,Object>)new JsonPath(rsp.getBody()).evaluate("");
    }
    
    HttpRequestBuilder httpClient() {
        InetSocketAddress[] addresses = cluster().httpAddresses();
        InetSocketAddress address = addresses[randomInt(addresses.length - 1)];
        return new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(InternalNode.HTTP_ENABLED, true).build();
    }
}
