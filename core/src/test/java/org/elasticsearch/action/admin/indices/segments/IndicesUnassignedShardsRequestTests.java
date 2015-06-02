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

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.action.admin.indices.shards.IndicesUnassigedShardsResponse;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/*
   TODO: test exception reporting when shard index can not be opened
 */
public class IndicesUnassignedShardsRequestTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    protected int numberOfShards() {
        return 5;
    }

    @Before
    public void setupIndex() {
        prepareCreate("test", 2);
        int numDocs = scaledRandomIntBetween(100, 1000);
        for (int j = 0; j < numDocs; ++j) {
            String id = Integer.toString(j);
            client().prepareIndex("test", "type1", id).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush("test").get();
    }

    @Test
    public void testBasic() {
        ensureGreen();
        IndicesUnassigedShardsResponse rsp = client().admin().indices().prepareUnassignedShards("test").get();
        assertThat(rsp.getShardStatuses().size(), equalTo(0));
    }

    @Test
    public void testUnassignedShards() throws Exception {
        ensureGreen();
        // TODO: there has to be a better way of having unassigned shards
        internalCluster().stopRandomDataNode();
        RoutingNodes.UnassignedShards unassigned = clusterService().state().routingNodes().unassigned();
        IndicesUnassigedShardsResponse response = client().admin().indices().prepareUnassignedShards("test").get();

        ImmutableOpenMap<String, Map<Integer, List<IndicesUnassigedShardsResponse.ShardStatus>>> statuses = response.getShardStatuses();
        for (MutableShardRouting shardRouting : unassigned) {
            assertTrue(statuses.containsKey(shardRouting.getIndex()));
            Map<Integer, List<IndicesUnassigedShardsResponse.ShardStatus>> listMap = statuses.get(shardRouting.getIndex());
            assertTrue(listMap.containsKey(shardRouting.id()));
        }
        ensureYellow();
    }

    @Test
    public void testSerialization() throws Exception {
        ensureGreen();
        // TODO: there has to be a better way of having unassigned shards
        internalCluster().stopRandomDataNode();
        RoutingNodes.UnassignedShards unassigned = clusterService().state().routingNodes().unassigned();
        IndicesUnassigedShardsResponse response = client().admin().indices().prepareUnassignedShards("test").get();
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        contentBuilder.startObject();
        response.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        BytesReference bytes = contentBuilder.bytes();
        final XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(bytes);
        Map<String, Object> map = parser.mapAndClose();
        if (unassigned.size() > 0) {
            Map<String, Object> indices = (Map<String, Object>) map.get("indices");
            assertTrue(indices.containsKey("test"));
            Map<String, Object> shards = ((Map<String, Object>) ((Map<String, Object>) indices.get("test")).get("shards"));
            int nUnassignedShardsForIndex = 0;
            for (MutableShardRouting routing : unassigned) {
                if (routing.getIndex().equals("test")) {
                    assertTrue(shards.containsKey(String.valueOf(routing.id())));
                    nUnassignedShardsForIndex++;
                }
            }
            assertThat(shards.size(), equalTo(nUnassignedShardsForIndex));
        }
    }
}
