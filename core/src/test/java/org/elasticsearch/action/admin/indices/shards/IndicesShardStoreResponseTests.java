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

package org.elasticsearch.action.admin.indices.shards;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.equalTo;

public class IndicesShardStoreResponseTests extends ESTestCase {

    @Test
    public void testBasicSerialization() throws Exception {
        ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>>> indexStoreStatuses = ImmutableOpenMap.builder();
        List<IndicesShardStoresResponse.Failure> failures = new ArrayList<>();
        ImmutableOpenIntMap.Builder<List<IndicesShardStoresResponse.StoreStatus>> storeStatuses = ImmutableOpenIntMap.builder();

        DiscoveryNode node1 = new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", DummyTransportAddress.INSTANCE, Version.CURRENT);
        List<IndicesShardStoresResponse.StoreStatus> storeStatusList = new ArrayList<>();
        storeStatusList.add(new IndicesShardStoresResponse.StoreStatus(node1, 3, IndicesShardStoresResponse.StoreStatus.Allocation.PRIMARY, null));
        storeStatusList.add(new IndicesShardStoresResponse.StoreStatus(node2, 2, IndicesShardStoresResponse.StoreStatus.Allocation.REPLICA, null));
        storeStatusList.add(new IndicesShardStoresResponse.StoreStatus(node1, 1, IndicesShardStoresResponse.StoreStatus.Allocation.UNUSED, new IOException("corrupted")));
        storeStatuses.put(0, storeStatusList);
        storeStatuses.put(1, storeStatusList);
        ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> storesMap = storeStatuses.build();
        indexStoreStatuses.put("test", storesMap);
        indexStoreStatuses.put("test2", storesMap);

        failures.add(new IndicesShardStoresResponse.Failure("node1", "test", 3, new NodeDisconnectedException(node1, "")));

        IndicesShardStoresResponse storesResponse = new IndicesShardStoresResponse(indexStoreStatuses.build(), Collections.unmodifiableList(failures));
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        contentBuilder.startObject();
        storesResponse.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        BytesReference bytes = contentBuilder.bytes();

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(bytes)) {
            Map<String, Object> map = parser.map();
            List failureList = (List) map.get("failures");
            assertThat(failureList.size(), equalTo(1));
            HashMap failureMap = (HashMap) failureList.get(0);
            assertThat(failureMap.containsKey("index"), equalTo(true));
            assertThat(((String) failureMap.get("index")), equalTo("test"));
            assertThat(failureMap.containsKey("shard"), equalTo(true));
            assertThat(((int) failureMap.get("shard")), equalTo(3));
            assertThat(failureMap.containsKey("node"), equalTo(true));
            assertThat(((String) failureMap.get("node")), equalTo("node1"));

            Map<String, Object> indices = (Map<String, Object>) map.get("indices");
            for (String index : new String[] {"test", "test2"}) {
                assertThat(indices.containsKey(index), equalTo(true));
                Map<String, Object> shards = ((Map<String, Object>) ((Map<String, Object>) indices.get(index)).get("shards"));
                assertThat(shards.size(), equalTo(2));
                for (String shardId : shards.keySet()) {
                    HashMap shardStoresStatus = (HashMap) shards.get(shardId);
                    assertThat(shardStoresStatus.containsKey("stores"), equalTo(true));
                    List stores = (ArrayList) shardStoresStatus.get("stores");
                    assertThat(stores.size(), equalTo(storeStatusList.size()));
                    for (int i = 0; i < stores.size(); i++) {
                        HashMap storeInfo = ((HashMap) stores.get(i));
                        IndicesShardStoresResponse.StoreStatus storeStatus = storeStatusList.get(i);
                        assertThat(storeInfo.containsKey("version"), equalTo(true));
                        assertThat(((int) storeInfo.get("version")), equalTo(((int) storeStatus.getVersion())));
                        assertThat(storeInfo.containsKey("allocation"), equalTo(true));
                        assertThat(((String) storeInfo.get("allocation")), equalTo(storeStatus.getAllocation().value()));
                        assertThat(storeInfo.containsKey(storeStatus.getNode().id()), equalTo(true));
                        if (storeStatus.getStoreException() != null) {
                            assertThat(storeInfo.containsKey("store_exception"), equalTo(true));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testStoreStatusOrdering() throws Exception {
        DiscoveryNode node1 = new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, Version.CURRENT);
        List<IndicesShardStoresResponse.StoreStatus> orderedStoreStatuses = new ArrayList<>();
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 2, IndicesShardStoresResponse.StoreStatus.Allocation.PRIMARY, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 1, IndicesShardStoresResponse.StoreStatus.Allocation.PRIMARY, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 1, IndicesShardStoresResponse.StoreStatus.Allocation.REPLICA, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 1, IndicesShardStoresResponse.StoreStatus.Allocation.UNUSED, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 3, IndicesShardStoresResponse.StoreStatus.Allocation.REPLICA, new IOException("corrupted")));

        List<IndicesShardStoresResponse.StoreStatus> storeStatuses = new ArrayList<>(orderedStoreStatuses);
        Collections.shuffle(storeStatuses);
        CollectionUtil.timSort(storeStatuses);
        assertThat(storeStatuses, equalTo(orderedStoreStatuses));
    }
}
