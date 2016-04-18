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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardStateMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeDisconnectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class IndicesShardStoreResponseTests extends ESTestCase {
    public void testBasicSerialization() throws Exception {
        ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>>> indexStoreStatuses = ImmutableOpenMap.builder();
        List<IndicesShardStoresResponse.Failure> failures = new ArrayList<>();
        ImmutableOpenIntMap.Builder<List<IndicesShardStoresResponse.StoreStatus>> storeStatuses = ImmutableOpenIntMap.builder();

        DiscoveryNode node1 = new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        List<IndicesShardStoresResponse.StoreStatus> storeStatusList = new ArrayList<>();
        storeStatusList.add(new IndicesShardStoresResponse.StoreStatus(node1, 3, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null));
        storeStatusList.add(new IndicesShardStoresResponse.StoreStatus(node2, ShardStateMetaData.NO_VERSION, UUIDs.randomBase64UUID(), IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA, null));
        storeStatusList.add(new IndicesShardStoresResponse.StoreStatus(node1, ShardStateMetaData.NO_VERSION, UUIDs.randomBase64UUID(), IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED, new IOException("corrupted")));
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
                        boolean eitherLegacyVersionOrAllocationIdSet = false;
                        if (storeInfo.containsKey("legacy_version")) {
                            assertThat(((int) storeInfo.get("legacy_version")), equalTo(((int) storeStatus.getLegacyVersion())));
                            eitherLegacyVersionOrAllocationIdSet = true;
                        }
                        if (storeInfo.containsKey("allocation_id")) {
                            assertThat(((String) storeInfo.get("allocation_id")), equalTo((storeStatus.getAllocationId())));
                            eitherLegacyVersionOrAllocationIdSet = true;
                        }
                        assertThat(eitherLegacyVersionOrAllocationIdSet, equalTo(true));
                        assertThat(storeInfo.containsKey("allocation"), equalTo(true));
                        assertThat(((String) storeInfo.get("allocation")), equalTo(storeStatus.getAllocationStatus().value()));
                        assertThat(storeInfo.containsKey(storeStatus.getNode().getId()), equalTo(true));
                        if (storeStatus.getStoreException() != null) {
                            assertThat(storeInfo.containsKey("store_exception"), equalTo(true));
                        }
                    }
                }
            }
        }
    }

    public void testStoreStatusOrdering() throws Exception {
        DiscoveryNode node1 = new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        List<IndicesShardStoresResponse.StoreStatus> orderedStoreStatuses = new ArrayList<>();
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, ShardStateMetaData.NO_VERSION, UUIDs.randomBase64UUID(), IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, ShardStateMetaData.NO_VERSION, UUIDs.randomBase64UUID(), IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, ShardStateMetaData.NO_VERSION, UUIDs.randomBase64UUID(), IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 2, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 1, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 1, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 1, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED, null));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, ShardStateMetaData.NO_VERSION, UUIDs.randomBase64UUID(), IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA, new IOException("corrupted")));
        orderedStoreStatuses.add(new IndicesShardStoresResponse.StoreStatus(node1, 3, null, IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA, new IOException("corrupted")));

        List<IndicesShardStoresResponse.StoreStatus> storeStatuses = new ArrayList<>(orderedStoreStatuses);
        Collections.shuffle(storeStatuses, random());
        CollectionUtil.timSort(storeStatuses);
        assertThat(storeStatuses, equalTo(orderedStoreStatuses));
    }
}
