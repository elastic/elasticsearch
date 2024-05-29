/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shards;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.Failure;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus.AllocationStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class IndicesShardStoreResponseTests extends ESTestCase {
    public void testBasicSerialization() throws Exception {
        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        DiscoveryNode node2 = DiscoveryNodeUtils.builder("node2").roles(emptySet()).build();
        List<StoreStatus> storeStatusList = List.of(
            new StoreStatus(node1, null, AllocationStatus.PRIMARY, null),
            new StoreStatus(node2, UUIDs.randomBase64UUID(), AllocationStatus.REPLICA, null),
            new StoreStatus(node1, UUIDs.randomBase64UUID(), AllocationStatus.UNUSED, new IOException("corrupted"))
        );
        Map<Integer, List<StoreStatus>> storeStatuses = Map.of(0, storeStatusList, 1, storeStatusList);
        var indexStoreStatuses = Map.of("test", storeStatuses, "test2", storeStatuses);
        var failures = List.of(new Failure("node1", "test", 3, new NodeDisconnectedException(node1, "")));
        var storesResponse = new IndicesShardStoresResponse(indexStoreStatuses, failures);

        AbstractChunkedSerializingTestCase.assertChunkCount(storesResponse, this::getExpectedChunkCount);

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        ChunkedToXContent.wrapAsToXContent(storesResponse).toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(contentBuilder);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
            Map<String, Object> map = parser.map();
            List<?> failureList = (List<?>) map.get("failures");
            assertThat(failureList.size(), equalTo(1));
            @SuppressWarnings("unchecked")
            Map<String, ?> failureMap = (Map<String, ?>) failureList.get(0);
            assertThat(failureMap.containsKey("index"), equalTo(true));
            assertThat(((String) failureMap.get("index")), equalTo("test"));
            assertThat(failureMap.containsKey("shard"), equalTo(true));
            assertThat(((int) failureMap.get("shard")), equalTo(3));
            assertThat(failureMap.containsKey("node"), equalTo(true));
            assertThat(((String) failureMap.get("node")), equalTo("node1"));

            @SuppressWarnings("unchecked")
            Map<String, Object> indices = (Map<String, Object>) map.get("indices");
            for (String index : new String[] { "test", "test2" }) {
                assertThat(indices.containsKey(index), equalTo(true));
                @SuppressWarnings("unchecked")
                Map<String, Object> shards = ((Map<String, Object>) ((Map<String, Object>) indices.get(index)).get("shards"));
                assertThat(shards.size(), equalTo(2));
                for (String shardId : shards.keySet()) {
                    @SuppressWarnings("unchecked")
                    Map<String, ?> shardStoresStatus = (Map<String, ?>) shards.get(shardId);
                    assertThat(shardStoresStatus.containsKey("stores"), equalTo(true));
                    List<?> stores = (List<?>) shardStoresStatus.get("stores");
                    assertThat(stores.size(), equalTo(storeStatusList.size()));
                    for (int i = 0; i < stores.size(); i++) {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> storeInfo = ((Map<String, ?>) stores.get(i));
                        StoreStatus storeStatus = storeStatusList.get(i);
                        assertThat(((String) storeInfo.get("allocation_id")), equalTo((storeStatus.getAllocationId())));
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

    private int getExpectedChunkCount(IndicesShardStoresResponse response) {
        return 6 + response.getFailures().size() + response.getStoreStatuses().values().stream().mapToInt(m -> 4 + m.size()).sum();
    }

    public void testStoreStatusOrdering() throws Exception {
        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        List<StoreStatus> orderedStoreStatuses = new ArrayList<>();
        orderedStoreStatuses.add(new StoreStatus(node1, UUIDs.randomBase64UUID(), AllocationStatus.PRIMARY, null));
        orderedStoreStatuses.add(new StoreStatus(node1, UUIDs.randomBase64UUID(), AllocationStatus.REPLICA, null));
        orderedStoreStatuses.add(new StoreStatus(node1, UUIDs.randomBase64UUID(), AllocationStatus.UNUSED, null));
        orderedStoreStatuses.add(new StoreStatus(node1, null, AllocationStatus.PRIMARY, null));
        orderedStoreStatuses.add(new StoreStatus(node1, null, AllocationStatus.REPLICA, null));
        orderedStoreStatuses.add(new StoreStatus(node1, null, AllocationStatus.UNUSED, null));
        orderedStoreStatuses.add(new StoreStatus(node1, UUIDs.randomBase64UUID(), AllocationStatus.REPLICA, new IOException("corrupted")));
        orderedStoreStatuses.add(new StoreStatus(node1, null, AllocationStatus.REPLICA, new IOException("corrupted")));

        List<StoreStatus> storeStatuses = new ArrayList<>(orderedStoreStatuses);
        Collections.shuffle(storeStatuses, random());
        CollectionUtil.timSort(storeStatuses);
        assertThat(storeStatuses, equalTo(orderedStoreStatuses));
    }
}
