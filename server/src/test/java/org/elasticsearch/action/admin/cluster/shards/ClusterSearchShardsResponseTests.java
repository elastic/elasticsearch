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

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterSearchShardsResponseTests extends ESTestCase {

    public void testSerialization() throws Exception {
        Map<String, AliasFilter> indicesAndFilters = new HashMap<>();
        Set<DiscoveryNode> nodes = new HashSet<>();
        int numShards = randomIntBetween(1, 10);
        ClusterSearchShardsGroup[] clusterSearchShardsGroups = new ClusterSearchShardsGroup[numShards];
        for (int i = 0; i < numShards; i++) {
            String index = randomAlphaOfLengthBetween(3, 10);
            ShardId shardId = new ShardId(index, randomAlphaOfLength(12), i);
            String nodeId = randomAlphaOfLength(10);
            ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, nodeId, randomBoolean(), ShardRoutingState.STARTED);
            clusterSearchShardsGroups[i] = new ClusterSearchShardsGroup(shardId, new ShardRouting[]{shardRouting});
            DiscoveryNode node = new DiscoveryNode(shardRouting.currentNodeId(),
                    new TransportAddress(TransportAddress.META_ADDRESS, randomInt(0xFFFF)), VersionUtils.randomVersion(random()));
            nodes.add(node);
            AliasFilter aliasFilter;
            if (randomBoolean()) {
                aliasFilter = new AliasFilter(RandomQueryBuilder.createQuery(random()), "alias-" + index);
            } else {
                aliasFilter = new AliasFilter(null, Strings.EMPTY_ARRAY);
            }
            indicesAndFilters.put(index, aliasFilter);
        }
        ClusterSearchShardsResponse clusterSearchShardsResponse = new ClusterSearchShardsResponse(clusterSearchShardsGroups,
                nodes.toArray(new DiscoveryNode[nodes.size()]), indicesAndFilters);

        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(searchModule.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(entries);
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        try(BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            clusterSearchShardsResponse.writeTo(out);
            try(StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)) {
                in.setVersion(version);
                ClusterSearchShardsResponse deserialized = new ClusterSearchShardsResponse(in);
                assertArrayEquals(clusterSearchShardsResponse.getNodes(), deserialized.getNodes());
                assertEquals(clusterSearchShardsResponse.getGroups().length, deserialized.getGroups().length);
                for (int i = 0; i < clusterSearchShardsResponse.getGroups().length; i++) {
                    ClusterSearchShardsGroup clusterSearchShardsGroup = clusterSearchShardsResponse.getGroups()[i];
                    ClusterSearchShardsGroup deserializedGroup = deserialized.getGroups()[i];
                    assertEquals(clusterSearchShardsGroup.getShardId(), deserializedGroup.getShardId());
                    assertArrayEquals(clusterSearchShardsGroup.getShards(), deserializedGroup.getShards());
                }
                assertEquals(clusterSearchShardsResponse.getIndicesAndFilters(), deserialized.getIndicesAndFilters());
            }
        }
    }
}
