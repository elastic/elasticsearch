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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;

public class ClusterSearchShardsResponseTests extends AbstractStreamableXContentTestCase<ClusterSearchShardsResponse> {

    protected static NamedWriteableRegistry namedWriteableRegistry;
    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void cleanup() {
        namedWriteableRegistry = null;
        xContentRegistry = null;
    }

    public void testStreamOutputSerialization() throws Exception {
        ClusterSearchShardsResponse clusterSearchShardsResponse = createTestInstance();

        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(searchModule.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(entries);
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.CURRENT);
        try(BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            clusterSearchShardsResponse.writeTo(out);
            try(StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)) {
                in.setVersion(version);
                ClusterSearchShardsResponse deserialized = new ClusterSearchShardsResponse();
                deserialized.readFrom(in);
                assertArrayEquals(clusterSearchShardsResponse.getNodes(), deserialized.getNodes());
                assertEquals(clusterSearchShardsResponse.getGroups().length, deserialized.getGroups().length);
                for (int i = 0; i < clusterSearchShardsResponse.getGroups().length; i++) {
                    ClusterSearchShardsGroup clusterSearchShardsGroup = clusterSearchShardsResponse.getGroups()[i];
                    ClusterSearchShardsGroup deserializedGroup = deserialized.getGroups()[i];
                    assertEquals(clusterSearchShardsGroup.getShardId(), deserializedGroup.getShardId());
                    assertArrayEquals(clusterSearchShardsGroup.getShards(), deserializedGroup.getShards());
                }
                if (version.onOrAfter(Version.V_5_1_1)) {
                    assertEquals(clusterSearchShardsResponse.getIndicesAndFilters(), deserialized.getIndicesAndFilters());
                } else {
                    assertNull(deserialized.getIndicesAndFilters());
                }
            }
        }
    }


    @Override
    protected ClusterSearchShardsResponse doParseInstance(XContentParser parser) {
        return ClusterSearchShardsResponse.fromXContent(parser);
    }

    @Override
    protected ClusterSearchShardsResponse mutateInstance(ClusterSearchShardsResponse response) {
        int i = randomIntBetween(0, 2);
        switch(i) {
            case 0:
                return new ClusterSearchShardsResponse(mutate(response.getGroups()), response.getNodes(), response.getIndicesAndFilters());
            case 1:
                return new ClusterSearchShardsResponse(response.getGroups(), mutate(response.getNodes()), response.getIndicesAndFilters());
            case 2:
                return new ClusterSearchShardsResponse(response.getGroups(), response.getNodes(), mutate(response.getIndicesAndFilters()));
            default:
                throw new UnsupportedOperationException();
        }
    }

    private ClusterSearchShardsGroup[] mutate(ClusterSearchShardsGroup[] groups) {
        int size = groups.length;
        for(;;){
            groups = randomShardsGroups(3);
            if (size != groups.length) {
                break;
            }
        }
        return groups;
    }

    private DiscoveryNode[] mutate(DiscoveryNode[] nodes) {
        int size = nodes.length;
        for(;;){
            nodes = randomDiscoveryNodes(3);
            if (size != nodes.length) {
                break;
            }
        }
        return nodes;
    }

    private Map<String, AliasFilter> mutate(Map<String, AliasFilter> indicesAndFilters) {
        final int size = indicesAndFilters.size();
        for(;;){
            indicesAndFilters = randomIndicesAndFilters(3);
            if (size != indicesAndFilters.size()) {
                break;
            }
        }
        return indicesAndFilters;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[]{"nodes"};
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.startsWith("nodes") || p.startsWith("indices") || p.startsWith("shards");
    }

    @Override
    protected ClusterSearchShardsResponse createTestInstance() {
        return new ClusterSearchShardsResponse(randomShardsGroups(), randomDiscoveryNodes(), randomIndicesAndFilters());
    }

    private ClusterSearchShardsGroup[] randomShardsGroups() {
        return randomShardsGroups(5);
    }

    private ClusterSearchShardsGroup[] randomShardsGroups(int max) {
        ClusterSearchShardsGroup[] groups = new ClusterSearchShardsGroup[randomIntBetween(1, max)];
        for(int i = 0; i < groups.length; i++ ){
            final String index = "index-" + randomIntBetween(1, 10);
            ShardId shardId = new ShardId(index,
                // TODO: impossible to recreate uuid - see ShardRouting#PARSER
                IndexMetaData.INDEX_UUID_NA_VALUE,
                randomIntBetween(1, 10));
            ShardRouting[] shardRoutings = new ShardRouting[randomIntBetween(1, 2)];
            for(int j = 0; j < shardRoutings.length; j++){
                final Snapshot snapshot = new Snapshot("repo-" + randomIntBetween(1, 10),
                    new SnapshotId("snapshot-" + randomIntBetween(1, 5), // TODO: impossible to recreate uuid - see ShardRouting#PARSER
                        IndexMetaData.INDEX_UUID_NA_VALUE));
                final String nodeId = randomAlphaOfLength(10);
                final boolean primary = randomBoolean();
                final RecoverySource recoverySource =
                    primary
                        ? new RecoverySource.SnapshotRecoverySource(snapshot, Version.CURRENT, index)
                        : RecoverySource.PeerRecoverySource.INSTANCE;
                shardRoutings[j] = TestShardRouting.newShardRouting(shardId, null, primary,
                    recoverySource, ShardRoutingState.UNASSIGNED);
            }
            groups[i] = new ClusterSearchShardsGroup(shardId, shardRoutings);
        }
        return groups;
    }

    private DiscoveryNode[] randomDiscoveryNodes() {
        return randomDiscoveryNodes(5);
    }

    private DiscoveryNode[] randomDiscoveryNodes(int max) {
        DiscoveryNode[] nodes = new DiscoveryNode[randomIntBetween(1, max)];
        for(int i = 0; i < nodes.length; i++ ){
            nodes[i] = new DiscoveryNode("node_" + randomIntBetween(1, nodes.length), randomAlphaOfLengthBetween(3, 10),
                buildNewFakeTransportAddress(), randomAttributes(), Collections.emptySet(), Version.CURRENT);
        }
        return nodes;
    }

    private Map<String, AliasFilter> randomIndicesAndFilters() {
        return randomIndicesAndFilters(5);
    }

    private Map<String, AliasFilter> randomIndicesAndFilters(int max) {
        Map<String, AliasFilter> map = new HashMap<>();
        for(int i = 0, len = randomIntBetween(0, max); i < len; i++) {
            String[] aliases = new String[randomIntBetween(0, 5)];
            for(int j = 0; j < aliases.length; j++){
                aliases[j] = "alias" + randomAlphaOfLengthBetween(3, 10);
            }
            // aliases are stored in sorted order...
            Arrays.sort(aliases);
            final QueryBuilder filter = RandomQueryBuilder.createQuery(random());
            // de-facto - no aliases - filter does not make any sense
            AliasFilter aliasFilter = new AliasFilter(aliases.length > 0 ? filter : null, aliases);
            map.put("index-" + randomIntBetween(0, 5), aliasFilter);
        }

        return map;
    }

    private Map<String, String> randomAttributes() {
        Map<String, String> map = new HashMap<>();
        for(int i = 0, len = randomIntBetween(0, 5); i < len; i++) {
            map.put("attr-" + randomIntBetween(0, 5), randomAlphaOfLengthBetween(3, 10));
        }
        return map;
    }

    @Override
    protected ClusterSearchShardsResponse createBlankInstance() {
        return new ClusterSearchShardsResponse();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
