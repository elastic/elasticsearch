/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SearchShardsResponseTests extends AbstractWireSerializingTestCase<SearchShardsResponse> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<SearchShardsResponse> instanceReader() {
        return SearchShardsResponse::new;
    }

    @Override
    protected SearchShardsResponse createTestInstance() {
        List<DiscoveryNode> nodes = randomList(1, 10, () -> DiscoveryNodeUtils.create(UUIDs.randomBase64UUID()));
        int numGroups = randomIntBetween(0, 10);
        List<SearchShardsGroup> groups = new ArrayList<>();
        for (int i = 0; i < numGroups; i++) {
            String index = randomAlphaOfLengthBetween(5, 10);
            ShardId shardId = new ShardId(index, UUIDs.randomBase64UUID(), i);
            int numOfAllocatedNodes = randomIntBetween(0, 5);
            List<String> allocatedNodes = new ArrayList<>();
            for (int j = 0; j < numOfAllocatedNodes; j++) {
                allocatedNodes.add(UUIDs.randomBase64UUID());
            }
            groups.add(new SearchShardsGroup(shardId, allocatedNodes, randomBoolean()));
        }
        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (SearchShardsGroup g : groups) {
            AliasFilter aliasFilter;
            if (randomBoolean()) {
                aliasFilter = AliasFilter.of(RandomQueryBuilder.createQuery(random()), "alias-" + g.shardId().getIndexName());
            } else {
                aliasFilter = AliasFilter.EMPTY;
            }
            aliasFilters.put(g.shardId().getIndex().getUUID(), aliasFilter);
        }
        return new SearchShardsResponse(groups, nodes, aliasFilters);
    }

    @Override
    protected SearchShardsResponse mutateInstance(SearchShardsResponse r) throws IOException {
        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                List<SearchShardsGroup> groups = new ArrayList<>(r.getGroups());
                ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), UUIDs.randomBase64UUID(), randomInt(2));
                groups.add(new SearchShardsGroup(shardId, List.of(), randomBoolean()));
                return new SearchShardsResponse(groups, r.getNodes(), r.getAliasFilters());
            }
            case 1 -> {
                List<DiscoveryNode> nodes = new ArrayList<>(r.getNodes());
                nodes.add(DiscoveryNodeUtils.create(UUIDs.randomBase64UUID()));
                return new SearchShardsResponse(r.getGroups(), nodes, r.getAliasFilters());
            }
            case 2 -> {
                Map<String, AliasFilter> aliasFilters = new HashMap<>(r.getAliasFilters());
                aliasFilters.put(UUIDs.randomBase64UUID(), AliasFilter.of(RandomQueryBuilder.createQuery(random()), "alias-index"));
                return new SearchShardsResponse(new ArrayList<>(r.getGroups()), r.getNodes(), aliasFilters);
            }
            default -> {
                throw new AssertionError("invalid option");
            }
        }
    }

    public void testLegacyResponse() {
        DiscoveryNode node1 = DiscoveryNodeUtils.create(
            "node-1",
            new TransportAddress(TransportAddress.META_ADDRESS, randomInt(0xFFFF)),
            VersionUtils.randomVersion(random())
        );
        DiscoveryNode node2 = DiscoveryNodeUtils.create(
            "node-2",
            new TransportAddress(TransportAddress.META_ADDRESS, randomInt(0xFFFF)),
            VersionUtils.randomVersion(random())
        );
        final ClusterSearchShardsGroup[] groups = new ClusterSearchShardsGroup[2];
        {
            ShardId shardId = new ShardId("index-1", "uuid-1", 0);
            var shard1 = TestShardRouting.newShardRouting(shardId, node1.getId(), randomBoolean(), ShardRoutingState.STARTED);
            var shard2 = TestShardRouting.newShardRouting(shardId, node2.getId(), randomBoolean(), ShardRoutingState.STARTED);
            groups[0] = new ClusterSearchShardsGroup(shardId, new ShardRouting[] { shard1, shard2 });
        }
        {
            ShardId shardId = new ShardId("index-2", "uuid-2", 7);
            var shard1 = TestShardRouting.newShardRouting(shardId, node1.getId(), randomBoolean(), ShardRoutingState.STARTED);
            groups[1] = new ClusterSearchShardsGroup(shardId, new ShardRouting[] { shard1 });
        }
        AliasFilter aliasFilter = AliasFilter.of(new TermQueryBuilder("t", "v"), "alias-1");
        var legacyResponse = new ClusterSearchShardsResponse(groups, new DiscoveryNode[] { node1, node2 }, Map.of("index-1", aliasFilter));
        SearchShardsResponse newResponse = SearchShardsResponse.fromLegacyResponse(legacyResponse);
        assertThat(newResponse.getNodes(), equalTo(List.of(node1, node2)));
        assertThat(newResponse.getAliasFilters(), equalTo(Map.of("uuid-1", aliasFilter)));
        assertThat(newResponse.getGroups(), hasSize(2));
        SearchShardsGroup group1 = Iterables.get(newResponse.getGroups(), 0);
        assertThat(group1.shardId(), equalTo(new ShardId("index-1", "uuid-1", 0)));
        assertThat(group1.allocatedNodes(), equalTo(List.of("node-1", "node-2")));
        assertFalse(group1.skipped());
        assertFalse(group1.preFiltered());

        SearchShardsGroup group2 = Iterables.get(newResponse.getGroups(), 1);
        assertThat(group2.shardId(), equalTo(new ShardId("index-2", "uuid-2", 7)));
        assertThat(group2.allocatedNodes(), equalTo(List.of("node-1")));
        assertFalse(group2.skipped());
        assertFalse(group2.preFiltered());

        TransportVersion version = TransportVersionUtils.randomCompatibleVersion(random());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            AssertionError error = expectThrows(AssertionError.class, () -> newResponse.writeTo(out));
            assertThat(error.getMessage(), equalTo("Serializing a response created from a legacy response is not allowed"));
        }
    }
}
