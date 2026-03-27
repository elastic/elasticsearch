/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.IndexReshardService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.VersionUtils.randomCompatibleVersion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

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
            groups.add(new SearchShardsGroup(shardId, allocatedNodes, SplitShardCountSummary.fromInt(randomIntBetween(0, 1024))));
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
        return new SearchShardsResponse(groups, 0, nodes, aliasFilters);
    }

    @Override
    protected SearchShardsResponse mutateInstance(SearchShardsResponse r) throws IOException {
        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                List<SearchShardsGroup> groups = new ArrayList<>(r.getGroups());
                ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), UUIDs.randomBase64UUID(), randomInt(2));
                groups.add(new SearchShardsGroup(shardId, List.of(), SplitShardCountSummary.fromInt(randomIntBetween(0, 1024))));
                return new SearchShardsResponse(groups, 0, r.getNodes(), r.getAliasFilters());
            }
            case 1 -> {
                List<DiscoveryNode> nodes = new ArrayList<>(r.getNodes());
                nodes.add(DiscoveryNodeUtils.create(UUIDs.randomBase64UUID()));
                return new SearchShardsResponse(r.getGroups(), 0, nodes, r.getAliasFilters());
            }
            case 2 -> {
                Map<String, AliasFilter> aliasFilters = new HashMap<>(r.getAliasFilters());
                aliasFilters.put(UUIDs.randomBase64UUID(), AliasFilter.of(RandomQueryBuilder.createQuery(random()), "alias-index"));
                return new SearchShardsResponse(new ArrayList<>(r.getGroups()), 0, r.getNodes(), aliasFilters);
            }
            default -> {
                throw new AssertionError("invalid option");
            }
        }
    }

    public void testLegacyResponse() {
        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node-1")
            .address(new TransportAddress(TransportAddress.META_ADDRESS, randomInt(0xFFFF)))
            .version(randomCompatibleVersion(Version.CURRENT), IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current())
            .build();
        DiscoveryNode node2 = DiscoveryNodeUtils.builder("node-2")
            .address(new TransportAddress(TransportAddress.META_ADDRESS, randomInt(0xFFFF)))
            .version(randomCompatibleVersion(Version.CURRENT), IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current())
            .build();
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
        assertThat(group1.reshardSplitShardCountSummary(), equalTo(SplitShardCountSummary.UNSET));
        assertFalse(group1.preFiltered());

        SearchShardsGroup group2 = Iterables.get(newResponse.getGroups(), 1);
        assertThat(group2.shardId(), equalTo(new ShardId("index-2", "uuid-2", 7)));
        assertThat(group2.allocatedNodes(), equalTo(List.of("node-1")));
        assertThat(group2.reshardSplitShardCountSummary(), equalTo(SplitShardCountSummary.UNSET));
        assertFalse(group2.preFiltered());

        TransportVersion version = TransportVersionUtils.randomCompatibleVersion();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            AssertionError error = expectThrows(AssertionError.class, () -> newResponse.writeTo(out));
            assertEquals("Serializing a response created from a legacy response is not allowed", error.getMessage());
        }
    }

    public void testWireDeserializationUsesAggregateSkippedCount() throws IOException {
        TransportVersion version = TransportVersion.current();
        // search_shards_num_skipped2: skipped shards are only in the aggregate vint, not per-group booleans on wire.
        SearchShardsResponse response = readResponseFromWire(version, List.of(false, false), 1);
        assertEquals(2, response.getGroups().size());
        assertEquals(1, response.getNumSkippedShards());
        response = readResponseFromWire(version, List.of(false, false), 0);
        assertEquals(2, response.getGroups().size());
        assertEquals(0, response.getNumSkippedShards());

    }

    public void testWireDeserializationRejectsHybridSkippedEncodingOnCurrentVersion() {
        TransportVersion version = TransportVersion.current();
        AssertionError assertionError = expectThrows(AssertionError.class, () -> readResponseFromWire(version, List.of(true, false), 5));
        assertThat(assertionError.getMessage(), containsString("skippedOnWire should be 0"));
    }

    public void testWireDeserializationFallsBackToSkippedGroups() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(SearchShardsResponse.SEARCH_SHARDS_NUM_SKIPPED2);
        SearchShardsResponse response = readResponseFromWire(oldVersion, List.of(true, true, false), 0);
        assertEquals(1, response.getGroups().size());
        assertEquals(2, response.getNumSkippedShards());
    }

    private SearchShardsResponse readResponseFromWire(TransportVersion version, List<Boolean> skippedFlags, int numSkippedShards)
        throws IOException {
        List<ShardId> shardIds = new ArrayList<>();
        for (int i = 0; i < skippedFlags.size(); i++) {
            shardIds.add(new ShardId("index-" + i, UUIDs.randomBase64UUID(), i));
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            out.writeCollection(shardIds, (stream, shardId) -> {
                shardId.writeTo(stream);
                stream.writeStringCollection(List.of("node-a"));
                stream.writeBoolean(skippedFlags.get(shardId.id()));
                if (version.supports(IndexReshardService.RESHARDING_SHARD_SUMMARY_IN_ESQL)) {
                    SplitShardCountSummary.UNSET.writeTo(stream);
                }
            });
            out.writeCollection(List.<DiscoveryNode>of());
            out.writeMap(Map.<String, AliasFilter>of(), (stream, aliasFilter) -> stream.writeWriteable(aliasFilter));
            if (version.supports(SearchShardsResponse.SEARCH_SHARDS_RESOLVED_INDEX_EXPRESSIONS)) {
                out.writeOptionalWriteable(null);
            }
            if (version.supports(SearchShardsResponse.SEARCH_SHARDS_NUM_SKIPPED2)) {
                out.writeVInt(numSkippedShards);
            }
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return new SearchShardsResponse(in);
            }
        }
    }
}
