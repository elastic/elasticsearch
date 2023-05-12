/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        List<DiscoveryNode> nodes = randomList(1, 10, () -> TestDiscoveryNode.create(UUIDs.randomBase64UUID()));
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
            groups.add(new SearchShardsGroup(shardId, allocatedNodes, true, randomBoolean()));
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
                groups.add(new SearchShardsGroup(shardId, List.of(), true, randomBoolean()));
                return new SearchShardsResponse(groups, r.getNodes(), r.getAliasFilters());
            }
            case 1 -> {
                List<DiscoveryNode> nodes = new ArrayList<>(r.getNodes());
                nodes.add(TestDiscoveryNode.create(UUIDs.randomBase64UUID()));
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
}
