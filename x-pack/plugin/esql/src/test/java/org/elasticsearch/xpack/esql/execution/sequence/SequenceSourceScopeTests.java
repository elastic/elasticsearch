/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/** Ensures broad source patterns retain grouped alias scope without a Boolean clause for every concrete index. */
public class SequenceSourceScopeTests extends ESTestCase {
    public void testThousandsOfIndicesShareOneUnfilteredClause() {
        List<SearchShardsGroup> shards = new ArrayList<>();
        for (int index = 0; index < 2048; index++) {
            shards.add(group(index));
        }
        var scope = new SequenceSourceScope(new SearchShardsResponse(shards, 0, List.of(), java.util.Map.of()));
        BoolQueryBuilder membership = (BoolQueryBuilder) scope.query();
        assertEquals(1, membership.should().size());
        BoolQueryBuilder clause = (BoolQueryBuilder) membership.should().getFirst();
        assertEquals(1, clause.filter().size());
        assertEquals(2048, ((TermsQueryBuilder) clause.filter().getFirst()).values().size());
    }

    public void testEquivalentAliasFiltersAreGroupedSeparatelyFromUnfilteredIndices() {
        List<SearchShardsGroup> shards = new ArrayList<>();
        var aliases = new HashMap<String, AliasFilter>();
        for (int index = 0; index < 6; index++) {
            SearchShardsGroup shard = group(index);
            shards.add(shard);
            if (index % 3 != 0) {
                aliases.put(shard.shardId().getIndex().getUUID(), AliasFilter.of(termQuery("tenant", index % 3), "alias" + index));
            }
        }
        var scope = new SequenceSourceScope(new SearchShardsResponse(shards, 0, List.of(), aliases));
        BoolQueryBuilder membership = (BoolQueryBuilder) scope.query();
        assertEquals(3, membership.should().size());
        for (var query : membership.should()) {
            var clause = (BoolQueryBuilder) query;
            assertEquals(2, ((TermsQueryBuilder) clause.filter().getFirst()).values().size());
        }
    }

    private static SearchShardsGroup group(int index) {
        return new SearchShardsGroup(
            new ShardId("events" + index, "uuid" + index, 0),
            List.of("node"),
            false,
            SplitShardCountSummary.UNSET
        );
    }
}
