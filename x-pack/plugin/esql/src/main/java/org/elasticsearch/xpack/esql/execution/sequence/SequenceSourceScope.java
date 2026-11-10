/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

/**
 * Captures one source expression's alias filters and routed shards before opening the shared snapshot. Query
 * filtering restores that branch's alias constraints; routing is checked against the hit's physical shard rather
 * than narrowing the PIT request, which would renumber the shared document ordinals.
 */
final class SequenceSourceScope {
    private final QueryBuilder query;
    private final Set<ShardId> shards;

    SequenceSourceScope(SearchShardsResponse response) {
        shards = response.getGroups().stream().map(group -> group.shardId()).collect(Collectors.toUnmodifiableSet());
        var filters = new LinkedHashMap<QueryBuilder, Set<String>>();
        for (ShardId shard : shards) {
            var alias = response.getAliasFilters().get(shard.getIndex().getUUID());
            QueryBuilder filter = alias == null ? null : alias.getQueryBuilder();
            filters.computeIfAbsent(filter, ignored -> new LinkedHashSet<>()).add(shard.getIndexName());
        }
        var membership = boolQuery().minimumShouldMatch(1);
        filters.forEach((alias, indices) -> {
            var clause = boolQuery().filter(termsQuery("_index", indices));
            if (alias != null) {
                clause.filter(alias);
            }
            membership.should(clause);
        });
        // Unfiltered patterns commonly span thousands of indices. Keep one terms query per distinct alias filter,
        // rather than creating a Boolean clause per index and inheriting Lucene's maximum-clause restriction.
        query = filters.isEmpty() ? new MatchNoneQueryBuilder() : membership;
    }

    QueryBuilder query() {
        return query;
    }

    /** Snapshot authorization keeps the original aliases; branch-local exclusions cannot subtract another stage's input. */
    static List<String> positiveExpressions(List<String> expressions) {
        return expressions.stream().filter(expression -> expression.startsWith("-") == false).toList();
    }

    /** Used by PIT can-match to open contexts only for indices in at least one captured branch scope. */
    Set<String> indexNames() {
        return shards.stream().map(ShardId::getIndexName).collect(Collectors.toSet());
    }

    boolean contains(SearchHit hit) {
        return hit.getShard() != null && shards.contains(hit.getShard().getShardId());
    }

    long estimatedBytes() {
        return 512L + 256L * shards.size() + 4L * query.toString().length();
    }
}
