/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

public class SearchProfileResultsBuilderTests extends ESTestCase {
    public void testFetchWithoutQuery() {
        Map<SearchShardTarget, SearchProfileQueryPhaseResult> searchPhase = randomSearchPhaseResults(rarely() ? 0 : between(1, 2));
        FetchSearchResult fetchPhase = fetchResult(
            randomValueOtherThanMany(searchPhase::containsKey, SearchProfileResultsBuilderTests::randomTarget),
            null
        );
        Exception e = expectThrows(IllegalStateException.class, () -> builder(searchPhase).build(List.of(fetchPhase)));
        assertThat(
            e.getMessage(),
            matchesPattern(
                "Profile returned fetch phase information for .+ but didn't return query phase information\\. Query phase keys were .+"
            )
        );
    }

    public void testQueryWithoutAnyFetch() {
        Map<SearchShardTarget, SearchProfileQueryPhaseResult> searchPhase = randomSearchPhaseResults(between(1, 2));
        FetchSearchResult fetchPhase = fetchResult(searchPhase.keySet().iterator().next(), null);
        SearchProfileResults result = builder(searchPhase).build(List.of(fetchPhase));
        assertThat(
            result.getShardResults().values().stream().filter(r -> r.getQueryPhase() != null).count(),
            equalTo((long) searchPhase.size())
        );
        assertThat(result.getShardResults().values().stream().filter(r -> r.getFetchPhase() != null).count(), equalTo(0L));
    }

    public void testQueryAndFetch() {
        Map<SearchShardTarget, SearchProfileQueryPhaseResult> searchPhase = randomSearchPhaseResults(between(1, 2));
        List<FetchSearchResult> fetchPhase = searchPhase.entrySet()
            .stream()
            .map(e -> fetchResult(e.getKey(), new ProfileResult("fetch", "", Map.of(), Map.of(), 1, List.of())))
            .collect(toList());
        SearchProfileResults result = builder(searchPhase).build(fetchPhase);
        assertThat(
            result.getShardResults().values().stream().filter(r -> r.getQueryPhase() != null).count(),
            equalTo((long) searchPhase.size())
        );
        assertThat(
            result.getShardResults().values().stream().filter(r -> r.getFetchPhase() != null).count(),
            equalTo((long) searchPhase.size())
        );
    }

    private static Map<SearchShardTarget, SearchProfileQueryPhaseResult> randomSearchPhaseResults(int size) {
        Map<SearchShardTarget, SearchProfileQueryPhaseResult> results = Maps.newMapWithExpectedSize(size);
        while (results.size() < size) {
            results.put(randomTarget(), SearchProfileQueryPhaseResultTests.createTestItem());
        }
        return results;
    }

    private static SearchProfileResultsBuilder builder(Map<SearchShardTarget, SearchProfileQueryPhaseResult> searchPhase) {
        return new SearchProfileResultsBuilder(
            searchPhase.entrySet().stream().collect(toMap(e -> e.getKey().toString(), Map.Entry::getValue))
        );
    }

    private static FetchSearchResult fetchResult(SearchShardTarget target, ProfileResult profileResult) {
        FetchSearchResult fetchResult = new FetchSearchResult();
        fetchResult.shardResult(SearchHits.EMPTY_WITH_TOTAL_HITS, profileResult);
        fetchResult.setSearchShardTarget(target);
        return fetchResult;
    }

    private static SearchShardTarget randomTarget() {
        return new SearchShardTarget(randomAlphaOfLength(5), new ShardId(randomAlphaOfLength(5), "uuid", randomInt(6)), null);
    }
}
