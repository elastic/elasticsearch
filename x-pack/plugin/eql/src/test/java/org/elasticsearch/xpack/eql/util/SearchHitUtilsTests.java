/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class SearchHitUtilsTests extends ESTestCase {

    public void testMergeKeepsTheFirstFailureForARepeatedKey() {
        ShardSearchFailure search = new ShardSearchFailure(new IllegalStateException("search"));
        Map<String, ShardSearchFailure> recorded = new HashMap<>();
        recorded.put(search.toString(), search);
        assertThat(SearchHitUtils.mergeShardFailures(recorded, List.of(search)), equalTo(List.of(search)));
    }

    public void testMergeAddsAFetchFailureWithADifferentKey() {
        ShardSearchFailure search = new ShardSearchFailure(new IllegalStateException("search"));
        ShardSearchFailure fetch = new ShardSearchFailure(new IllegalStateException("fetch"));
        Map<String, ShardSearchFailure> recorded = new HashMap<>();
        recorded.put(search.toString(), search);
        assertThat(SearchHitUtils.mergeShardFailures(recorded, List.of(fetch)), containsInAnyOrder(search, fetch));
    }

    public void testMergeWithNoFetchFailuresReturnsTheRecordedList() {
        ShardSearchFailure search = new ShardSearchFailure(new IllegalStateException("search"));
        Map<String, ShardSearchFailure> recorded = new HashMap<>();
        recorded.put(search.toString(), search);
        assertThat(SearchHitUtils.mergeShardFailures(recorded, List.of()), equalTo(List.of(search)));
    }
}
