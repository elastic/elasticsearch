/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResultTests;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResultTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;

public class SearchProfileQueryPhaseResultTests extends AbstractWireSerializingTestCase<SearchProfileQueryPhaseResult> {
    static SearchProfileQueryPhaseResult createTestItem() {
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>();
        int queryItems = rarely() ? 0 : randomIntBetween(1, 2);
        for (int q = 0; q < queryItems; q++) {
            queryProfileResults.add(QueryProfileShardResultTests.createTestItem());
        }
        AggregationProfileShardResult aggProfileShardResult = AggregationProfileShardResultTests.createTestItem(1);
        return new SearchProfileQueryPhaseResult(queryProfileResults, aggProfileShardResult);
    }

    @Override
    protected SearchProfileQueryPhaseResult createTestInstance() {
        return createTestItem();
    }

    @Override
    protected Reader<SearchProfileQueryPhaseResult> instanceReader() {
        return SearchProfileQueryPhaseResult::new;
    }
}
