/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryStatsTests extends AbstractWireSerializingTestCase<QueryStats> {

    @Override
    protected Reader<QueryStats> instanceReader() {
        return QueryStats::new;
    }

    @Override
    protected QueryStats createTestInstance() {
        if (randomBoolean()) {
            return new QueryStats();
        }
        Map<String, Long> queriesCounts = new HashMap<>();
        List<String> queryTypes = List.of(
            "match",
            "match_phrase",
            "bool",
            "term",
            "terms",
            "multi_match",
            "function_score",
            "range",
            "script_score",
            "knn"
        );
        for (int i = 0; i < randomIntBetween(3, 10); i++) {
            queriesCounts.put(randomFrom(queryTypes), randomLongBetween(1, Long.MAX_VALUE));
        }
        return new QueryStats(queriesCounts);
    }

    @Override
    protected QueryStats mutateInstance(QueryStats instance) throws IOException {
        if ((instance.getQueriesCounts().size() > 0) && randomBoolean()) {
            return new QueryStats();
        }
        Map<String, Long> queriesCounts = new HashMap<>();
        List<String> queryTypes = List.of(
            "match",
            "match_phrase",
            "bool",
            "term",
            "terms",
            "multi_match",
            "function_score",
            "range",
            "script_score",
            "knn"
        );
        for (int i = 0; i < randomIntBetween(3, 10); i++) {
            queriesCounts.put(randomFrom(queryTypes), randomLongBetween(1, Long.MAX_VALUE));
        }
        return new QueryStats(queriesCounts);
    }
}
