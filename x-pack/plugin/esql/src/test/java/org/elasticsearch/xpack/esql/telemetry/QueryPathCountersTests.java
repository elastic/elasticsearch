/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Feature;

import java.util.BitSet;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class QueryPathCountersTests extends ESTestCase {

    public void testNoQueryPathsBeforeAnyQuery() {
        assertFalse(stats(new QueryPathCounters()).hasCounters());
    }

    public void testQueryPathCountsTotalAndTheCommandsItUsed() {
        QueryPathCounters queryPaths = new QueryPathCounters();
        QueryPath path = new QueryPath(QueryMetric.KIBANA, Feature.DISCOVER.toString(), LeadingCommand.FROM);
        queryPaths.total(path, commands(FeatureMetric.FROM, FeatureMetric.WHERE));
        queryPaths.total(path, commands(FeatureMetric.FROM, FeatureMetric.STATS));
        assertThat(
            nestedQueryPaths(stats(queryPaths)),
            equalTo(
                Map.of(
                    "kibana",
                    Map.of("discover", Map.of("from", Map.of("total", 2L, "commands", Map.of("from", 2L, "where", 1L, "stats", 1L))))
                )
            )
        );
    }

    public void testFailedQueryPathCountsFailed() {
        QueryPathCounters queryPaths = new QueryPathCounters();
        QueryPath path = new QueryPath(QueryMetric.REST, DirectClient.PYTHON.toString(), LeadingCommand.TS);
        queryPaths.total(path, commands());
        queryPaths.failed(path);
        assertThat(
            nestedQueryPaths(stats(queryPaths)),
            equalTo(Map.of("rest", Map.of("python", Map.of("ts", Map.of("total", 1L, "failed", 1L)))))
        );
    }

    public void testQueryPathsAreCountedApart() {
        QueryPathCounters queryPaths = new QueryPathCounters();
        queryPaths.total(new QueryPath(QueryMetric.FLEET, QueryPath.NO_SOURCE, LeadingCommand.ROW), commands(FeatureMetric.ROW));
        queryPaths.total(new QueryPath(QueryMetric.FLEET, QueryPath.NO_SOURCE, LeadingCommand.FROM), commands());
        queryPaths.total(new QueryPath(QueryMetric.OTHER, QueryPath.NO_SOURCE, LeadingCommand.ROW), commands());
        assertThat(
            nestedQueryPaths(stats(queryPaths)),
            equalTo(
                Map.of(
                    "fleet",
                    Map.of("_none", Map.of("row", Map.of("total", 1L, "commands", Map.of("row", 1L)), "from", Map.of("total", 1L))),
                    "other",
                    Map.of("_none", Map.of("row", Map.of("total", 1L)))
                )
            )
        );
    }

    public void testPopulateKeepsExistingCounters() {
        QueryPathCounters queryPaths = new QueryPathCounters();
        queryPaths.total(new QueryPath(QueryMetric.REST, DirectClient.CURL.toString(), LeadingCommand.FROM), commands());
        Counters counters = new Counters();
        counters.inc("queries.rest.total", 3);
        queryPaths.populate(counters);
        assertThat(counters.get("queries.rest.total"), equalTo(3L));
        assertThat(counters.get("query_paths.rest.curl.from.total"), equalTo(1L));
    }

    public void testConcurrentQueryPathCountsAreExact() throws Exception {
        QueryPathCounters queryPaths = new QueryPathCounters();
        QueryPath path = new QueryPath(QueryMetric.REST, DirectClient.CURL.toString(), LeadingCommand.FROM);
        int threads = between(2, 8);
        int perThread = between(100, 1000);
        startInParallel(threads, t -> {
            for (int i = 0; i < perThread; i++) {
                queryPaths.total(path, commands(FeatureMetric.WHERE));
                queryPaths.failed(path);
            }
        });
        Counters stats = stats(queryPaths);
        long expected = (long) threads * perThread;
        assertThat(stats.get("query_paths.rest.curl.from.total"), equalTo(expected));
        assertThat(stats.get("query_paths.rest.curl.from.failed"), equalTo(expected));
        assertThat(stats.get("query_paths.rest.curl.from.commands.where"), equalTo(expected));
    }

    private static BitSet commands(FeatureMetric... used) {
        BitSet bits = new BitSet(FeatureMetric.values().length);
        for (FeatureMetric metric : used) {
            bits.set(metric.ordinal());
        }
        return bits;
    }

    private static Counters stats(QueryPathCounters queryPaths) {
        Counters counters = new Counters();
        queryPaths.populate(counters);
        return counters;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nestedQueryPaths(Counters stats) {
        return (Map<String, Object>) stats.toNestedMap().get(QueryPath.USAGE_KEY);
    }
}
