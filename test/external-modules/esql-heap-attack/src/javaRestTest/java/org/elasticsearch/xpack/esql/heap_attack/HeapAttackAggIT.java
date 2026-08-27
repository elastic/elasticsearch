/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.ListMatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Heap-attack tests for ES|QL aggregation functions. Many aggs are still in
 * {@link HeapAttackIT} but will be moved here before long.
 */
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE)
public class HeapAttackAggIT extends HeapAttackTestCase {

    /**
     * Tests that SPARKLINE with a manageable number of columns succeeds and returns the
     * expected bucketed sums. With 2 SPARKLINE columns and 10 000 groups
     * (b, c, d, e ∈ [0,9]^4), the TOP accumulators in phase 2 of the expanded plan hold
     * 3 × 10 000 groups × 10 entries × 16 bytes ≈ 5 MB, well within the 60% of
     * 512 MB request circuit breaker (≈ 307 MB).
     * <p>
     * Each group has exactly one document per time bucket (a ∈ [0,9] maps to one day each).
     * The inner aggregate for s0 is SUM(SIN(a * 1)) = SIN(a), so the sparkline values are
     * [SIN(0), SIN(1), …, SIN(9)].
     */
    public void testSparklineAggWithManyLongs() throws IOException {
        initManyLongs(10);
        Map<String, Object> result = sparklineFromMany(2, 10);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "s0").entry("type", "double"))
            .item(matchesMap().entry("name", "s1").entry("type", "double"))
            .item(matchesMap().entry("name", "b").entry("type", "long"))
            .item(matchesMap().entry("name", "c").entry("type", "long"))
            .item(matchesMap().entry("name", "d").entry("type", "long"))
            .item(matchesMap().entry("name", "e").entry("type", "long"));
        assertResultMap(result, columns, hasSize(10_000));
        // Spot-check: s0 = SUM(SIN(a * 1)) = SIN(a); one doc per bucket where `a` equals
        // the bucket index, so the sorted sparkline starts [SIN(0), SIN(1), …, SIN(9)].
        // SparklineGenerateEmptyBuckets uses "≤ maxDate" when iterating, so the to-date
        // boundary ("1970-01-11") is included as a trailing empty (0) bucket — we check
        // the first 10 values only.
        List<?> rows = (List<?>) result.get("values");
        List<?> sparkline = (List<?>) ((List<?>) rows.getFirst()).getFirst();
        for (int i = 0; i < 10; i++) {
            assertThat(((Number) sparkline.get(i)).doubleValue(), closeTo(Math.sin(i), 1e-10));
        }
    }

    /**
     * Tests that too may SPARKLINEs trips the circuit breaker.
     */
    public void testSparklineAggWithTooManyLongs() throws IOException {
        initManyLongs(10);
        assertCircuitBreaks(attempt -> sparklineFromMany(attempt * 50, 10));
    }

    /**
     * Tests that few SPARKLINEs with too many buckets trips the circuit breaker.
     */
    public void testSparklineAggWithTooManyBuckets() throws IOException {
        initManyLongs(10);
        assertCircuitBreaks(attempt -> sparklineFromMany(50, attempt * 10));
    }

    private Map<String, Object> sparklineFromMany(int numSparklines, int bucketCount) throws IOException {
        logger.info("running {} SPARKLINE columns with {} buckets over 10k groups", numSparklines, bucketCount);
        StringBuilder query = startQuery();
        query.append("FROM manylongs\\n");
        query.append("| EVAL ts = TO_DATETIME(a * 86400000)\\n");
        query.append("| STATS ");
        for (int s = 0; s < numSparklines; s++) {
            if (s != 0) {
                query.append(", ");
            }

            // Each column uses SUM(SIN(a * (s+1))) so no two sparklines share the same
            // inner aggregate expression; this prevents common-subexpression elimination
            // from collapsing all TOP accumulators into one.
            query.append("s").append(s).append(" = SPARKLINE(");
            query.append("SUM(SIN(a * ").append(s + 1).append(")), ");
            query.append("ts, ");
            query.append(bucketCount).append(", ");
            query.append("\\\"1970-01-01\\\", \\\"1970-01-11\\\")");
        }
        query.append("\\n BY b, c, d, e");
        query.append("\\n| LIMIT 10000\"}");
        return responseAsMap(query(query.toString(), null));
    }
}
