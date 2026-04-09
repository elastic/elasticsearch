/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.operator.GroupKeyEncoder;
import org.elasticsearch.compute.operator.GroupedLimitOperator;
import org.elasticsearch.compute.operator.topn.GroupedTopNOperator;
import org.elasticsearch.swisshash.BytesRefSwissHash;
import org.elasticsearch.test.ListMatcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;

/**
 * Heap-attack tests for {@code LIMIT BY} ({@code GroupedLimitOperator})
 * and {@code SORT | LIMIT BY} ({@code GroupedTopNOperator}).
 * Each test exercises a distinct circuit-breaking path inside its respective operator.
 */
@TimeoutSuite(millis = 5 * TimeUnits.MINUTE)
public class HeapAttackLimitByIT extends HeapAttackTestCase {

    /**
     * Lower the request circuit breaker to 40% of JVM heap (default is 60%). A tighter limit ensures
     * the breaker fires well before the JVM runs out of memory.
     *
     * We want to have some headroom between what the circuit breaker sees and the real free JVM heap:
     * <ul>
     *   <li> We need to discount size of cached pages in {@code PageCacheRecycler}.
     *        Those cached pages are invisible to the breaker but still consume heap. </li>
     *   <li> ES classpath also takes up heap space </li>
     *   <li> Memory fragmentation might cause the circuit breaker to think we can allocate
     *        an array when it's not possible to find a big enough gap in heap </li>
     * </ul>
     */
    @Before
    public void lowerRequestBreakerLimit() throws IOException {
        setRequestBreakerLimit("40%");
    }

    /**
     * Restores circuit breaker default limit
     */
    @After
    public void resetRequestBreakerLimit() throws IOException {
        setRequestBreakerLimit(null);
    }
    // -------------------------------------------------------------------------
    // LIMIT BY — GroupedLimitOperator
    // -------------------------------------------------------------------------

    /**
     * Grouping by a,b,c,d,e plus a 100-char string should succeed. 100K unique groups with
     * ~140-byte keys ≈ 10 MB in the hash table, well within the circuit breaker.
     */
    public void testLimitByHighCardinality() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongsAndString(10, 100);
        Map<String, Object> result = limitByWideStringKey(1);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertResultMap(result, columns, values);
    }

    /**
     * The GroupedLimitOperator stores all group keys in a hash table. With 100K unique groups
     * (a,b,c,d,e ∈ [0,9]) and a wide string column in the key, the hash table trips the circuit
     * breaker. {@code REPEAT(f, scale)} widens the string per attempt without needing many EVAL
     * columns: at scale=10 each key is ~4KB, so 100K groups ≈ 400 MB.
     */
    public void testLimitByHighCardinalityTooMuchMemory() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongsAndString(10, 100);
        assertCircuitBreaksVia(
            attempt -> limitByWideStringKey(attempt * 40),
            GroupedLimitOperator.class.getName(),
            bytesRefHashClassName()
        );
    }

    private Map<String, Object> limitByWideStringKey(int repeatScale) throws IOException {
        logger.info("limit by a,b,c,d,e + string repeated {}x", repeatScale);
        StringBuilder query = startQuery();
        query.append("FROM manylongsandstring\\n");
        if (repeatScale > 1) {
            query.append("| EVAL g = REPEAT(f, ").append(repeatScale).append(")\\n");
            query.append("| LIMIT 1 BY a, b, c, d, e, g\\n");
        } else {
            query.append("| LIMIT 1 BY a, b, c, d, e, f\\n");
        }
        query.append("| STATS MAX(a)\"}");
        return responseAsMap(query(query.toString(), null));
    }

    /**
     * Grouping by 30 repeated 1 MB strings should succeed. With the breaker at 40 % of 512 MB
     * (≈ 205 MB) and N = 30:
     * <ul>
     *   <li>REPEAT scratch buffers: ~34 MB (30 × 1.125 MB, oversized by 12.5 %)</li>
     *   <li>keyword blocks: ~30 MB</li>
     *   <li>keyEncoder scratch at peak grow: old ≈ 29 MB + new ≈ 34 MB</li>
     * </ul>
     * The peak tracked total (~127 MB) stays well within the 205 MB limit.
     */
    public void testLimitByKeyEncoderDoesNotCircuitBreak() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initSingleDocIndex();
        Map<String, Object> result = limitByManyStrings(30);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"));
        assertResultMap(result, columns, matchesList().item(List.of(1)));
    }

    /**
     * The {@code GroupKeyEncoder} encodes group key columns into a {@code BreakingBytesRefBuilder}
     * scratch buffer (labeled {@code "group-key-encoder"}) which is charged to the circuit breaker
     * and never released between rows. By grouping by many wide strings, the scratch accumulates
     * past the circuit breaker limit independently of how many rows or unique groups there are.
     * <p>
     * Memory model with N columns of 1 MB each (one document):
     * <ul>
     *   <li>N REPEAT scratch buffers (labeled {@code "repeat"}): ~1.125N MB (oversized by 12.5 %
     *       due to {@link org.apache.lucene.util.ArrayUtil#oversize})</li>
     *   <li>N keyword blocks: N MB</li>
     *   <li>keyEncoder scratch (accumulated during encoding): up to ~1.125N MB (also oversized)</li>
     * </ul>
     * EVAL succeeds while 2.125N &lt; limit (≈ 205 MB at 40 %), i.e. N ≤ 96.
     * Then the keyEncoder scratch pushes the total over the limit on the K-th column encoded.
     * During each grow the breaker sees both old and new array capacities (~2.25K), so the trip
     * condition is 2.125N + 2.25K &gt; limit. We use N = k = 80 which leaves ample JVM headroom:
     * at the trip point (~341 MB live heap) there are ~171 MB free in the 512 MB heap.
     */
    public void testLimitByKeyEncoderTooMuchMemory() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initSingleDocIndex();
        assertCircuitBreaksVia(
            attempt -> limitByManyStrings(attempt * 80),
            GroupedLimitOperator.class.getName(),
            GroupKeyEncoder.class.getName()
        );
    }

    private Map<String, Object> limitByManyStrings(int count) throws IOException {
        logger.info("limit by {} repeated 1MB string cols", count);
        // Each REPEAT(TO_STRING(a), 1_000_000) produces a 1MB string at execution time (a=1, so
        // TO_STRING(a)="1"). Using TO_STRING(a) rather than a literal prevents the optimizer from
        // constant-folding the REPEAT at plan time. The resulting 1MB string per column is kept in
        // a thread-local scratch buffer (labeled "repeat") for the duration of the query. Using them
        // as group keys forces the GroupKeyEncoder to copy each string into its own scratch buffer,
        // accumulating the charge column by column.
        StringBuilder query = startQuery();
        // REPEAT(TO_STRING(a), ...) instead of REPEAT("x", ...) prevents constant folding at plan
        // time: the string argument depends on a field so the optimizer cannot precompute the result.
        // At execution time TO_STRING(1) = "1" (1 byte), giving the same 1MB string per column.
        query.append("FROM single\\n| EVAL s0 = REPEAT(TO_STRING(a), 1000000)");
        for (int i = 1; i < count; i++) {
            query.append(", s").append(i).append(" = REPEAT(TO_STRING(a), 1000000)");
        }
        query.append("\\n| LIMIT 1 BY s0");
        for (int i = 1; i < count; i++) {
            query.append(", s").append(i);
        }
        query.append("\\n| KEEP a\"}");
        return responseAsMap(query(query.toString(), null));
    }

    /**
     * Unlike {@code SORT | LIMIT BY}, a near-{@code Integer.MAX_VALUE} per-group limit in
     * {@code LIMIT BY} does not cause excessive memory usage. {@code GroupedLimitOperator} is
     * a streaming filter that only stores one counter per unique group — the limit value itself
     * has no effect on memory.
     */
    public void testLimitByLargePerGroupCount() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(1);
        StringBuilder query = startQuery();
        query.append("FROM manylongs | LIMIT 2147483630 BY a | STATS MAX(a)\"}");
        Map<String, Object> result = responseAsMap(query(query.toString(), null));
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        assertResultMap(result, columns, any(List.class));
    }

    // -------------------------------------------------------------------------
    // SORT | LIMIT BY — GroupedTopNOperator
    // -------------------------------------------------------------------------

    /**
     * Sorting by 30 repeated 1 MB string columns should succeed. Each {@code TopNRow} stores
     * ~30 MB of encoded sort keys, which stays within the circuit breaker for a single row.
     */
    public void testTopNByManySortColumns() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initSingleDocIndex();
        Map<String, Object> result = topNByManyStringSortCols(30);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"));
        assertResultMap(result, columns, matchesList().item(List.of(1)));
    }

    /**
     * The GroupedTopNOperator stores full sort keys for every row in its per-group queues.
     * Sorting by many 1 MB string columns means each stored {@code TopNRow}'s encoded keys
     * buffer is very wide (encoding gets done by {@code TopNOperator.RowFiller}), tripping the
     * circuit breaker even with a single row.
     */
    public void testTopNByManySortColumnsTooMuchMemory() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initSingleDocIndex();
        assertCircuitBreaksVia(
            attempt -> topNByManyStringSortCols(attempt * 80),
            GroupedTopNOperator.class.getName(),
            "TopNOperator$RowFiller"
        );
    }

    private Map<String, Object> topNByManyStringSortCols(int count) throws IOException {
        logger.info("topn by with {} string sort cols", count);
        StringBuilder query = startQuery();
        query.append("FROM single\\n| EVAL s0 = REPEAT(TO_STRING(a), 1000000)");
        for (int i = 1; i < count; i++) {
            query.append(", s").append(i).append(" = REPEAT(TO_STRING(a), 1000000)");
        }
        // Sort by all string columns so they are stored as sort keys in each TopNRow.
        query.append("\\n| SORT s0");
        for (int i = 1; i < count; i++) {
            query.append(", s").append(i);
        }
        query.append("\\n| LIMIT 1 BY a\\n| KEEP a\"}");
        return responseAsMap(query(query.toString(), null));
    }

    /**
     * Same as {@link #testLimitByHighCardinality} but for {@code SORT | LIMIT BY}.
     * Sorting by a single column keeps {@code TopNRow} narrow; the hash table stores the wide
     * group keys. 100K groups with ~140-byte keys ≈ 10 MB, well within the circuit breaker.
     */
    public void testTopNByHighCardinality() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongsAndString(10, 100);
        Map<String, Object> result = topNByWideStringKey(1);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertResultMap(result, columns, values);
    }

    /**
     * Same as {@link #testLimitByHighCardinalityTooMuchMemory} but for {@code SORT | LIMIT BY}.
     * The {@code GroupedTopNOperator} stores group keys in a hash table ({@code keysHash}).
     * {@code REPEAT(f, scale)} widens the string per attempt: at scale=40 each key is ~4 KB,
     * so 100K groups ≈ 400 MB, tripping the circuit breaker.
     */
    public void testTopNByHighCardinalityTooMuchMemory() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongsAndString(10, 100);
        assertCircuitBreaksVia(attempt -> topNByWideStringKey(attempt * 40), GroupedTopNOperator.class.getName(), bytesRefHashClassName());
    }

    private Map<String, Object> topNByWideStringKey(int repeatScale) throws IOException {
        logger.info("topn by a,b,c,d,e + string repeated {}x", repeatScale);
        StringBuilder query = startQuery();
        query.append("FROM manylongsandstring\\n");
        if (repeatScale > 1) {
            query.append("| EVAL g = REPEAT(f, ").append(repeatScale).append(")\\n");
            query.append("| SORT a\\n| LIMIT 1 BY a, b, c, d, e, g\\n");
        } else {
            query.append("| SORT a\\n| LIMIT 1 BY a, b, c, d, e, f\\n");
        }
        query.append("| STATS MAX(a)\"}");
        return responseAsMap(query(query.toString(), null));
    }

    /**
     * Grouping by 30 repeated 1 MB strings with a preceding sort should succeed.
     * Same memory model as {@link #testLimitByKeyEncoderDoesNotCircuitBreak} (~127 MB peak)
     * but with {@code GroupedTopNOperator} rather than {@code GroupedLimitOperator}.
     */
    public void testTopNByKeyEncoderDoesNotCircuitBreak() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initSingleDocIndex();
        Map<String, Object> result = topNByManyStrings(30);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"));
        assertResultMap(result, columns, matchesList().item(List.of(1)));
    }

    /**
     * Same circuit-breaking path as {@link #testLimitByKeyEncoderTooMuchMemory} but triggered
     * inside {@code GroupedTopNOperator}: grouping by many wide strings causes the
     * {@code GroupKeyEncoder} scratch buffer to accumulate past the circuit-breaker limit.
     */
    public void testTopNByKeyEncoderTooMuchMemory() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initSingleDocIndex();
        assertCircuitBreaksVia(
            attempt -> topNByManyStrings(attempt * 80),
            GroupedTopNOperator.class.getName(),
            GroupKeyEncoder.class.getName()
        );
    }

    private Map<String, Object> topNByManyStrings(int count) throws IOException {
        logger.info("topn by with {} repeated 1MB string group key cols", count);
        StringBuilder query = startQuery();
        query.append("FROM single\\n| EVAL s0 = REPEAT(TO_STRING(a), 1000000)");
        for (int i = 1; i < count; i++) {
            query.append(", s").append(i).append(" = REPEAT(TO_STRING(a), 1000000)");
        }
        query.append("\\n| SORT a\\n");
        query.append("| LIMIT 1 BY s0");
        for (int i = 1; i < count; i++) {
            query.append(", s").append(i);
        }
        query.append("\\n| KEEP a\"}");
        return responseAsMap(query(query.toString(), null));
    }

    /**
     * This limits 10 rows per group across 100K unique groups, which should succeed.
     */
    public void testTopNByManyGroupsSmallTopCount() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(10);
        Map<String, Object> result = topNByManyGroups(10);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "b").entry("type", "long"));
        assertResultMap(result, columns, any(List.class));
    }

    /**
     * The GroupedTopNOperator creates a {@code TopNQueue}
     * for every group, and each queue pre-allocates a heap array of {@code topCount} slots.
     * With 100K unique groups (all combinations of a,b,c,d,e ∈ [0,9]) and a large per-group
     * limit, the combined queue allocations trip the circuit breaker even when the queues are
     * never filled.
     */
    public void testTopNByManyGroupsLargeTopCountTooMuchMemory() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(10);
        assertCircuitBreaksVia(
            attempt -> topNByManyGroups(attempt * 400),
            GroupedTopNOperator.class.getName(),
            "org.elasticsearch.compute.operator.topn.TopNQueue"
        );
    }

    private Map<String, Object> topNByManyGroups(int topCount) throws IOException {
        logger.info("topn by {} rows per group across 100K unique groups", topCount);
        // No EVAL needed: (a,b,c,d,e) ∈ [0,9]^5 gives 10^5 = 100K unique groups.
        // Sort by `a` to keep rows narrow; each group gets a TopNQueue pre-allocating
        // a heap array of topCount slots charged to the circuit breaker.
        StringBuilder query = startQuery();
        query.append("FROM manylongs\\n");
        query.append("| SORT a\\n");
        query.append("| LIMIT ").append(topCount).append(" BY a, b, c, d, e\\n");
        query.append("| KEEP a, b\"}");
        return responseAsMap(query(query.toString(), null));
    }

    /**
     * Like {@code testStupidTopN} in {@link HeapAttackIT}, but for {@code SORT | LIMIT BY}.
     * A near-{@code Integer.MAX_VALUE} per-group limit forces the first {@code TopNQueue}
     * to try to pre-allocate a ~16 GB heap array, which must trip the circuit breaker immediately.
     */
    public void testTopNByLargePerGroupCountTooMuchMemory() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(1);
        assertCircuitBreaksVia(attempt -> {
            StringBuilder query = startQuery();
            query.append("FROM manylongs | SORT a | LIMIT 2147483630 BY a | KEEP a\"}");
            return responseAsMap(query(query.toString(), null));
        }, GroupedTopNOperator.class.getName(), "org.elasticsearch.compute.operator.topn.TopNQueue");
    }

    /**
     * Returns the concrete {@link BytesRefSwissHash} class when swiss-table hashing is enabled,
     * or {@link BytesRefHash} otherwise. Use this when asserting circuit breaks that originate
     * from the group-key hash table.
     */
    private static String bytesRefHashClassName() {
        return HashImplFactory.SWISS_HASH_AVAILABLE ? BytesRefSwissHash.class.getName() : BytesRefHash.class.getName();
    }
}
