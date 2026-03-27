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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.compute.operator.GroupKeyEncoder;
import org.elasticsearch.compute.operator.GroupedLimitOperator;
import org.elasticsearch.compute.operator.topn.GroupedTopNOperator;
import org.elasticsearch.compute.operator.topn.TopNQueue;
import org.elasticsearch.compute.operator.topn.TopNRow;
import org.elasticsearch.swisshash.BytesRefSwissHash;
import org.elasticsearch.test.ListMatcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;

/**
 * Heap-attack tests for {@code LIMIT BY} ({@code GroupedLimitOperator})
 * and {@code SORT | LIMIT BY} ({@code GroupedTopNOperator}).
 * Each test exercises a distinct circuit-breaking path inside its respective operator.
 */
@TimeoutSuite(millis = 5 * TimeUnits.MINUTE)
public class HeapAttackLimitByIT extends HeapAttackTestCase {

    // -------------------------------------------------------------------------
    // LIMIT BY — GroupedLimitOperator
    // -------------------------------------------------------------------------

    /**
     * This limits by 100 computed columns which should succeed. The GroupedLimitOperator stores group
     * keys in a hash table; 100K unique groups with 105 columns each is well within the circuit breaker.
     */
    public void testLimitBySomeLongs() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(10);
        Map<String, Object> result = limitByManyLongs(100);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertResultMap(result, columns, values);
    }

    /**
     * The GroupedLimitOperator stores all group keys in a hash table. Grouping by all 5 original
     * fields (a,b,c,d,e) gives 100K unique groups; adding many computed columns widens each key
     * until the hash table trips the circuit breaker.
     */
    public void testLimitByTooMuchMemory() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(10);
        assertCircuitBreaksVia(attempt -> limitByManyLongs(attempt * 1000), GroupedLimitOperator.class, BytesRefSwissHash.class);
    }

    private Map<String, Object> limitByManyLongs(int count) throws IOException {
        logger.info("limit by {} group cols", count);
        StringBuilder query = makeManyLongs(count);
        // Group by all 5 original fields so there are 10^5 = 100K unique groups,
        // then also include the computed fields to widen each group key.
        query.append("| LIMIT 1 BY a, b, c, d, e, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| STATS MAX(a)\"}");
        return responseAsMap(query(query.toString(), null));
    }

    /**
     * Grouping by 50 repeated 1MB strings should succeed. The {@code GroupKeyEncoder} scratch
     * buffer grows to 50MB; combined with 50MB from REPEAT scratch buffers and 50MB of block
     * storage, this stays comfortably within the circuit breaker.
     */
    public void testLimitByKeyEncoderDoesNotCircuitBreak() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initSingleDocIndex();
        Map<String, Object> result = limitByManyStrings(50);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"));
        assertResultMap(result, columns, matchesList().item(List.of(1)));
    }

    /**
     * The {@code GroupKeyEncoder} encodes group key columns into a {@code BreakingBytesRefBuilder}
     * scratch buffer (labeled {@code "group-key-encoder"}) which is charged to the circuit breaker
     * and never released between rows. By grouping by many wide strings, the scratch accumulates
     * past the circuit breaker limit independently of how many rows or unique groups there are.
     * <p>
     * Memory model with N columns of 1MB each (one document):
     * <ul>
     *   <li>N REPEAT scratch buffers (labeled {@code "repeat"}): N MB</li>
     *   <li>N keyword blocks: N MB</li>
     *   <li>keyEncoder scratch (accumulated during encoding): up to N MB</li>
     * </ul>
     * EVAL succeeds while 2N &lt; limit, then the keyEncoder scratch pushes 2N + K over the limit
     * on the K-th column encoded. The window 103 ≤ N ≤ 153 satisfies both constraints at once.
     */
    public void testLimitByKeyEncoderTooMuchMemory() throws IOException {
        assumeTrue("LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initSingleDocIndex();
        assertCircuitBreaksVia(attempt -> limitByManyStrings(attempt * 110), GroupedLimitOperator.class, GroupKeyEncoder.class);
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

    // -------------------------------------------------------------------------
    // SORT | LIMIT BY — GroupedTopNOperator
    // -------------------------------------------------------------------------

    /**
     * This sorts by 500 columns then limits 1000 rows per group, which should succeed. The sort keys
     * are stored per row in the GroupedTopNOperator queues; 500 columns × 10K rows is within limits.
     */
    public void testTopNBySomeLongs() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(10);
        Map<String, Object> result = topNByManyLongs(500);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "b").entry("type", "long"));
        assertResultMap(result, columns, any(List.class));
    }

    /**
     * The GroupedTopNOperator stores full sort keys for every row in its per-group queues. Sorting by
     * many columns means each stored row is very wide; with 1000 rows per group the total memory
     * trips the circuit breaker.
     */
    public void testTopNByTooMuchMemory() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(10);
        assertCircuitBreaksVia(attempt -> topNByManyLongs(attempt * 5000), GroupedTopNOperator.class, TopNRow.class);
    }

    private Map<String, Object> topNByManyLongs(int count) throws IOException {
        logger.info("topn by with {} sort cols", count);
        StringBuilder query = makeManyLongs(count);
        // Sort by all computed columns so they cannot be column-pruned and must be
        // stored as sort keys in GroupedTopNOperator's per-group TopNQueues.
        query.append("| SORT a, b, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| LIMIT 1000 BY a\\n| KEEP a, b\"}");
        return responseAsMap(query(query.toString(), null));
    }

    /**
     * This sorts by 1 column then limits 1 row per group across 100K unique groups with 100 group
     * key columns, which should succeed. The GroupedTopNOperator stores group keys in a hash table;
     * 100K groups with 105-column keys is within the circuit breaker.
     */
    public void testTopNByWideGroupKeySomeLongs() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(10);
        Map<String, Object> result = topNByWideGroupKey(100);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "b").entry("type", "long"));
        assertResultMap(result, columns, any(List.class));
    }

    /**
     * The GroupedTopNOperator stores group keys in a hash table ({@code keysHash}). Grouping by
     * all 5 original fields plus many computed columns widens each encoded group key until the
     * hash table trips the circuit breaker. Unlike {@link #testTopNByTooMuchMemory} which stresses
     * sort key row storage, this stresses the group key hash table.
     */
    public void testTopNByWideGroupKeyTooMuchMemory() throws IOException {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", Build.current().isSnapshot());
        initManyLongs(10);
        assertCircuitBreaksVia(attempt -> topNByWideGroupKey(attempt * 1000), GroupedTopNOperator.class, BytesRefSwissHash.class);
    }

    private Map<String, Object> topNByWideGroupKey(int count) throws IOException {
        logger.info("topn by with {} group key cols", count);
        StringBuilder query = makeManyLongs(count);
        // Sort by a single column to keep TopNRow narrow; group by all 5 original fields
        // plus computed columns to widen the composite group key stored in keysHash.
        // 100K unique groups (a,b,c,d,e ∈ [0,9]) × wide encoded keys trips the circuit breaker.
        query.append("| SORT a\\n");
        query.append("| LIMIT 1 BY a, b, c, d, e, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| KEEP a, b\"}");
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
        assertCircuitBreaksVia(attempt -> topNByManyGroups(attempt * 400), GroupedTopNOperator.class, TopNQueue.class);
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
     * Asserts that the given operation eventually trips the circuit breaker via the expected code
     * path, as confirmed by all {@code classes} appearing in the exception's stack trace.
     * <p>
     * Unlike the base {@link #assertCircuitBreaks} which stops on the first circuit-breaking
     * exception regardless of origin, this method continues to the next attempt if the exception
     * came from a different part of the pipeline (e.g. an upstream {@code LIMIT} tripping before
     * the operator under test). Each attempt scales the load, so a later attempt is more likely
     * to reach the operator under test.
     */
    private void assertCircuitBreaksVia(TryCircuitBreaking tryBreaking, Class<?>... classes) throws IOException {
        List<String> classNames = Arrays.stream(classes).map(Class::getName).collect(Collectors.toList());
        int attempt = 1;
        while (attempt <= MAX_ATTEMPTS) {
            try {
                Map<String, Object> response = tryBreaking.attempt(attempt);
                logger.warn("{}: should circuit broken but got {}", attempt, response);
            } catch (ResponseException e) {
                Map<?, ?> map = responseAsMap(e.getResponse());
                Object error = map.get("error");
                if (error instanceof Map<?, ?> errorMap
                    && "circuit_breaking_exception".equals(errorMap.get("type"))
                    && errorMap.get("stack_trace") instanceof String stackTrace
                    && classNames.stream().allMatch(stackTrace::contains)) {
                    assertMap(
                        map,
                        matchesMap().entry("status", 429)
                            .entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
                    );
                    return;
                }
                logger.warn("{}: circuit broke but not via expected classes {}: {}", attempt, classNames, map);
            }
            attempt++;
        }
        fail("giving up after " + attempt + " attempts waiting for circuit break via " + classNames);
    }
}
