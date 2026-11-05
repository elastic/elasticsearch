/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.ListMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Heap-attack tests for the FIRST, LAST and VALUES aggregations.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackFirstLastValuesIT extends HeapAttackTestCase {

    /**
     * Tests that FIRST agg with a large grouping state trips the circuit breaker.
     * We don't require many fields in the index. However, for the small number of fields that we have, we want them to have a large
     * number of multivalues. We also require a large number of documents. Further, our query groups by a unique id field, in order to
     * generate a large of buckets. All the aforementioned participates in inflating the size of the grouping state.
     */
    public void testFirstAggWithManyLongs() throws IOException {
        initMvLongsIndex(scaleToHeap(5000), 2, 3000, true);
        assertCircuitBreaks(attempt -> aggregateByIdOnManyLongs("FIRST"));
    }

    /**
     * Tests that LAST agg with a large grouping state trips the circuit breaker.
     * @throws IOException
     * @see #testFirstAggWithManyLongs()
     */
    public void testLastAggWithManyLongs() throws IOException {
        initMvLongsIndex(scaleToHeap(5000), 2, 3000, true);
        assertCircuitBreaks(attempt -> aggregateByIdOnManyLongs("LAST"));
    }

    /**
     * Tests that FIRST agg with huge text fields trips the circuit breaker.
     * @throws IOException
     * @see #testFirstAggWithManyLongs()
     */
    public void testFirstAggWithGiantText() throws IOException {
        initGiantTextField(scaleToHeap(50), true, 3);
        assertCircuitBreaks(attempt -> aggregateByIdOnLargeText("FIRST"));
    }

    /**
     * Tests that LAST agg with huge text fields trips the circuit breaker.
     * @throws IOException
     * @see #testFirstAggWithManyLongs()
     */
    public void testLastAggWithGiantText() throws IOException {
        initGiantTextField(scaleToHeap(50), true, 3);
        assertCircuitBreaks(attempt -> aggregateByIdOnLargeText("LAST"));
    }

    /**
     * Tests that VALUES(long) agg with a large grouping state just small enough
     * not to trip the breaker.
     */
    public void testValuesAggWithManyLongs() throws IOException {
        int countPer = 10;
        int echoFactor = 10;
        initManyLongs(countPer);
        Map<String, Object> result = valuesFromMany(false, countPer, echoFactor);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "VALUES(vv)").entry("type", "long"));
        assertMap(result, matchesMap().entry("columns", columns).entry("values", hasSize(1)));
        List<?> values = (List<?>) result.get("values");
        values = (List<?>) values.getFirst();
        values = (List<?>) values.getFirst();
        long[] sortedValues = values.stream().mapToLong(o -> ((Number) o).longValue()).sorted().toArray();
        int size = countPer * countPer * countPer * countPer * countPer * (1 + 3 * echoFactor);
        assertThat(sortedValues.length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(sortedValues[i], equalTo((long) i));
        }
    }

    /**
     * Tests that VALUES(long) agg with a large grouping state trips the circuit breaker.
     */
    public void testValuesAggWithTooManyLongs() throws IOException {
        int countPer = 10;
        initManyLongs(countPer);
        assertCircuitBreaks(attempt -> valuesFromMany(false, countPer, attempt * 20));
    }

    /**
     * Tests that VALUES(keyword) agg with a large grouping state just small enough
     * not to trip the breaker.
     */
    public void testValuesAggWithManyKeywords() throws IOException {
        int countPer = 10;
        int echoFactor = 5;
        initManyLongs(countPer);

        Map<String, Object> result = valuesFromMany(true, countPer, echoFactor);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "VALUES(vv)").entry("type", "keyword"));
        assertMap(result, matchesMap().entry("columns", columns).entry("values", hasSize(1)));
        List<?> values = (List<?>) result.get("values");
        values = (List<?>) values.getFirst();
        values = (List<?>) values.getFirst();
        List<String> sortedValues = values.stream().map(o -> (String) o).sorted().toList();
        int fromCounts = countPer * countPer * countPer * countPer * countPer;
        int echoMulti = 1 + 3 * echoFactor;
        int size = fromCounts * echoMulti;
        assertThat(sortedValues.size(), equalTo(size));
        List<String> expected = new ArrayList<>(size);
        for (int a = 0; a < echoMulti; a++) {
            for (int c = 0; c < fromCounts; c++) {
                expected.add("a".repeat(a) + c);
            }
        }
        Collections.sort(expected);
        for (int i = 0; i < size; i++) {
            assertThat(sortedValues.get(i), equalTo(expected.get(i)));
        }
    }

    /**
     * Tests that VALUES(keyword) agg with a large grouping state trips the circuit breaker.
     */
    public void testValuesAggWithTooManyKeywords() throws IOException {
        int countPer = 10;
        initManyLongs(countPer);
        assertCircuitBreaks(attempt -> valuesFromMany(true, countPer, attempt * 10));
    }
}
