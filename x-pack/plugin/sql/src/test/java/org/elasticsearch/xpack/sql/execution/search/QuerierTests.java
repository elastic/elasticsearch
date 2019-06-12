/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.execution.search.Querier.AggSortingQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class QuerierTests extends ESTestCase {

    @SuppressWarnings("rawtypes")
    public void testAggSortingAscending() {
        Tuple<Integer, Comparator> tuple = new Tuple<>(0, Comparator.naturalOrder());
        Querier.AggSortingQueue queue = new AggSortingQueue(10, Collections.singletonList(tuple));
        for (int i = 50; i >= 0; i--) {
            queue.insertWithOverflow(new Tuple<>(Collections.singletonList(i), i));
        }
        List<List<?>> results = queue.asList();

        assertEquals(10, results.size());
        for (int i = 0; i < 10; i ++) {
            assertEquals(i, results.get(i).get(0));
        }
    }

    @SuppressWarnings("rawtypes")
    public void testAggSortingDescending() {
        Tuple<Integer, Comparator> tuple = new Tuple<>(0, Comparator.reverseOrder());
        Querier.AggSortingQueue queue = new AggSortingQueue(10, Collections.singletonList(tuple));
        for (int i = 0; i <= 50; i++) {
            queue.insertWithOverflow(new Tuple<>(Collections.singletonList(i), i));
        }
        List<List<?>> results = queue.asList();

        assertEquals(10, results.size());
        for (int i = 0; i < 10; i ++) {
            assertEquals(50 - i, results.get(i).get(0));
        }
    }

    @SuppressWarnings("rawtypes")
    public void testAggSorting_TwoFields() {
        List<Tuple<Integer, Comparator>> tuples = new ArrayList<>(2);
        tuples.add(new Tuple<>(0, Comparator.reverseOrder()));
        tuples.add(new Tuple<>(1, Comparator.naturalOrder()));
        Querier.AggSortingQueue queue = new AggSortingQueue(10, tuples);

        for (int i = 1; i <= 100; i++) {
            queue.insertWithOverflow(new Tuple<>(Arrays.asList(i % 50 + 1, i), i));
        }
        List<List<?>> results = queue.asList();

        assertEquals(10, results.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(50 - (i / 2), results.get(i).get(0));
            assertEquals(49 - (i / 2) + ((i % 2) * 50), results.get(i).get(1));
        }
    }
}
