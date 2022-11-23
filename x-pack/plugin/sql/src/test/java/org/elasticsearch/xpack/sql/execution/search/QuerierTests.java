/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.execution.search.Querier.AggSortingQueue;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.sql.execution.search.SearchHitCursorTests.randomHitExtractor;

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
        for (int i = 0; i < 10; i++) {
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
        for (int i = 0; i < 10; i++) {
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

    @SuppressWarnings("rawtypes")
    public void testAggSorting_TwoFields_One_Presorted() {
        List<Tuple<Integer, Comparator>> tuples = new ArrayList<>(2);
        tuples.add(new Tuple<>(0, null));
        tuples.add(new Tuple<>(1, Comparator.reverseOrder()));
        Querier.AggSortingQueue queue = new AggSortingQueue(20, tuples);

        for (int i = 1; i <= 100; i++) {
            queue.insertWithOverflow(new Tuple<>(Arrays.asList(i <= 5 ? null : 100 - i + 1, i), i));
        }
        List<List<?>> results = queue.asList();

        assertEquals(20, results.size());
        for (int i = 0; i < 20; i++) {
            assertEquals(i < 5 ? null : 100 - i, results.get(i).get(0));
            assertEquals(i < 5 ? 5 - i : i + 1, results.get(i).get(1));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testAggSorting_FourFields() {
        List<Comparator> comparators = Arrays.<Comparator>asList(
            Comparator.naturalOrder(),
            Comparator.naturalOrder(),
            Comparator.reverseOrder(),
            Comparator.naturalOrder()
        );
        List<Tuple<Integer, Comparator>> tuples = new ArrayList<>(4);
        tuples.add(new Tuple<>(0, null));
        tuples.add(new Tuple<>(1, comparators.get(1)));
        tuples.add(new Tuple<>(2, null));
        tuples.add(new Tuple<>(3, comparators.get(3)));
        Querier.AggSortingQueue queue = new AggSortingQueue(35, tuples);

        List<List<Integer>> expected = new ArrayList<>(128);
        for (int i = 0; i < 128; i++) {
            int col1 = i / 16;
            int col2 = 15 - (i / 8);
            int col3 = 32 - (i / 4);
            int col4 = 127 - i;

            expected.add(Arrays.asList(col1, col2, col3, col4));
            queue.insertWithOverflow(new Tuple<>(Arrays.asList(col1, col2, col3, col4), i));
        }

        expected.sort((o1, o2) -> {
            for (int i = 0; i < 4; i++) {
                int result = comparators.get(i).compare(o1.get(i), o2.get(i));
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        });
        List<List<?>> results = queue.asList();

        assertEquals(35, results.size());
        for (int i = 0; i < 35; i++) {
            for (int j = 0; j < 4; j++) {
                assertEquals(expected.get(i).get(j), results.get(i).get(j));
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public void testAggSorting_Randomized() {
        // Initialize comparators for fields (columns)
        int noColumns = randomIntBetween(3, 10);
        List<Tuple<Integer, Comparator>> tuples = new ArrayList<>(noColumns);
        boolean[] ordering = new boolean[noColumns];
        for (int j = 0; j < noColumns; j++) {
            boolean order = randomBoolean();
            ordering[j] = order;
            Comparator comp = order ? Comparator.naturalOrder() : Comparator.reverseOrder();
            tuples.add(new Tuple<>(j, comp));
        }

        // Insert random no of documents (rows) with random 0/1 values for each field
        int noDocs = randomIntBetween(10, 50);
        int queueSize = randomIntBetween(4, noDocs / 2);
        List<List<Integer>> expected = new ArrayList<>(noDocs);
        Querier.AggSortingQueue queue = new AggSortingQueue(queueSize, tuples);
        for (int i = 0; i < noDocs; i++) {
            List<Integer> values = new ArrayList<>(noColumns);
            for (int j = 0; j < noColumns; j++) {
                values.add(randomBoolean() ? 1 : 0);
            }
            queue.insertWithOverflow(new Tuple<>(values, i));
            expected.add(values);
        }

        List<List<?>> results = queue.asList();
        assertEquals(queueSize, results.size());
        expected.sort((o1, o2) -> {
            for (int j = 0; j < noColumns; j++) {
                if (o1.get(j) < o2.get(j)) {
                    return ordering[j] ? -1 : 1;
                } else if (o1.get(j) > o2.get(j)) {
                    return ordering[j] ? 1 : -1;
                }
            }
            return 0;
        });
        assertEquals(expected.subList(0, queueSize), results);
    }

    public void testFullQueueSortingOnLocalSort() {
        Tuple<Integer, Integer> actions = runLocalAggSorterWithNoLimit(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS);

        assertEquals("Exactly one response expected", 1, actions.v1().intValue());
        assertEquals("No failures expected", 0, actions.v2().intValue());
    }

    public void testQueueOverflowSortingOnLocalSort() {
        Tuple<Integer, Integer> actions = runLocalAggSorterWithNoLimit(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 2);

        assertEquals("No response expected", 0, actions.v1().intValue());
        assertEquals("Exactly one failure expected", 1, actions.v2().intValue());
    }

    Tuple<Integer, Integer> runLocalAggSorterWithNoLimit(int dataSize) {
        class TestResultRowSet<E extends NamedWriteable> extends ResultRowSet<E> implements SchemaRowSet {

            private int rowCounter = 0;
            private final int dataSize;

            TestResultRowSet(List<E> extractors, BitSet mask, int dataSize) {
                super(extractors, mask);
                this.dataSize = dataSize;
            }

            @Override
            protected Object extractValue(NamedWriteable namedWriteable) {
                return rowCounter++;
            }

            @Override
            protected boolean doHasCurrent() {
                return true;
            }

            @Override
            protected boolean doNext() {
                return rowCounter < dataSize;
            }

            @Override
            protected void doReset() {}

            @Override
            public Schema schema() {
                return new Schema(emptyList(), emptyList());
            }

            @Override
            public int size() {
                return dataSize; // irrelevant
            }
        }
        ;

        Cursor.Page page = new Cursor.Page(
            new TestResultRowSet<NamedWriteable>(List.of(randomHitExtractor(0)), new BitSet(), dataSize),
            Cursor.EMPTY
        );

        AtomicInteger responses = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        ActionListener<Cursor.Page> listener = new ActionListener<>() {
            @Override
            public void onResponse(Cursor.Page page) {
                responses.getAndIncrement();
            }

            @Override
            public void onFailure(Exception e) {
                failures.getAndIncrement();
            }
        };

        SqlSession session = new SqlSession(SqlTestUtils.TEST_CFG, null, null, null, null, null, null, null, null);
        Querier querier = new Querier(session);
        Querier.LocalAggregationSorterListener localSorter = querier.new LocalAggregationSorterListener(listener, emptyList(), -1);
        localSorter.onResponse(page);

        return new Tuple<>(responses.get(), failures.get());
    }
}
