/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.compute.aggregation.GroupingAggregatorFunctionTestCase.matchingGroups;

public class FirstLastAggregatorTestingUtils {

    /**
     * Processes the input pages for the given group, and feeds the unpacked data into the manual aggregator.
     */
    public static void processPages(GroundTruthFirstLastAggregator work, List<Page> input, Long group) {
        for (Page page : input) {
            matchingGroups(page, group).forEach(p -> {
                Block values = page.getBlock(1);
                LongBlock timestamps = page.getBlock(2);
                Tuple<List<Long>, List<Object>> pair = unpack(timestamps, values, p);
                work.add(pair.v1(), pair.v2());
            });
        }
    }

    /**
     * Processes the input pages, and feeds the unpacked data into the manual aggregator.
     */
    public static void processPages(GroundTruthFirstLastAggregator work, List<Page> input) {
        for (Page page : input) {
            Block values = page.getBlock(0);
            LongBlock timestamps = page.getBlock(1);

            for (int p = 0; p < page.getPositionCount(); ++p) {
                Tuple<List<Long>, List<Object>> pair = unpack(timestamps, values, p);
                work.add(pair.v1(), pair.v2());
            }
        }
    }

    /**
     * Unpacks values of the passed two blocks, at the given position, into two lists. If there are no values for a particular block
     * at that position, the corresponding list is empty.
     */
    private static Tuple<List<Long>, List<Object>> unpack(LongBlock timestamps, Block values, int p) {
        // extract all timestamps at this position into a list
        List<Long> timestampsAtPosition = BlockTestUtils.valuesAtPosition(timestamps, p, true);

        // extract all values at this position into a list
        List<Object> valuesAtPosition = BlockTestUtils.valuesAtPosition(values, p, true);

        return Tuple.tuple(timestampsAtPosition, valuesAtPosition);
    }

    /**
     * This class servers as a manual aggregator for FIRST/LAST aggregator functions, that is used as the ground truth
     * during unit tests.
     */
    static class GroundTruthFirstLastAggregator {
        /**
         * A container for all values associated with the best timestamp, across all pages and positions. Elements within each list
         * represent the values found at a particular (block, position) combination, and can be empty if no values were found.
         * The lists are sorted before insertion into this set to allow for proper comparisons later during the check phase, which also
         * performs the same sorting operation on the result coming from the real aggregator.
         */
        private final Set<List<Object>> expectedValues = new HashSet<>();

        /**
         * Whether this is a FIRST or LAST aggregator
         */
        private final boolean first;

        /**
         * The best timestamp
         */
        private Long expectedTimestamp;

        /**
         * Whether an observation has been made
         */
        private boolean observed;

        GroundTruthFirstLastAggregator(boolean first) {
            this.first = first;
            this.observed = false;
        }

        /**
         * This method is called exactly once per position, as all timestamps and values for that position are passed in here.
         */
        void add(List<Long> timestamps, List<Object> values) {
            if (timestamps.isEmpty()) {
                // no-op: a null timestamp sorts last so it can't beat any existing timestamp
                return;
            }
            long timestamp = first ? Collections.min(timestamps) : Collections.max(timestamps);
            if (observed == false || (first ? timestamp < expectedTimestamp : timestamp > expectedTimestamp)) {
                expectedTimestamp = timestamp;
                expectedValues.clear();
                Collections.sort(values, null);
                expectedValues.add(values);
                observed = true;
            } else if (timestamp == expectedTimestamp) {
                Collections.sort(values, null);
                expectedValues.add(values);
            }
        }

        /**
         * Asserts that the passed value appears in the set of values we're expecting. Because the order in which pages are fed into the
         * real aggregators is not determined, this verifier only guarantees that "a" value that's associated with the absolute first/last
         * timestamp was returned.
         *
         * @param actual The value received from the real aggregator.
         */
        void check(Object actual) {
            if (expectedValues.isEmpty()) {
                if (actual != null) {
                    throw new AssertionError(String.format(Locale.ROOT, "Expected null but was %s", actual));
                }
            } else {
                List<Object> actualValues = new ArrayList<>();
                if (actual instanceof List<?> list) {
                    actualValues.addAll(list);
                    Collections.sort(actualValues, null);
                } else if (actual != null) {
                    actualValues.add(actual);
                }

                if (expectedValues.contains(actualValues) == false) {
                    throw new AssertionError(String.format(Locale.ROOT, "Expected %s but was %s.", expectedMessage(), actual));
                }
            }
        }

        private String expectedMessage() {
            if (expectedValues.size() == 1) {
                return expectedValues.iterator().next().toString();
            }
            if (expectedValues.size() > 10) {
                return "one of " + expectedValues.size() + " values";
            }
            return "one of " + expectedValues;
        }
    }
}
