/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.compute.aggregation.GroupingAggregatorFunctionTestCase.matchingGroups;

public class AllFirstAllLastTestingUtils {

    /**
     * Processes the input pages for the given group, and feeds the unpacked data into the manual aggregator.
     */
    public static void processPages(GroundTruthFirstLastAggregator work, List<Page> input, Long group) {
        for (Page page : input) {
            matchingGroups(page, group).forEach(p -> {
                Block values = page.getBlock(1);
                LongBlock timestamps = page.getBlock(2);
                Tuple<List<Long>, List<Object>> pair = unpack(timestamps, values, p);
                work.addMv(pair.v1(), pair.v2());
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
                work.addMv(pair.v1(), pair.v2());
            }
        }
    }

    /**
     * Unpacks the timestamps and values, of the passed blocks at the given position, into two lists.
     */
    private static Tuple<List<Long>, List<Object>> unpack(LongBlock timestamps, Block values, int p) {
        List<Long> timestampsList = new ArrayList<>();
        List<Object> valuesList = new ArrayList<>();

        // extract all timestamps to the list
        int tsStart = timestamps.getFirstValueIndex(p);
        int tsEnd = tsStart + timestamps.getValueCount(p);
        for (int tsOffset = tsStart; tsOffset < tsEnd; tsOffset++) {
            long t = timestamps.getLong(tsOffset);
            timestampsList.add(t);
        }

        // extract all values to the list
        int vStart = values.getFirstValueIndex(p);
        int vEnd = vStart + values.getValueCount(p);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
            if (values instanceof IntBlock b) {
                valuesList.add(b.getInt(vOffset));
            } else if (values instanceof LongBlock b) {
                valuesList.add(b.getLong(vOffset));
            } else if (values instanceof DoubleBlock b) {
                valuesList.add(b.getDouble(vOffset));
            } else if (values instanceof FloatBlock b) {
                valuesList.add(b.getFloat(vOffset));
            } else if (values instanceof BytesRefBlock b) {
                BytesRef scratch = new BytesRef();
                b.getBytesRef(vOffset, scratch);
                valuesList.add(scratch);
            } else if (values instanceof BooleanBlock b) {
                valuesList.add(b.getBoolean(vOffset));
            } else {
                throw new IllegalStateException("Unsupported block type: " + values.getClass());
            }
        }

        return Tuple.tuple(timestampsList, valuesList);
    }

    /**
     * This class servers as a manual aggregator for ALL_FIRST/ALL_LAST aggregator functions, that is used as the ground truth
     * during unit tests.
     */
    static class GroundTruthFirstLastAggregator {
        // Affords us the ability to assert multiple expected values for a timestamp as long as they all appear in the same position.
        // Because the order in which pages are fed into the real aggregators is not determined, we can only guarantee "a" value that
        // is associated with the absolute first/last timestamp.
        private final List<Map<Object, Integer>> expectedValuesMv = new ArrayList<>();

        // Whether this is a FIRST or LAST aggregator
        private final boolean first;

        // The best timestamp
        private Long expectedTimestamp;

        // Whether an observation has been made
        private boolean observed;

        GroundTruthFirstLastAggregator(boolean first) {
            this.first = first;
            this.observed = false;
        }

        /**
         * This method is called just once per position, as all timestamps and values for that position are passed in here.
         */
        void addMv(List<Long> timestamps, List<Object> values) {
            if (timestamps.isEmpty()) {
                // no-op: a null timestamp sorts last so it can't beat any existing timestamp
                return;
            }
            long timestamp = first ? Collections.min(timestamps) : Collections.max(timestamps);
            if (observed == false || (first ? timestamp < expectedTimestamp : timestamp > expectedTimestamp)) {
                expectedTimestamp = timestamp;
                expectedValuesMv.clear();
                addHMvHelper(values);
                observed = true;
            } else if (timestamp == expectedTimestamp) {
                addHMvHelper(values);
            }
        }

        void addHMvHelper(List<Object> values) {
            Map<Object, Integer> map = new HashMap<>();

            if (values.isEmpty()) {
                map.put(null, 1);
                expectedValuesMv.add(map);
                return;
            }

            for (Object val : values) {
                int count = map.getOrDefault(val, 0);
                map.put(val, count + 1);
            }

            expectedValuesMv.add(map);
        }

        void checkMv(Object v) {
            if (expectedValuesMv.isEmpty()) {
                if (v != null) {
                    throw new AssertionError(String.format(Locale.ROOT, "Expected null but was %s", v));
                }
            } else {
                if (v instanceof List<?> list) {
                    // List<Long> longs = list.stream().map(e -> (Long)e).toList();
                    // agg function returned multi-values
                    Map<Object, Integer> actualMap = new HashMap<>();
                    for (Object item : list) {
                        int count = actualMap.getOrDefault(item, 0);
                        actualMap.put(item, count + 1);
                    }
                    if (checkMvHelper(actualMap) == false) {
                        throw new AssertionError(String.format(Locale.ROOT, "Expected %s but was %s.", expectedMessage(), v));
                    }
                } else {
                    // agg function returned a single value
                    if (checkMvHelper(v) == false) {
                        throw new AssertionError(String.format(Locale.ROOT, "Expected %s but was %s.", expectedMessage(), v));
                    }
                }
            }
        }

        private boolean checkMvHelper(Map<Object, Integer> actual) {
            boolean result = false;

            for (Map<Object, Integer> expected : expectedValuesMv) {

                if (actual.size() != expected.size()) {
                    continue;
                }

                boolean currentMapMatches = true;

                for (Object value : expected.keySet()) {
                    int expectedCount = expected.get(value);
                    int actualCount = actual.getOrDefault(value, -1);
                    if (expectedCount != actualCount) {
                        currentMapMatches = false;
                        break;
                    }
                }

                if (currentMapMatches) {
                    result = true;
                    break;
                }
            }

            return result;
        }

        private boolean checkMvHelper(Object v) {
            boolean result = false;

            for (Map<Object, Integer> expected : expectedValuesMv) {
                if (expected.size() == 1 && expected.containsKey(v)) {
                    result = true;
                    break;
                }
            }

            return result;
        }

        private String expectedMessage() {
            if (expectedValuesMv.size() == 1) {
                return expectedValuesMv.iterator().next().toString();
            }
            if (expectedValuesMv.size() > 10) {
                return "one of " + expectedValuesMv.size() + " values";
            }
            return "one of " + expectedValuesMv;
        }
    }
}
