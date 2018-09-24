/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.xpack.core.ml.filestructurefinder.FieldStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Calculate statistics for a set of scalar field values.
 * Count, cardinality (distinct count) and top hits (most common values) are always calculated.
 * Extra statistics are calculated if the field is numeric: min, max, mean and median.
 */
public class FieldStatsCalculator {

    private long count;
    private SortedMap<String, Integer> countsByStringValue = new TreeMap<>();
    private SortedMap<Double, Integer> countsByNumericValue = new TreeMap<>();

    /**
     * Add a collection of values to the calculator.
     * The values to be added can be combined by the caller and added in a
     * single call to this method or added in multiple calls to this method.
     * @param fieldValues Zero or more values to add.  May not be <code>null</code>.
     */
    public void accept(Collection<String> fieldValues) {

        count += fieldValues.size();

        for (String fieldValue : fieldValues) {

            countsByStringValue.compute(fieldValue, (k, v) -> (v == null) ? 1 : (1 + v));

            if (countsByNumericValue != null) {

                try {
                    countsByNumericValue.compute(Double.valueOf(fieldValue), (k, v) -> (v == null) ? 1 : (1 + v));
                } catch (NumberFormatException e) {
                    countsByNumericValue = null;
                }
            }
        }
    }

    /**
     * Calculate field statistics based on the previously accepted values.
     * @param numTopHits The maximum number of entries to include in the top hits.
     * @return The calculated field statistics.
     */
    public FieldStats calculate(int numTopHits) {

        if (countsByNumericValue != null && countsByNumericValue.isEmpty() == false) {
            return new FieldStats(count, countsByNumericValue.size(), countsByNumericValue.firstKey(), countsByNumericValue.lastKey(),
                calculateMean(), calculateMedian(), findNumericTopHits(numTopHits));
        } else {
            return new FieldStats(count, countsByStringValue.size(), findStringTopHits(numTopHits));
        }
    }

    Double calculateMean() {

        assert countsByNumericValue != null;

        if (countsByNumericValue.isEmpty()) {
            return null;
        }

        double runningCount = 0.0;
        double runningMean = Double.NaN;

        for (Map.Entry<Double, Integer> entry : countsByNumericValue.entrySet()) {

            double entryCount = (double) entry.getValue();
            double newRunningCount = runningCount + entryCount;

            // Updating a running mean like this is more numerically stable than using (sum / count)
            if (runningCount > 0.0) {
                runningMean = runningMean * (runningCount / newRunningCount) + entry.getKey() * (entryCount / newRunningCount);
            } else if (entryCount > 0.0) {
                runningMean = entry.getKey();
            }

            runningCount = newRunningCount;
        }

        return runningMean;
    }

    Double calculateMedian() {

        assert countsByNumericValue != null;

        if (count % 2 == 1) {

            // Simple case - median is middle value
            long targetCount = count / 2 + 1;
            long currentUpperBound = 0;

            for (Map.Entry<Double, Integer> entry : countsByNumericValue.entrySet()) {

                currentUpperBound += entry.getValue();

                if (currentUpperBound >= targetCount) {
                    return entry.getKey();
                }
            }

        } else {

            // More complicated case - median is average of two middle values
            long target1Count = count / 2;
            long target2Count = target1Count + 1;
            double target1Value = Double.NaN;
            long prevUpperBound = -1;
            long currentUpperBound = 0;

            for (Map.Entry<Double, Integer> entry : countsByNumericValue.entrySet()) {

                currentUpperBound += entry.getValue();

                if (currentUpperBound >= target2Count) {

                    if (prevUpperBound < target1Count) {
                        // Both target values are the same
                        return entry.getKey();
                    } else {
                        return (target1Value + entry.getKey()) / 2.0;
                    }
                }

                if (currentUpperBound >= target1Count) {
                    target1Value = entry.getKey();
                }

                prevUpperBound = currentUpperBound;
            }
        }

        return null;
    }

    List<Map<String, Object>> findNumericTopHits(int numTopHits) {
        assert countsByNumericValue != null;
        return findTopHits(numTopHits, countsByNumericValue, Comparator.comparing(Map.Entry<Double, Integer>::getKey),
            FieldStats::toIntegerIfInteger);
    }

    List<Map<String, Object>> findStringTopHits(int numTopHits) {
        return findTopHits(numTopHits, countsByStringValue, Comparator.comparing(Map.Entry<String, Integer>::getKey), s -> s);
    }

    /**
     * Order by descending count, with a secondary sort to ensure reproducibility of results.
     */
    private static <T> List<Map<String, Object>> findTopHits(int numTopHits, Map<T, Integer> countsByValue,
                                                             Comparator<Map.Entry<T, Integer>> secondarySort,
                                                             Function<T, Object> outputMapper) {

        List<Map.Entry<T, Integer>> sortedByCount = countsByValue.entrySet().stream()
            .sorted(Comparator.comparing(Map.Entry<T, Integer>::getValue, Comparator.reverseOrder()).thenComparing(secondarySort))
            .limit(numTopHits).collect(Collectors.toList());

        List<Map<String, Object>> topHits = new ArrayList<>(sortedByCount.size());

        for (Map.Entry<T, Integer> entry : sortedByCount) {

            Map<String, Object> topHit = new LinkedHashMap<>(3);
            topHit.put("value", outputMapper.apply(entry.getKey()));
            topHit.put("count", entry.getValue());
            topHits.add(topHit);
        }

        return topHits;
    }
}
