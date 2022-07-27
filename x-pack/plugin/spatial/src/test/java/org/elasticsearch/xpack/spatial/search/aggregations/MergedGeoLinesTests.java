/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MergedGeoLinesTests extends ESTestCase {

    public InternalGeoLine randomLine(SortOrder sortOrder, int maxLength, double magicDecimal) {
        String name = randomAlphaOfLength(5);
        int length = randomBoolean() ? maxLength : randomIntBetween(1, maxLength);
        boolean complete = length <= maxLength;
        long[] points = new long[length];
        double[] sortValues = new double[length];
        for (int i = 0; i < length; i++) {
            points[i] = randomIntBetween(1, 100);
            sortValues[i] = i + magicDecimal;
        }
        return new InternalGeoLine(name, points, sortValues, Collections.emptyMap(), complete, randomBoolean(), sortOrder, maxLength);
    }

    public void testSimpleMerge() {
        int numLines = 10;
        int maxLength = 100;
        int finalLength = 0;
        SortOrder sortOrder = SortOrder.ASC;
        List<InternalGeoLine> geoLines = new ArrayList<>();
        for (int i = 0; i < numLines; i++) {
            geoLines.add(randomLine(sortOrder, maxLength, ((double) i) / numLines));
            finalLength += geoLines.get(i).length();
        }
        finalLength = Math.min(maxLength, finalLength);
        MergedGeoLines mergedGeoLines = new MergedGeoLines(geoLines, finalLength, sortOrder);
        mergedGeoLines.merge();

        // assert that the mergedGeoLines are sorted (does not necessarily validate correctness, but it is a good heuristic)
        long[] sortedPoints = Arrays.copyOf(mergedGeoLines.getFinalPoints(), mergedGeoLines.getFinalPoints().length);
        double[] sortedValues = Arrays.copyOf(mergedGeoLines.getFinalSortValues(), mergedGeoLines.getFinalSortValues().length);
        new PathArraySorter(sortedPoints, sortedValues, sortOrder).sort();
        assertArrayEquals(sortedValues, mergedGeoLines.getFinalSortValues(), 0d);
        assertArrayEquals(sortedPoints, mergedGeoLines.getFinalPoints());
    }

    public void testMergeWithEmptyGeoLine() {
        int maxLength = 10;
        SortOrder sortOrder = SortOrder.ASC;
        InternalGeoLine lineWithPoints = randomLine(sortOrder, maxLength, 0.0);
        InternalGeoLine emptyLine = new InternalGeoLine(
            "name",
            new long[] {},
            new double[] {},
            Collections.emptyMap(),
            true,
            randomBoolean(),
            sortOrder,
            maxLength
        );
        List<InternalGeoLine> geoLines = List.of(lineWithPoints, emptyLine);
        MergedGeoLines mergedGeoLines = new MergedGeoLines(geoLines, lineWithPoints.length(), sortOrder);
        mergedGeoLines.merge();

        // assert that the mergedGeoLines are sorted (does not necessarily validate correctness, but it is a good heuristic)
        long[] sortedPoints = Arrays.copyOf(mergedGeoLines.getFinalPoints(), mergedGeoLines.getFinalPoints().length);
        double[] sortedValues = Arrays.copyOf(mergedGeoLines.getFinalSortValues(), mergedGeoLines.getFinalSortValues().length);
        new PathArraySorter(sortedPoints, sortedValues, sortOrder).sort();
        assertArrayEquals(sortedValues, mergedGeoLines.getFinalSortValues(), 0d);
        assertArrayEquals(sortedPoints, mergedGeoLines.getFinalPoints());
    }
}
