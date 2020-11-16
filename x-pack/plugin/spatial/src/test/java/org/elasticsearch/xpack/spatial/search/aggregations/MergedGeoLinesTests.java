/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MergedGeoLinesTests extends ESTestCase {

    public InternalGeoLine randomLine(SortOrder sortOrder, int maxLength) {
        String name = randomAlphaOfLength(5);
        int length = randomBoolean() ? maxLength : randomIntBetween(1, maxLength);
        boolean complete = length < maxLength;
        long[] points = new long[length];
        double[] sortValues = new double[length];
        int randomIncrement = randomBoolean() ? randomIntBetween(1, 5) : 0;
        for (int i = 0; i < length; i++) {
            points[i] = randomIntBetween(1, 100);
            sortValues[i] = (i + 1) * 2 + randomIncrement;
        }
        return new InternalGeoLine(name, points, sortValues, Collections.emptyMap(), complete, randomBoolean(), sortOrder, maxLength);
    }

    public void testSimpleMerge() {
        int numLines = 100;
        int maxLength = 500;
        int finalLength = 0;
        SortOrder sortOrder = SortOrder.ASC;
        List<InternalGeoLine> geoLines = new ArrayList<>();
        for (int i = 0; i < numLines; i++) {
            geoLines.add(randomLine(sortOrder, maxLength));
            finalLength += geoLines.get(i).length();
        }
        finalLength = Math.min(maxLength, finalLength);
        MergedGeoLines mergedGeoLines = new MergedGeoLines(geoLines, finalLength, sortOrder);
        mergedGeoLines.merge();
        double[] sortedValues = Arrays.copyOf(mergedGeoLines.getFinalSortValues(), mergedGeoLines.getFinalSortValues().length);
        Arrays.sort(sortedValues);
        assertArrayEquals(sortedValues, mergedGeoLines.getFinalSortValues(), 0d);
    }
}
