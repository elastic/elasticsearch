/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class MergedGeoLinesTests extends ESTestCase {

    private InternalGeoLine randomLine(SortOrder sortOrder, int maxLength, double magicDecimal) {
        String name = randomAlphaOfLength(5);
        int length = randomBoolean() ? maxLength : randomIntBetween(1, maxLength);
        boolean complete = length <= maxLength;
        long[] points = new long[length];
        double[] sortValues = new double[length];
        for (int i = 0; i < length; i++) {
            points[i] = randomIntBetween(1, 100);
            sortValues[i] = i + magicDecimal;
        }
        return new InternalGeoLine(
            name,
            points,
            sortValues,
            Collections.emptyMap(),
            complete,
            randomBoolean(),
            sortOrder,
            maxLength,
            false,
            false
        );
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
        MergedGeoLines mergedGeoLines = new MergedGeoLines.Overlapping(geoLines, finalLength, sortOrder, false);
        mergedGeoLines.merge();

        // assert that the mergedGeoLines are sorted (does not necessarily validate correctness, but it is a good heuristic)
        long[] sortedPoints = Arrays.copyOf(mergedGeoLines.getFinalPoints(), mergedGeoLines.getFinalPoints().length);
        double[] sortedValues = Arrays.copyOf(mergedGeoLines.getFinalSortValues(), mergedGeoLines.getFinalSortValues().length);
        PathArraySorter.forOrder(sortOrder).apply(sortedPoints, sortedValues).sort();
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
            maxLength,
            false,
            false
        );
        List<InternalGeoLine> geoLines = List.of(lineWithPoints, emptyLine);
        MergedGeoLines mergedGeoLines = new MergedGeoLines.Overlapping(geoLines, lineWithPoints.length(), sortOrder, false);
        mergedGeoLines.merge();

        // assert that the mergedGeoLines are sorted (does not necessarily validate correctness, but it is a good heuristic)
        long[] sortedPoints = Arrays.copyOf(mergedGeoLines.getFinalPoints(), mergedGeoLines.getFinalPoints().length);
        double[] sortedValues = Arrays.copyOf(mergedGeoLines.getFinalSortValues(), mergedGeoLines.getFinalSortValues().length);
        PathArraySorter.forOrder(sortOrder).apply(sortedPoints, sortedValues).sort();
        assertArrayEquals(sortedValues, mergedGeoLines.getFinalSortValues(), 0d);
        assertArrayEquals(sortedPoints, mergedGeoLines.getFinalPoints());
    }

    public void testMergeNonOverlappingGeoLinesAppendOnly() {
        int docsPerLine = 10;
        int numLines = 10;
        int finalLength = docsPerLine * numLines; // all docs included, only append, no truncation
        for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
            boolean simplify = randomBoolean();
            List<InternalGeoLine> sorted = makeGeoLines(docsPerLine, numLines, simplify, sortOrder);
            // Shuffle to ensure the tests cover geo_lines coming from data nodes in random order
            List<InternalGeoLine> shuffled = shuffleGeoLines(sorted);
            MergedGeoLines mergedGeoLines = new MergedGeoLines.NonOverlapping(shuffled, finalLength, sortOrder, simplify);
            mergedGeoLines.merge();
            assertLinesTruncated(sorted, docsPerLine, finalLength, mergedGeoLines);
        }
    }

    public void testMergeNonOverlappingGeoLinesAppendAndTruncate() {
        int docsPerLine = 10;
        int numLines = 10;
        int finalLength = 25;  // should get two and a half geolines appended and truncated
        boolean simplify = false;
        for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
            List<InternalGeoLine> sorted = makeGeoLines(docsPerLine, numLines, simplify, sortOrder);
            // Shuffle to ensure the tests cover geo_lines coming from data nodes in random order
            List<InternalGeoLine> shuffled = shuffleGeoLines(sorted);
            MergedGeoLines mergedGeoLines = new MergedGeoLines.NonOverlapping(shuffled, finalLength, sortOrder, simplify);
            mergedGeoLines.merge();
            assertLinesTruncated(sorted, docsPerLine, finalLength, mergedGeoLines);
        }
    }

    public void testMergeNonOverlappingGeoLinesAppendAndSimplify() {
        int docsPerLine = 10;
        int numLines = 10;
        int finalLength = 25;  // should get entire 100 points simplified down to 25
        boolean simplify = true;
        for (SortOrder sortOrder : new SortOrder[] { SortOrder.ASC, SortOrder.DESC }) {
            List<InternalGeoLine> sorted = makeGeoLines(docsPerLine, numLines, simplify, sortOrder);
            // Shuffle to ensure the tests cover geo_lines coming from data nodes in random order
            List<InternalGeoLine> shuffled = shuffleGeoLines(sorted);
            MergedGeoLines mergedGeoLines = new MergedGeoLines.NonOverlapping(shuffled, finalLength, sortOrder, simplify);
            mergedGeoLines.merge();
            assertLinesSimplified(sorted, sortOrder, finalLength, mergedGeoLines);
        }
    }

    private void assertLinesTruncated(List<InternalGeoLine> lines, int docsPerLine, int finalLength, MergedGeoLines mergedGeoLines) {
        double[] values = mergedGeoLines.getFinalSortValues();
        long[] points = mergedGeoLines.getFinalPoints();
        assertThat("Same length arrays", values.length, equalTo(points.length));
        assertThat("Geoline is truncated", values.length, equalTo(finalLength));
        for (int i = 0; i < values.length; i++) {
            int doc = i % docsPerLine;
            int line = i / docsPerLine;
            InternalGeoLine original = lines.get(line);
            long[] originalPoints = original.line();
            double[] originalValues = original.sortVals();
            assertThat("Point at " + i + " same as point at (" + line + ":" + doc + ")", originalPoints[doc], equalTo(points[i]));
            assertThat("Value at " + i + " same as value at (" + line + ":" + doc + ")", originalValues[doc], equalTo(values[i]));
        }
    }

    private void assertLinesSimplified(List<InternalGeoLine> lines, SortOrder sortOrder, int finalLength, MergedGeoLines mergedGeoLines) {
        double[] values = mergedGeoLines.getFinalSortValues();
        long[] points = mergedGeoLines.getFinalPoints();
        assertThat("Same length arrays", values.length, equalTo(points.length));
        assertThat("Geo-line is simplified", values.length, equalTo(finalLength));
        GeoLineAggregatorTests.TestLine simplified = makeSimplifiedLine(lines, sortOrder, finalLength);
        assertThat("Geo-line is simplified to correct length", values.length, equalTo(simplified.sortValues.length));
        for (int i = 0; i < values.length; i++) {
            assertThat("Point at " + i, simplified.encodedPoints[i], equalTo(points[i]));
            assertThat("Value at " + i, simplified.sortValues[i], equalTo(values[i]));
        }
    }

    private GeoLineAggregatorTests.TestLine makeSimplifiedLine(List<InternalGeoLine> geoLines, SortOrder sortOrder, int finalLength) {
        TreeSet<InternalGeoLine> sorted = switch (sortOrder) {
            case DESC -> new TreeSet<>((o1, o2) -> Double.compare(o2.sortVals()[0], o1.sortVals()[0]));
            default -> new TreeSet<>(Comparator.comparingDouble(o -> o.sortVals()[0]));
        };
        sorted.addAll(geoLines);
        GeoLineAggregatorTests.TestGeometrySimplifierMonitor monitor = new GeoLineAggregatorTests.TestGeometrySimplifierMonitor();
        var simplifier = new GeoLineAggregatorTests.TestGeometrySimplifier(finalLength, monitor);
        int index = 0;
        for (InternalGeoLine geoLine : sorted) {
            double[] values = geoLine.sortVals();
            long[] points = geoLine.line();
            for (int i = 0; i < values.length; i++) {
                double x = GeoEncodingUtils.decodeLongitude((int) (points[i] >>> 32));
                double y = GeoEncodingUtils.decodeLatitude((int) (points[i] & 0xffffffffL));
                var point = new GeoLineAggregatorTests.TestSimplifiablePoint(index, x, y, values[i]);
                simplifier.consume(point);
                index++;
            }
        }
        assertThat("Simplifier added points", monitor.addedCount, CoreMatchers.equalTo(index));
        assertThat("Simplifier Removed points", monitor.removedCount, CoreMatchers.equalTo(index - finalLength));
        return simplifier.produce();
    }

    private List<InternalGeoLine> makeGeoLines(int docsPerLine, int numLines, boolean simplify, SortOrder sortOrder) {
        ArrayList<InternalGeoLine> lines = new ArrayList<>();
        NonOverlappingConfig lineConfig = new NonOverlappingConfig(docsPerLine, -50, -50, 1, 1, 1000, 10);
        while (lines.size() < numLines) {
            lines.add(lineConfig.makeGeoLine("test", sortOrder, simplify));
            lineConfig = lineConfig.nextNonOverlapping();
        }
        // Sort so we can make validity assertions
        List<InternalGeoLine> sorted = sortGeoLines(lines, sortOrder);
        for (int line = 1; line < sorted.size(); line++) {
            InternalGeoLine previous = sorted.get(line - 1);
            InternalGeoLine current = sorted.get(line);
            double[] pv = previous.sortVals();
            double[] cv = current.sortVals();
            assertThat("Previous line ordered", pv[0], compareTo(pv[pv.length - 1], sortOrder));
            assertThat("Current line ordered", cv[0], compareTo(cv[cv.length - 1], sortOrder));
            assertThat("Lines non-overlapping", pv[pv.length - 1], compareTo(cv[0], sortOrder));
        }
        return sorted;
    }

    private static Matcher<Double> compareTo(double v, SortOrder sortOrder) {
        if (sortOrder == SortOrder.ASC) {
            return lessThan(v);
        } else {
            return greaterThan(v);
        }
    }

    private static List<InternalGeoLine> sortGeoLines(List<InternalGeoLine> lines, SortOrder sortOrder) {
        TreeSet<InternalGeoLine> sorted = switch (sortOrder) {
            case DESC -> new TreeSet<>((o1, o2) -> Double.compare(o2.sortVals()[0], o1.sortVals()[0]));
            default -> new TreeSet<>(Comparator.comparingDouble(o -> o.sortVals()[0]));
        };
        // The Comparator above relies on each line having at least one point, so filter out empty geo_lines
        for (InternalGeoLine line : lines) {
            if (line.length() > 0) {
                sorted.add(line);
            }
        }
        return new ArrayList<>(sorted);
    }

    private static List<InternalGeoLine> shuffleGeoLines(List<InternalGeoLine> lines) {
        ArrayList<InternalGeoLine> shuffled = new ArrayList<>(lines);
        Collections.shuffle(shuffled, random());
        return shuffled;
    }

    private record NonOverlappingConfig(int docs, double startX, double startY, double dX, double dY, double startValue, double dV) {
        private NonOverlappingConfig withStartPosition(double startX, double startY, double startValue) {
            return new NonOverlappingConfig(docs, startX, startY, dX, dY, startValue, dV);
        }

        private NonOverlappingConfig nextNonOverlapping() {
            return withStartPosition(startX + docs * dX, startY + docs * dY, startValue + docs * dV);
        }

        private InternalGeoLine makeGeoLine(String name, SortOrder sortOrder, boolean simplified) {
            double[] values = new double[docs];
            long[] points = new long[docs];
            for (int i = 0; i < points.length; i++) {
                double x = startX + i * dX;
                double y = startY + i * dY;
                points[i] = (((long) GeoEncodingUtils.encodeLongitude(x)) << 32) | GeoEncodingUtils.encodeLatitude(y) & 0xffffffffL;
                values[i] = startValue + i * dV;
            }
            if (sortOrder == SortOrder.DESC) {
                // We created 'ASC' above, so reverse the arrays
                ArrayUtils.reverseSubArray(values, 0, values.length);
                ArrayUtils.reverseSubArray(points, 0, points.length);
            }
            return new InternalGeoLine(
                name,
                points,
                values,
                Collections.emptyMap(),
                randomBoolean(),
                randomBoolean(),
                sortOrder,
                docs,
                true,
                simplified
            );
        }
    }

}
