/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.xpack.core.ml.filestructurefinder.FieldStats;

import java.util.Arrays;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;

public class FieldStatsCalculatorTests extends FileStructureTestCase {

    public void testMean() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("1", "3.5", "2.5", "9"));

        assertEquals(4.0, calculator.calculateMean(), 1e-10);
    }

    public void testMedianGivenOddCount() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("3", "23", "-1", "5", "1000"));

        assertEquals(5.0, calculator.calculateMedian(), 1e-10);
    }

    public void testMedianGivenOddCountMinimal() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Collections.singletonList("3"));

        assertEquals(3.0, calculator.calculateMedian(), 1e-10);
    }

    public void testMedianGivenEvenCountMiddleValuesDifferent() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("3", "23", "-1", "5", "1000", "6"));

        assertEquals(5.5, calculator.calculateMedian(), 1e-10);
    }

    public void testMedianGivenEvenCountMiddleValuesSame() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("3", "23", "-1", "5", "1000", "5"));

        assertEquals(5.0, calculator.calculateMedian(), 1e-10);
    }

    public void testMedianGivenEvenCountMinimal() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("4", "4"));

        assertEquals(4.0, calculator.calculateMedian(), 1e-10);
    }

    public void testTopHitsNumeric() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("4", "4", "7", "4", "6", "5.2", "6", "5.2", "16", "4", "5.2"));

        List<Map<String, Object>> topHits = calculator.findNumericTopHits(3);

        assertEquals(3, topHits.size());
        assertEquals(4, topHits.get(0).get("value"));
        assertEquals(4, topHits.get(0).get("count"));
        assertEquals(5.2, topHits.get(1).get("value"));
        assertEquals(3, topHits.get(1).get("count"));
        assertEquals(6, topHits.get(2).get("value"));
        assertEquals(2, topHits.get(2).get("count"));
    }

    public void testTopHitsString() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("s", "s", "d", "s", "f", "x", "f", "x", "n", "s", "x"));

        List<Map<String, Object>> topHits = calculator.findStringTopHits(3);

        assertEquals(3, topHits.size());
        assertEquals("s", topHits.get(0).get("value"));
        assertEquals(4, topHits.get(0).get("count"));
        assertEquals("x", topHits.get(1).get("value"));
        assertEquals(3, topHits.get(1).get("count"));
        assertEquals("f", topHits.get(2).get("value"));
        assertEquals(2, topHits.get(2).get("count"));
    }

    public void testCalculateGivenEmpty() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Collections.emptyList());

        FieldStats stats = calculator.calculate(3);

        assertEquals(0L, stats.getCount());
        assertEquals(0, stats.getCardinality());
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertNull(stats.getMeanValue());
        assertNull(stats.getMedianValue());
        assertEquals(0, stats.getTopHits().size());
    }

    public void testCalculateGivenNumericField() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("4.5", "4.5", "7", "4.5", "6", "5", "6", "5", "25", "4.5", "5"));

        FieldStats stats = calculator.calculate(3);

        assertEquals(11L, stats.getCount());
        assertEquals(5, stats.getCardinality());
        assertEquals(4.5, stats.getMinValue(), 1e-10);
        assertEquals(25.0, stats.getMaxValue(), 1e-10);
        assertEquals(7.0, stats.getMeanValue(), 1e-10);
        assertEquals(5.0, stats.getMedianValue(), 1e-10);

        List<Map<String, Object>> topHits = stats.getTopHits();

        assertEquals(3, topHits.size());
        assertEquals(4.5, topHits.get(0).get("value"));
        assertEquals(4, topHits.get(0).get("count"));
        assertEquals(5, topHits.get(1).get("value"));
        assertEquals(3, topHits.get(1).get("count"));
        assertEquals(6, topHits.get(2).get("value"));
        assertEquals(2, topHits.get(2).get("count"));
    }

    public void testCalculateGivenStringField() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("s", "s", "d", "s", "f", "x", "f", "x", "n", "s", "x"));

        FieldStats stats = calculator.calculate(3);

        assertEquals(11L, stats.getCount());
        assertEquals(5, stats.getCardinality());
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertNull(stats.getMeanValue());
        assertNull(stats.getMedianValue());

        List<Map<String, Object>> topHits = stats.getTopHits();

        assertEquals(3, topHits.size());
        assertEquals("s", topHits.get(0).get("value"));
        assertEquals(4, topHits.get(0).get("count"));
        assertEquals("x", topHits.get(1).get("value"));
        assertEquals(3, topHits.get(1).get("count"));
        assertEquals("f", topHits.get(2).get("value"));
        assertEquals(2, topHits.get(2).get("count"));
    }

    public void testCalculateGivenMixedField() {

        FieldStatsCalculator calculator = new FieldStatsCalculator();

        calculator.accept(Arrays.asList("4", "4", "d", "4", "f", "x", "f", "x", "16", "4", "x"));

        FieldStats stats = calculator.calculate(3);

        assertEquals(11L, stats.getCount());
        assertEquals(5, stats.getCardinality());
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertNull(stats.getMeanValue());
        assertNull(stats.getMedianValue());

        List<Map<String, Object>> topHits = stats.getTopHits();

        assertEquals(3, topHits.size());
        assertEquals("4", topHits.get(0).get("value"));
        assertEquals(4, topHits.get(0).get("count"));
        assertEquals("x", topHits.get(1).get("value"));
        assertEquals(3, topHits.get(1).get("count"));
        assertEquals("f", topHits.get(2).get("value"));
        assertEquals(2, topHits.get(2).get("count"));
    }

    public void testJavaStatsEquivalence() {

        DoubleSummaryStatistics summaryStatistics = new DoubleSummaryStatistics();
        FieldStatsCalculator calculator = new FieldStatsCalculator();

        for (int numValues = randomIntBetween(1000, 10000); numValues > 0; --numValues) {

            double value = randomDouble();
            summaryStatistics.accept(value);
            calculator.accept(Collections.singletonList(Double.toString(value)));
        }

        FieldStats stats = calculator.calculate(1);

        assertEquals(summaryStatistics.getCount(), stats.getCount());
        assertEquals(summaryStatistics.getMin(), stats.getMinValue(), 1e-10);
        assertEquals(summaryStatistics.getMax(), stats.getMaxValue(), 1e-10);
        assertEquals(summaryStatistics.getAverage(), stats.getMeanValue(), 1e-10);
    }
}
