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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldStatsCalculatorTests extends FileStructureTestCase {

    private static final Map<String, String> LONG = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "long");
    private static final Map<String, String> DOUBLE = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "double");
    private static final Map<String, String> KEYWORD = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "keyword");

    public void testMean() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(DOUBLE);

        calculator.accept(Arrays.asList("1", "3.5", "2.5", "9"));

        assertEquals(4.0, calculator.calculateMean(), 1e-10);
    }

    public void testMedianGivenOddCount() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(LONG);

        calculator.accept(Arrays.asList("3", "23", "-1", "5", "1000"));

        assertEquals(5.0, calculator.calculateMedian(), 1e-10);
    }

    public void testMedianGivenOddCountMinimal() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(LONG);

        calculator.accept(Collections.singletonList("3"));

        assertEquals(3.0, calculator.calculateMedian(), 1e-10);
    }

    public void testMedianGivenEvenCountMiddleValuesDifferent() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(LONG);

        calculator.accept(Arrays.asList("3", "23", "-1", "5", "1000", "6"));

        assertEquals(5.5, calculator.calculateMedian(), 1e-10);
    }

    public void testMedianGivenEvenCountMiddleValuesSame() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(LONG);

        calculator.accept(Arrays.asList("3", "23", "-1", "5", "1000", "5"));

        assertEquals(5.0, calculator.calculateMedian(), 1e-10);
    }

    public void testMedianGivenEvenCountMinimal() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(LONG);

        calculator.accept(Arrays.asList("4", "4"));

        assertEquals(4.0, calculator.calculateMedian(), 1e-10);
    }

    public void testTopHitsNumeric() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(DOUBLE);

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

        FieldStatsCalculator calculator = new FieldStatsCalculator(KEYWORD);

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

        FieldStatsCalculator calculator =
            new FieldStatsCalculator(randomFrom(Arrays.asList(LONG, DOUBLE, KEYWORD, FileStructureUtils.DATE_MAPPING_WITHOUT_FORMAT)));

        calculator.accept(Collections.emptyList());

        FieldStats stats = calculator.calculate(3);

        assertEquals(0L, stats.getCount());
        assertEquals(0, stats.getCardinality());
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertNull(stats.getMeanValue());
        assertNull(stats.getMedianValue());
        assertNull(stats.getEarliestTimestamp());
        assertNull(stats.getLatestTimestamp());

        assertEquals(0, stats.getTopHits().size());
    }

    public void testCalculateGivenNumericField() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(DOUBLE);

        calculator.accept(Arrays.asList("4.5", "4.5", "7", "4.5", "6", "5", "6", "5", "25", "4.5", "5"));

        FieldStats stats = calculator.calculate(3);

        assertEquals(11L, stats.getCount());
        assertEquals(5, stats.getCardinality());
        assertEquals(4.5, stats.getMinValue(), 1e-10);
        assertEquals(25.0, stats.getMaxValue(), 1e-10);
        assertEquals(7.0, stats.getMeanValue(), 1e-10);
        assertEquals(5.0, stats.getMedianValue(), 1e-10);
        assertNull(stats.getEarliestTimestamp());
        assertNull(stats.getLatestTimestamp());

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

        FieldStatsCalculator calculator = new FieldStatsCalculator(KEYWORD);

        calculator.accept(Arrays.asList("s", "s", "d", "s", "f", "x", "f", "x", "n", "s", "x"));

        FieldStats stats = calculator.calculate(3);

        assertEquals(11L, stats.getCount());
        assertEquals(5, stats.getCardinality());
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertNull(stats.getMeanValue());
        assertNull(stats.getMedianValue());
        assertNull(stats.getEarliestTimestamp());
        assertNull(stats.getLatestTimestamp());

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

        FieldStatsCalculator calculator = new FieldStatsCalculator(KEYWORD);

        calculator.accept(Arrays.asList("4", "4", "d", "4", "f", "x", "f", "x", "16", "4", "x"));

        FieldStats stats = calculator.calculate(3);

        assertEquals(11L, stats.getCount());
        assertEquals(5, stats.getCardinality());
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertNull(stats.getMeanValue());
        assertNull(stats.getMedianValue());
        assertNull(stats.getEarliestTimestamp());
        assertNull(stats.getLatestTimestamp());

        List<Map<String, Object>> topHits = stats.getTopHits();

        assertEquals(3, topHits.size());
        assertEquals("4", topHits.get(0).get("value"));
        assertEquals(4, topHits.get(0).get("count"));
        assertEquals("x", topHits.get(1).get("value"));
        assertEquals(3, topHits.get(1).get("count"));
        assertEquals("f", topHits.get(2).get("value"));
        assertEquals(2, topHits.get(2).get("count"));
    }

    public void testGivenDateFieldWithoutFormat() {

        FieldStatsCalculator calculator = new FieldStatsCalculator(FileStructureUtils.DATE_MAPPING_WITHOUT_FORMAT);

        calculator.accept(Arrays.asList("2018-10-08T10:49:16.642", "2018-10-08T10:49:16.642", "2018-10-08T10:49:16.642",
            "2018-09-08T11:12:13.789", "2019-01-28T01:02:03.456", "2018-09-08T11:12:13.789"));

        FieldStats stats = calculator.calculate(3);

        assertEquals(6L, stats.getCount());
        assertEquals(3, stats.getCardinality());
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertNull(stats.getMeanValue());
        assertNull(stats.getMedianValue());
        assertEquals("2018-09-08T11:12:13.789", stats.getEarliestTimestamp());
        assertEquals("2019-01-28T01:02:03.456", stats.getLatestTimestamp());

        List<Map<String, Object>> topHits = stats.getTopHits();

        assertEquals(3, topHits.size());
        assertEquals("2018-10-08T10:49:16.642", topHits.get(0).get("value"));
        assertEquals(3, topHits.get(0).get("count"));
        assertEquals("2018-09-08T11:12:13.789", topHits.get(1).get("value"));
        assertEquals(2, topHits.get(1).get("count"));
        assertEquals("2019-01-28T01:02:03.456", topHits.get(2).get("value"));
        assertEquals(1, topHits.get(2).get("count"));
    }

    public void testGivenDateFieldWithFormat() {

        Map<String, String> dateMapping = new HashMap<>();
        dateMapping.put(FileStructureUtils.MAPPING_TYPE_SETTING, "date");
        dateMapping.put(FileStructureUtils.MAPPING_FORMAT_SETTING, "M/dd/yyyy h:mma");
        FieldStatsCalculator calculator = new FieldStatsCalculator(dateMapping);

        calculator.accept(Arrays.asList("10/08/2018 10:49AM", "10/08/2018 10:49AM", "10/08/2018 10:49AM",
            "9/08/2018 11:12AM", "1/28/2019 1:02AM", "9/08/2018 11:12AM"));

        FieldStats stats = calculator.calculate(3);

        assertEquals(6L, stats.getCount());
        assertEquals(3, stats.getCardinality());
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertNull(stats.getMeanValue());
        assertNull(stats.getMedianValue());
        assertEquals("9/08/2018 11:12AM", stats.getEarliestTimestamp());
        assertEquals("1/28/2019 1:02AM", stats.getLatestTimestamp());

        List<Map<String, Object>> topHits = stats.getTopHits();

        assertEquals(3, topHits.size());
        assertEquals("10/08/2018 10:49AM", topHits.get(0).get("value"));
        assertEquals(3, topHits.get(0).get("count"));
        assertEquals("9/08/2018 11:12AM", topHits.get(1).get("value"));
        assertEquals(2, topHits.get(1).get("count"));
        assertEquals("1/28/2019 1:02AM", topHits.get(2).get("value"));
        assertEquals(1, topHits.get(2).get("count"));
    }

    public void testJavaStatsEquivalence() {

        DoubleSummaryStatistics summaryStatistics = new DoubleSummaryStatistics();
        FieldStatsCalculator calculator = new FieldStatsCalculator(DOUBLE);

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
