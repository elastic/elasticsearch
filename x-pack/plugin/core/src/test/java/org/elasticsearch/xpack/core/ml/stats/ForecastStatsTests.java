/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats.Fields;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ForecastStatsTests extends AbstractWireSerializingTestCase<ForecastStats> {

    public void testEmpty() throws IOException {
        ForecastStats forecastStats = new ForecastStats();

        XContentBuilder builder = JsonXContent.contentBuilder();
        forecastStats.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        Map<String, Object> properties = parser.map();
        assertTrue(properties.containsKey(Fields.TOTAL));
        assertTrue(properties.containsKey(Fields.FORECASTED_JOBS));
        assertFalse(properties.containsKey(Fields.MEMORY));
        assertFalse(properties.containsKey(Fields.RECORDS));
        assertFalse(properties.containsKey(Fields.RUNTIME));
        assertFalse(properties.containsKey(Fields.STATUSES));
    }

    public void testMerge() {
        StatsAccumulator memoryStats = new StatsAccumulator();
        memoryStats.add(1000);
        memoryStats.add(45000);
        memoryStats.add(2300);

        StatsAccumulator recordStats = new StatsAccumulator();
        recordStats.add(10);
        recordStats.add(0);
        recordStats.add(20);

        StatsAccumulator runtimeStats = new StatsAccumulator();
        runtimeStats.add(0);
        runtimeStats.add(0);
        runtimeStats.add(10);

        CountAccumulator statusStats = new CountAccumulator();
        statusStats.add("finished", 2L);
        statusStats.add("failed", 5L);

        ForecastStats forecastStats = new ForecastStats(3, memoryStats, recordStats, runtimeStats, statusStats);

        StatsAccumulator memoryStats2 = new StatsAccumulator();
        memoryStats2.add(10);
        memoryStats2.add(30);

        StatsAccumulator recordStats2 = new StatsAccumulator();
        recordStats2.add(10);
        recordStats2.add(0);

        StatsAccumulator runtimeStats2 = new StatsAccumulator();
        runtimeStats2.add(96);
        runtimeStats2.add(0);

        CountAccumulator statusStats2 = new CountAccumulator();
        statusStats2.add("finished", 2L);
        statusStats2.add("scheduled", 1L);

        ForecastStats forecastStats2 = new ForecastStats(2, memoryStats2, recordStats2, runtimeStats2, statusStats2);

        forecastStats.merge(forecastStats2);

        Map<String, Object> mergedStats = forecastStats.asMap();

        assertEquals(2L, mergedStats.get(Fields.FORECASTED_JOBS));
        assertEquals(5L, mergedStats.get(Fields.TOTAL));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedMemoryStats = (Map<String, Double>) mergedStats.get(Fields.MEMORY);

        assertTrue(mergedMemoryStats != null);
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.AVG), equalTo(9668.0));
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.MAX), equalTo(45000.0));
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.MIN), equalTo(10.0));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedRecordStats = (Map<String, Double>) mergedStats.get(Fields.RECORDS);

        assertTrue(mergedRecordStats != null);
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.AVG), equalTo(8.0));
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.MAX), equalTo(20.0));
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.MIN), equalTo(0.0));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedRuntimeStats = (Map<String, Double>) mergedStats.get(Fields.RUNTIME);

        assertTrue(mergedRuntimeStats != null);
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.AVG), equalTo(21.2));
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.MAX), equalTo(96.0));
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.MIN), equalTo(0.0));

        @SuppressWarnings("unchecked")
        Map<String, Long> mergedCountStats = (Map<String, Long>) mergedStats.get(Fields.STATUSES);

        assertTrue(mergedCountStats != null);
        assertEquals(3, mergedCountStats.size());
        assertEquals(4, mergedCountStats.get("finished").longValue());
        assertEquals(5, mergedCountStats.get("failed").longValue());
        assertEquals(1, mergedCountStats.get("scheduled").longValue());
    }

    public void testChainedMerge() {
        StatsAccumulator memoryStats = new StatsAccumulator();
        memoryStats.add(1000);
        memoryStats.add(45000);
        memoryStats.add(2300);
        StatsAccumulator recordStats = new StatsAccumulator();
        recordStats.add(10);
        recordStats.add(0);
        recordStats.add(20);
        StatsAccumulator runtimeStats = new StatsAccumulator();
        runtimeStats.add(0);
        runtimeStats.add(0);
        runtimeStats.add(10);
        CountAccumulator statusStats = new CountAccumulator();
        statusStats.add("finished", 2L);
        statusStats.add("failed", 5L);
        ForecastStats forecastStats = new ForecastStats(3, memoryStats, recordStats, runtimeStats, statusStats);

        StatsAccumulator memoryStats2 = new StatsAccumulator();
        memoryStats2.add(10);
        memoryStats2.add(30);
        StatsAccumulator recordStats2 = new StatsAccumulator();
        recordStats2.add(10);
        recordStats2.add(0);
        StatsAccumulator runtimeStats2 = new StatsAccumulator();
        runtimeStats2.add(96);
        runtimeStats2.add(0);
        CountAccumulator statusStats2 = new CountAccumulator();
        statusStats2.add("finished", 2L);
        statusStats2.add("scheduled", 1L);
        ForecastStats forecastStats2 = new ForecastStats(2, memoryStats2, recordStats2, runtimeStats2, statusStats2);

        StatsAccumulator memoryStats3 = new StatsAccumulator();
        memoryStats3.add(500);
        StatsAccumulator recordStats3 = new StatsAccumulator();
        recordStats3.add(50);
        StatsAccumulator runtimeStats3 = new StatsAccumulator();
        runtimeStats3.add(32);
        CountAccumulator statusStats3 = new CountAccumulator();
        statusStats3.add("finished", 1L);
        ForecastStats forecastStats3 = new ForecastStats(1, memoryStats3, recordStats3, runtimeStats3, statusStats3);

        ForecastStats forecastStats4 = new ForecastStats();

        // merge 4 into 3
        forecastStats3.merge(forecastStats4);

        // merge 3 into 2
        forecastStats2.merge(forecastStats3);

        // merger 2 into 1
        forecastStats.merge(forecastStats2);

        Map<String, Object> mergedStats = forecastStats.asMap();

        assertEquals(3L, mergedStats.get(Fields.FORECASTED_JOBS));
        assertEquals(6L, mergedStats.get(Fields.TOTAL));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedMemoryStats = (Map<String, Double>) mergedStats.get(Fields.MEMORY);

        assertTrue(mergedMemoryStats != null);
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.AVG), equalTo(8140.0));
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.MAX), equalTo(45000.0));
        assertThat(mergedMemoryStats.get(StatsAccumulator.Fields.MIN), equalTo(10.0));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedRecordStats = (Map<String, Double>) mergedStats.get(Fields.RECORDS);

        assertTrue(mergedRecordStats != null);
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.AVG), equalTo(15.0));
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.MAX), equalTo(50.0));
        assertThat(mergedRecordStats.get(StatsAccumulator.Fields.MIN), equalTo(0.0));

        @SuppressWarnings("unchecked")
        Map<String, Double> mergedRuntimeStats = (Map<String, Double>) mergedStats.get(Fields.RUNTIME);

        assertTrue(mergedRuntimeStats != null);
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.AVG), equalTo(23.0));
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.MAX), equalTo(96.0));
        assertThat(mergedRuntimeStats.get(StatsAccumulator.Fields.MIN), equalTo(0.0));

        @SuppressWarnings("unchecked")
        Map<String, Long> mergedCountStats = (Map<String, Long>) mergedStats.get(Fields.STATUSES);

        assertTrue(mergedCountStats != null);
        assertEquals(3, mergedCountStats.size());
        assertEquals(5, mergedCountStats.get("finished").longValue());
        assertEquals(5, mergedCountStats.get("failed").longValue());
        assertEquals(1, mergedCountStats.get("scheduled").longValue());
    }

    public void testUniqueCountOfJobs() {
        ForecastStats forecastStats = createForecastStats(5, 10);
        ForecastStats forecastStats2 = createForecastStats(2, 8);
        ForecastStats forecastStats3 = createForecastStats(0, 0);
        ForecastStats forecastStats4 = createForecastStats(0, 0);
        ForecastStats forecastStats5 = createForecastStats(1, 12);

        forecastStats.merge(forecastStats2);
        forecastStats.merge(forecastStats3);
        forecastStats.merge(forecastStats4);
        forecastStats.merge(forecastStats5);

        assertEquals(3L, forecastStats.asMap().get(Fields.FORECASTED_JOBS));
    }

    @Override
    public ForecastStats createTestInstance() {
        return createForecastStats(1, 22);
    }

    @Override
    protected Reader<ForecastStats> instanceReader() {
        return ForecastStats::new;
    }

    public ForecastStats createForecastStats(long minTotal, long maxTotal) {
        ForecastStats forecastStats = new ForecastStats(randomLongBetween(minTotal, maxTotal), createStatsAccumulator(),
                createStatsAccumulator(), createStatsAccumulator(), createCountAccumulator());

        return forecastStats;
    }

    private StatsAccumulator createStatsAccumulator() {
        return new StatsAccumulatorTests().createTestInstance();
    }

    private CountAccumulator createCountAccumulator() {
        return new CountAccumulatorTests().createTestInstance();

    }
}
