/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class FlushJobParamsTests extends ESTestCase {

    public void testBuilder_GivenDefault() {
        FlushJobParams params = FlushJobParams.builder().build();
        assertFalse(params.shouldCalculateInterim());
        assertFalse(params.shouldAdvanceTime());
        assertFalse(params.shouldSkipTime());
        assertEquals("", params.getStart());
        assertEquals("", params.getEnd());
    }

    public void testBuilder_GivenCalcInterim() {
        FlushJobParams params = FlushJobParams.builder().calcInterim(true).build();
        assertTrue(params.shouldCalculateInterim());
        assertFalse(params.shouldAdvanceTime());
        assertFalse(params.shouldSkipTime());
        assertEquals("", params.getStart());
        assertEquals("", params.getEnd());
    }

    public void testBuilder_GivenCalcInterimAndStart() {
        FlushJobParams params = FlushJobParams.builder()
            .calcInterim(true)
            .forTimeRange(TimeRange.builder().startTime("42").build())
            .build();
        assertTrue(params.shouldCalculateInterim());
        assertFalse(params.shouldAdvanceTime());
        assertFalse(params.shouldSkipTime());
        assertEquals("42", params.getStart());
        assertEquals("43", params.getEnd());
    }

    public void testBuilder_GivenCalcInterimAndEnd_throws() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FlushJobParams.builder().calcInterim(true).forTimeRange(TimeRange.builder().endTime("100").build()).build()
        );

        assertEquals("Invalid flush parameters: 'start' has not been specified.", e.getMessage());
    }

    public void testBuilder_GivenCalcInterimAndStartAndEnd() {
        FlushJobParams params = FlushJobParams.builder()
            .calcInterim(true)
            .forTimeRange(TimeRange.builder().startTime("3600").endTime("7200").build())
            .build();
        assertTrue(params.shouldCalculateInterim());
        assertFalse(params.shouldAdvanceTime());
        assertEquals("3600", params.getStart());
        assertEquals("7200", params.getEnd());
    }

    public void testBuilder_GivenAdvanceTime() {
        FlushJobParams params = FlushJobParams.builder().advanceTime("1821").build();
        assertFalse(params.shouldCalculateInterim());
        assertEquals("", params.getStart());
        assertEquals("", params.getEnd());
        assertTrue(params.shouldAdvanceTime());
        assertEquals(1821, params.getAdvanceTime());
    }

    public void testBuilder_GivenCalcInterimAndAdvanceTime() {
        FlushJobParams params = FlushJobParams.builder().calcInterim(true).advanceTime("1940").build();
        assertTrue(params.shouldCalculateInterim());
        assertEquals("", params.getStart());
        assertEquals("", params.getEnd());
        assertTrue(params.shouldAdvanceTime());
        assertEquals(1940, params.getAdvanceTime());
    }

    public void testBuilder_GivenCalcInterimWithTimeRangeAndAdvanceTime() {
        FlushJobParams params = FlushJobParams.builder()
            .calcInterim(true)
            .forTimeRange(TimeRange.builder().startTime("1").endTime("2").build())
            .advanceTime("1940")
            .build();
        assertTrue(params.shouldCalculateInterim());
        assertEquals("1", params.getStart());
        assertEquals("2", params.getEnd());
        assertTrue(params.shouldAdvanceTime());
        assertEquals(1940, params.getAdvanceTime());
    }

    public void testBuilder_GivenAdvanceTimeIsEarlierThanSkipTime() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> FlushJobParams.builder().advanceTime("2017-01-01T00:00:00Z").skipTime("2017-02-01T00:00:00Z").build()
        );

        assertEquals("advance_time [2017-01-01T00:00:00Z] must be later than skip_time [2017-02-01T00:00:00Z]", e.getMessage());
    }

    public void testBuilder_GivenAdvanceTimeIsEqualToSkipTime() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> FlushJobParams.builder().advanceTime("2017-01-01T00:00:00Z").skipTime("2017-01-01T00:00:00Z").build()
        );

        assertEquals("advance_time [2017-01-01T00:00:00Z] must be later than skip_time [2017-01-01T00:00:00Z]", e.getMessage());
    }

    public void testBuilder_GivenAdvanceTimeIsLaterToSkipTime() {
        FlushJobParams params = FlushJobParams.builder().advanceTime("2017-02-01T00:00:00Z").skipTime("2017-01-01T00:00:00Z").build();

        assertThat(params.getSkipTime(), equalTo(1483228800L));
        assertThat(params.getAdvanceTime(), equalTo(1485907200L));
    }

    public void testValidate_GivenOnlyStartSpecified() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FlushJobParams.builder().forTimeRange(TimeRange.builder().startTime("1").build()).build()
        );

        assertEquals("Invalid flush parameters: unexpected 'start'.", e.getMessage());
    }

    public void testFlushUpload_GivenOnlyEndSpecified() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FlushJobParams.builder().forTimeRange(TimeRange.builder().endTime("1").build()).build()
        );

        assertEquals("Invalid flush parameters: unexpected 'end'.", e.getMessage());
    }

    public void testFlushUpload_GivenInterimResultsAndOnlyEndSpecified() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FlushJobParams.builder().calcInterim(true).forTimeRange(TimeRange.builder().endTime("1").build()).build()
        );

        assertEquals("Invalid flush parameters: 'start' has not been specified.", e.getMessage());
    }

    public void testFlushUpload_GivenInterimResultsAndStartAndEndSpecifiedAsEpochs() {
        FlushJobParams params = FlushJobParams.builder()
            .calcInterim(true)
            .forTimeRange(TimeRange.builder().startTime("1428494400").endTime("1428498000").build())
            .build();
        assertTrue(params.shouldCalculateInterim());
        assertFalse(params.shouldAdvanceTime());
        assertEquals("1428494400", params.getStart());
        assertEquals("1428498000", params.getEnd());
    }

    public void testFlushUpload_GivenInterimResultsAndSameStartAndEnd() {
        FlushJobParams params = FlushJobParams.builder()
            .calcInterim(true)
            .forTimeRange(TimeRange.builder().startTime("1428494400").endTime("1428494400").build())
            .build();

        assertTrue(params.shouldCalculateInterim());
        assertFalse(params.shouldAdvanceTime());
        assertEquals("1428494400", params.getStart());
        assertEquals("1428494401", params.getEnd());
    }

    public void testFlushUpload_GivenInterimResultsAndOnlyStartSpecified() {
        FlushJobParams params = FlushJobParams.builder()
            .calcInterim(true)
            .forTimeRange(TimeRange.builder().startTime("1428494400").build())
            .build();

        assertTrue(params.shouldCalculateInterim());
        assertFalse(params.shouldAdvanceTime());
        assertEquals("1428494400", params.getStart());
        assertEquals("1428494401", params.getEnd());
    }

    public void testFlushUpload_GivenValidAdvanceTime() {
        FlushJobParams params = FlushJobParams.builder().advanceTime("2015-04-08T13:00:00.000Z").build();
        assertFalse(params.shouldCalculateInterim());
        assertEquals("", params.getStart());
        assertEquals("", params.getEnd());
        assertTrue(params.shouldAdvanceTime());
        assertEquals(1428498000L, params.getAdvanceTime());
    }

    public void testFlushUpload_GivenCalcInterimAndAdvanceTime() {
        FlushJobParams params = FlushJobParams.builder().calcInterim(true).advanceTime("3600").build();
        assertTrue(params.shouldCalculateInterim());
        assertEquals("", params.getStart());
        assertEquals("", params.getEnd());
        assertTrue(params.shouldAdvanceTime());
        assertEquals(3600L, params.getAdvanceTime());
    }

    public void testFlushUpload_GivenCalcInterimWithTimeRangeAndAdvanceTime() {
        FlushJobParams params = FlushJobParams.builder()
            .calcInterim(true)
            .forTimeRange(TimeRange.builder().startTime("150").endTime("300").build())
            .advanceTime("200")
            .build();
        assertTrue(params.shouldCalculateInterim());
        assertEquals("150", params.getStart());
        assertEquals("300", params.getEnd());
        assertTrue(params.shouldAdvanceTime());
        assertEquals(200L, params.getAdvanceTime());
    }
}
