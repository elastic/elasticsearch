/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

public class TimeRangeTests extends ESTestCase {
    public void testGetStart() {
        assertEquals("", TimeRange.builder().build().getStart());
        assertEquals("10", TimeRange.builder().startTime("10").build().getStart());
        assertEquals("1462096800", TimeRange.builder().startTime("2016-05-01T10:00:00Z").build().getStart());
    }

    public void testGetEnd() {
        assertEquals("", TimeRange.builder().build().getEnd());
        assertEquals("20", TimeRange.builder().endTime("20").build().getEnd());
        assertEquals("1462096800", TimeRange.builder().endTime("2016-05-01T10:00:00Z").build().getEnd());
    }

    public void test_UnparseableStartThrows() {
        ElasticsearchParseException e = ESTestCase.expectThrows(
            ElasticsearchParseException.class,
            () -> TimeRange.builder().startTime("bad").build()
        );
        assertEquals(Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, TimeRange.START_PARAM, "bad"), e.getMessage());
    }

    public void test_UnparseableEndThrows() {
        ElasticsearchParseException e = ESTestCase.expectThrows(
            ElasticsearchParseException.class,
            () -> TimeRange.builder().endTime("bad").build()
        );
        assertEquals(Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, TimeRange.END_PARAM, "bad"), e.getMessage());
    }

    public void test_EndComesBeforeStartThrows() {
        IllegalArgumentException e = ESTestCase.expectThrows(
            IllegalArgumentException.class,
            () -> TimeRange.builder().startTime("2016-10-01T10:00:00Z").endTime("2016-09-30T10:00:00Z").build()
        );

        assertEquals(Messages.getMessage(Messages.REST_START_AFTER_END, "2016-09-30T10:00:00Z", "2016-10-01T10:00:00Z"), e.getMessage());
    }

    public void test_EndIsStartPlus1SecondIfNotSet() {

        TimeRange range = TimeRange.builder().startTime("2016-05-01T10:00:00Z").build();
        assertEquals("1462096800", range.getStart());
        assertEquals("1462096801", range.getEnd());
    }

    public void test_EndIsStartPlus1SecondIfEqualToStart() {

        TimeRange range = TimeRange.builder().startTime("2016-05-01T10:00:00Z").endTime("2016-05-01T10:00:00Z").build();
        assertEquals("1462096800", range.getStart());
        assertEquals("1462096801", range.getEnd());
    }
}
