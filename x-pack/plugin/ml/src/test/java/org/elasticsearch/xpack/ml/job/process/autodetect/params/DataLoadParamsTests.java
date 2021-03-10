/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

public class DataLoadParamsTests extends ESTestCase {
    public void testGetStart() {
        assertEquals("", new DataLoadParams(TimeRange.builder().build(), Optional.empty()).getStart());
        assertEquals("3", new DataLoadParams(TimeRange.builder().startTime("3").build(), Optional.empty()).getStart());
    }

    public void testGetEnd() {
        assertEquals("", new DataLoadParams(TimeRange.builder().build(), Optional.empty()).getEnd());
        assertEquals("1", new DataLoadParams(TimeRange.builder().endTime("1").build(), Optional.empty()).getEnd());
    }

    public void testIsResettingBuckets() {
        assertFalse(new DataLoadParams(TimeRange.builder().build(), Optional.empty()).isResettingBuckets());
        assertTrue(new DataLoadParams(TimeRange.builder().startTime("5").build(), Optional.empty()).isResettingBuckets());
    }
}
