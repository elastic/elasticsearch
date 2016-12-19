/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
        assertEquals("1", new DataLoadParams(TimeRange.builder().endTime("1").build()).getEnd());
    }

    public void testIsResettingBuckets() {
        assertFalse(new DataLoadParams(TimeRange.builder().build()).isResettingBuckets());
        assertTrue(new DataLoadParams(TimeRange.builder().startTime("5").build()).isResettingBuckets());
    }
}
