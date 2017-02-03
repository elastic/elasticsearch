/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.test.ESTestCase;

public class JobStateTests extends ESTestCase {

    public void testFromString() {
        assertEquals(JobState.fromString("closed"), JobState.CLOSED);
        assertEquals(JobState.fromString("closing"), JobState.CLOSING);
        assertEquals(JobState.fromString("failed"), JobState.FAILED);
        assertEquals(JobState.fromString("opening"), JobState.OPENING);
        assertEquals(JobState.fromString("opened"), JobState.OPENED);
    }

    public void testValidOrdinals() {
        assertEquals(0, JobState.CLOSING.ordinal());
        assertEquals(1, JobState.CLOSED.ordinal());
        assertEquals(2, JobState.OPENING.ordinal());
        assertEquals(3, JobState.OPENED.ordinal());
        assertEquals(4, JobState.FAILED.ordinal());
    }

    public void testIsAnyOf() {
        assertFalse(JobState.OPENED.isAnyOf());
        assertFalse(JobState.OPENED.isAnyOf(JobState.CLOSED, JobState.CLOSING, JobState.FAILED,
                JobState.OPENING));
        assertFalse(JobState.CLOSED.isAnyOf(JobState.CLOSING, JobState.FAILED, JobState.OPENING, JobState.OPENED));

        assertTrue(JobState.OPENED.isAnyOf(JobState.OPENED));
        assertTrue(JobState.OPENED.isAnyOf(JobState.OPENED, JobState.CLOSED));
        assertTrue(JobState.CLOSING.isAnyOf(JobState.CLOSED, JobState.CLOSING));
        assertTrue(JobState.CLOSED.isAnyOf(JobState.CLOSED, JobState.CLOSING));
    }
}
