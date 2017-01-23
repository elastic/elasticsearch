/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.test.ESTestCase;

public class JobStatusTests extends ESTestCase {

    public void testForString() {
        assertEquals(JobStatus.fromString("closed"), JobStatus.CLOSED);
        assertEquals(JobStatus.fromString("closing"), JobStatus.CLOSING);
        assertEquals(JobStatus.fromString("failed"), JobStatus.FAILED);
        assertEquals(JobStatus.fromString("opening"), JobStatus.OPENING);
        assertEquals(JobStatus.fromString("opened"), JobStatus.OPENED);
    }

    public void testValidOrdinals() {
        assertEquals(0, JobStatus.CLOSING.ordinal());
        assertEquals(1, JobStatus.CLOSED.ordinal());
        assertEquals(2, JobStatus.OPENING.ordinal());
        assertEquals(3, JobStatus.OPENED.ordinal());
        assertEquals(4, JobStatus.FAILED.ordinal());
    }

    public void testIsAnyOf() {
        assertFalse(JobStatus.OPENED.isAnyOf());
        assertFalse(JobStatus.OPENED.isAnyOf(JobStatus.CLOSED, JobStatus.CLOSING, JobStatus.FAILED,
                JobStatus.OPENING));
        assertFalse(JobStatus.CLOSED.isAnyOf(JobStatus.CLOSING, JobStatus.FAILED, JobStatus.OPENING, JobStatus.OPENED));

        assertTrue(JobStatus.OPENED.isAnyOf(JobStatus.OPENED));
        assertTrue(JobStatus.OPENED.isAnyOf(JobStatus.OPENED, JobStatus.CLOSED));
        assertTrue(JobStatus.CLOSING.isAnyOf(JobStatus.CLOSED, JobStatus.CLOSING));
        assertTrue(JobStatus.CLOSED.isAnyOf(JobStatus.CLOSED, JobStatus.CLOSING));
    }
}
