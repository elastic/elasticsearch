/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job;

import org.elasticsearch.test.ESTestCase;

public class JobStatusTests extends ESTestCase {

    public void testForString() {
        assertEquals(JobStatus.fromString("closed"), JobStatus.CLOSED);
        assertEquals(JobStatus.fromString("closing"), JobStatus.CLOSING);
        assertEquals(JobStatus.fromString("failed"), JobStatus.FAILED);
        assertEquals(JobStatus.fromString("paused"), JobStatus.PAUSED);
        assertEquals(JobStatus.fromString("pausing"), JobStatus.PAUSING);
        assertEquals(JobStatus.fromString("running"), JobStatus.RUNNING);
    }

    public void testValidOrdinals() {
        assertEquals(0, JobStatus.RUNNING.ordinal());
        assertEquals(1, JobStatus.CLOSING.ordinal());
        assertEquals(2, JobStatus.CLOSED.ordinal());
        assertEquals(3, JobStatus.FAILED.ordinal());
        assertEquals(4, JobStatus.PAUSING.ordinal());
        assertEquals(5, JobStatus.PAUSED.ordinal());
    }

    public void testIsAnyOf() {
        assertFalse(JobStatus.RUNNING.isAnyOf());
        assertFalse(JobStatus.RUNNING.isAnyOf(JobStatus.CLOSED, JobStatus.CLOSING, JobStatus.FAILED,
                JobStatus.PAUSED, JobStatus.PAUSING));
        assertFalse(JobStatus.CLOSED.isAnyOf(JobStatus.RUNNING, JobStatus.CLOSING, JobStatus.FAILED,
                JobStatus.PAUSED, JobStatus.PAUSING));

        assertTrue(JobStatus.RUNNING.isAnyOf(JobStatus.RUNNING));
        assertTrue(JobStatus.RUNNING.isAnyOf(JobStatus.RUNNING, JobStatus.CLOSED));
        assertTrue(JobStatus.PAUSED.isAnyOf(JobStatus.PAUSED, JobStatus.PAUSING));
        assertTrue(JobStatus.PAUSING.isAnyOf(JobStatus.PAUSED, JobStatus.PAUSING));
    }
}
