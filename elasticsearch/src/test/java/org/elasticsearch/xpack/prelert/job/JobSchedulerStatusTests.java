/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job;

import org.elasticsearch.test.ESTestCase;

public class JobSchedulerStatusTests extends ESTestCase {

    public void testForString() {
        assertEquals(JobSchedulerStatus.fromString("started"), JobSchedulerStatus.STARTED);
        assertEquals(JobSchedulerStatus.fromString("stopped"), JobSchedulerStatus.STOPPED);
    }

    public void testValidOrdinals() {
        assertEquals(0, JobSchedulerStatus.STARTED.ordinal());
        assertEquals(1, JobSchedulerStatus.STOPPED.ordinal());
    }

}
