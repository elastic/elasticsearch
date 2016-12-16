/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.scheduler;

import org.elasticsearch.test.ESTestCase;

public class SchedulerStatusTests extends ESTestCase {

    public void testForString() {
        assertEquals(SchedulerStatus.fromString("started"), SchedulerStatus.STARTED);
        assertEquals(SchedulerStatus.fromString("stopped"), SchedulerStatus.STOPPED);
    }

    public void testValidOrdinals() {
        assertEquals(0, SchedulerStatus.STARTED.ordinal());
        assertEquals(1, SchedulerStatus.STOPPED.ordinal());
    }

}
