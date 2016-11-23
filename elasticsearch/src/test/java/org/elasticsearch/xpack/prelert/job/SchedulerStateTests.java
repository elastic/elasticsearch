/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.support.AbstractSerializingTestCase;

public class SchedulerStateTests extends AbstractSerializingTestCase<SchedulerState> {

    @Override
    protected SchedulerState createTestInstance() {
        return new SchedulerState(randomFrom(JobSchedulerStatus.values()), randomPositiveLong(),
                randomBoolean() ? null : randomPositiveLong());
    }

    @Override
    protected Writeable.Reader<SchedulerState> instanceReader() {
        return SchedulerState::new;
    }

    @Override
    protected SchedulerState parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return SchedulerState.PARSER.apply(parser, () -> matcher);
    }

    public void testEquals_GivenDifferentClass() {

        assertFalse(new SchedulerState(JobSchedulerStatus.STARTED, 0L, null).equals("a string"));
    }

    public void testEquals_GivenSameReference() {
        SchedulerState schedulerState = new SchedulerState(JobSchedulerStatus.STARTED, 18L, 42L);
        assertTrue(schedulerState.equals(schedulerState));
    }

    public void testEquals_GivenEqualObjects() {
        SchedulerState schedulerState1 = new SchedulerState(JobSchedulerStatus.STARTED, 18L, 42L);
        SchedulerState schedulerState2 = new SchedulerState(schedulerState1.getStatus(), schedulerState1.getStartTimeMillis(),
                schedulerState1.getEndTimeMillis());

        assertTrue(schedulerState1.equals(schedulerState2));
        assertTrue(schedulerState2.equals(schedulerState1));
        assertEquals(schedulerState1.hashCode(), schedulerState2.hashCode());
    }

    public void testEquals_GivenDifferentStatus() {
        SchedulerState schedulerState1 = new SchedulerState(JobSchedulerStatus.STARTED, 18L, 42L);
        SchedulerState schedulerState2 = new SchedulerState(JobSchedulerStatus.STOPPED, schedulerState1.getStartTimeMillis(),
                schedulerState1.getEndTimeMillis());

        assertFalse(schedulerState1.equals(schedulerState2));
        assertFalse(schedulerState2.equals(schedulerState1));
    }

    public void testEquals_GivenDifferentStartTimeMillis() {
        SchedulerState schedulerState1 = new SchedulerState(JobSchedulerStatus.STARTED, null, 42L);
        SchedulerState schedulerState2 = new SchedulerState(JobSchedulerStatus.STOPPED, 19L, schedulerState1.getEndTimeMillis());

        assertFalse(schedulerState1.equals(schedulerState2));
        assertFalse(schedulerState2.equals(schedulerState1));
    }

    public void testEquals_GivenDifferentEndTimeMillis() {
        SchedulerState schedulerState1 = new SchedulerState(JobSchedulerStatus.STARTED, 18L, 42L);
        SchedulerState schedulerState2 = new SchedulerState(JobSchedulerStatus.STOPPED, schedulerState1.getStartTimeMillis(), 43L);

        assertFalse(schedulerState1.equals(schedulerState2));
        assertFalse(schedulerState2.equals(schedulerState1));
    }
}
