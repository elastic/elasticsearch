/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler.Listener;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformScheduledTaskTests extends ESTestCase {

    private static final String TRANSFORM_ID = "transform-id";
    private static final TimeValue FREQUENCY = TimeValue.timeValueSeconds(10);
    private static final TimeValue DEFAULT_FREQUENCY = TimeValue.timeValueSeconds(60);
    private static final long LAST_TRIGGERED_TIME_MILLIS = 100000L;
    private static final Listener LISTENER = event -> {};

    public void testBasics() {
        TransformScheduledTask task = new TransformScheduledTask(TRANSFORM_ID, FREQUENCY, LAST_TRIGGERED_TIME_MILLIS, 0, 123, LISTENER);
        assertThat(task.getTransformId(), is(equalTo(TRANSFORM_ID)));
        assertThat(task.getFrequency(), is(equalTo(FREQUENCY)));
        assertThat(task.getLastTriggeredTimeMillis(), is(equalTo(LAST_TRIGGERED_TIME_MILLIS)));
        assertThat(task.getFailureCount(), is(equalTo(0)));
        assertThat(task.getNextScheduledTimeMillis(), is(equalTo(123L)));
        assertThat(task.getListener(), is(equalTo(LISTENER)));
    }

    public void testNextScheduledTimeMillis() {
        {
            TransformScheduledTask task = new TransformScheduledTask(TRANSFORM_ID, FREQUENCY, LAST_TRIGGERED_TIME_MILLIS, 0, 123, LISTENER);
            // Verify that the explicitly-provided next scheduled time is returned when failure count is 0
            assertThat(task.getNextScheduledTimeMillis(), is(equalTo(123L)));
        }
        {
            TransformScheduledTask task = new TransformScheduledTask(TRANSFORM_ID, FREQUENCY, LAST_TRIGGERED_TIME_MILLIS, 1, 123, LISTENER);
            // Verify that the explicitly-provided next scheduled time is returned when failure count is greater than 0
            assertThat(task.getNextScheduledTimeMillis(), is(equalTo(123L)));
        }
        {
            TransformScheduledTask task = new TransformScheduledTask(TRANSFORM_ID, FREQUENCY, LAST_TRIGGERED_TIME_MILLIS, 0, LISTENER);
            // Verify that the next scheduled time is calculated properly when failure count is 0
            assertThat(task.getNextScheduledTimeMillis(), is(equalTo(110000L)));
        }
        {
            TransformScheduledTask task = new TransformScheduledTask(TRANSFORM_ID, FREQUENCY, LAST_TRIGGERED_TIME_MILLIS, 1, LISTENER);
            // Verify that the next scheduled time is calculated properly when failure count is greater than 0
            assertThat(task.getNextScheduledTimeMillis(), is(equalTo(105000L)));
        }
    }
}
