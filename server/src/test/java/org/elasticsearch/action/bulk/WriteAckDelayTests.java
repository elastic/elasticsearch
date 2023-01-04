/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class WriteAckDelayTests extends ESTestCase {

    public void testWriteNoIntervalReturnsNull() {
        Settings.Builder settings = Settings.builder();
        settings.put(WriteAckDelay.WRITE_ACK_DELAY_INTERVAL.getKey(), TimeValue.ZERO);
        settings.put(WriteAckDelay.WRITE_ACK_DELAY_RANDOMNESS_BOUND.getKey(), TimeValue.timeValueMillis(20));
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        WriteAckDelay writeAckDelay = WriteAckDelay.create(settings.build(), taskQueue.getThreadPool());

        assertNull(writeAckDelay);
    }

    public void testWriteAckCompletion() {
        Settings.Builder settings = Settings.builder();
        settings.put(WriteAckDelay.WRITE_ACK_DELAY_INTERVAL.getKey(), TimeValue.timeValueMillis(50));
        settings.put(WriteAckDelay.WRITE_ACK_DELAY_RANDOMNESS_BOUND.getKey(), TimeValue.timeValueMillis(20));
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        WriteAckDelay writeAckDelay = WriteAckDelay.create(settings.build(), taskQueue.getThreadPool());

        assertNotNull(writeAckDelay);

        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), equalTo(50L));

        AtomicInteger tasks = new AtomicInteger();
        writeAckDelay.accept(tasks::incrementAndGet);

        assertThat(tasks.get(), equalTo(0));
        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runAllRunnableTasks();

        assertThat(tasks.get(), equalTo(0));

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), lessThanOrEqualTo(70L));

        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runAllRunnableTasks();

        assertThat(tasks.get(), equalTo(1));

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), equalTo(100L));

        writeAckDelay.accept(tasks::incrementAndGet);
        writeAckDelay.accept(tasks::incrementAndGet);

        taskQueue.runAllRunnableTasks();

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis(), lessThanOrEqualTo(120L));

        assertThat(tasks.get(), equalTo(1));

        taskQueue.runAllRunnableTasks();

        assertThat(tasks.get(), equalTo(3));
    }
}
