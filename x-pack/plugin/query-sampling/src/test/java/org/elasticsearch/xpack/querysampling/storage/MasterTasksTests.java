/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class MasterTasksTests extends ESTestCase {

    public void testTheJobsOnlyRunWhileThisNodeIsTheMaster() {
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        AtomicInteger runs = new AtomicInteger();
        MasterTasks master = new MasterTasks(
            queue.getThreadPool(),
            queue.getThreadPool().generic(),
            List.of(new MasterTasks.Task(runs::incrementAndGet, TimeValue.timeValueSeconds(10)))
        );
        assertFalse("nothing is scheduled before this node is the master", queue.hasDeferredTasks());

        master.onMaster();
        master.onMaster(); // a second call must not schedule the jobs again
        step(queue);
        assertThat("one run per interval", runs.get(), equalTo(1));
        step(queue);
        assertThat(runs.get(), equalTo(2));

        master.offMaster();
        while (queue.hasDeferredTasks()) { // what is left of the schedule has been cancelled, so this ends
            step(queue);
        }
        assertThat("nothing runs once it stopped being the master", runs.get(), equalTo(2));

        master.onMaster();
        step(queue);
        assertThat("it starts again when it is elected again", runs.get(), equalTo(3));
    }

    // the next interval passes
    private static void step(DeterministicTaskQueue queue) {
        queue.advanceTime();
        queue.runAllRunnableTasks();
    }
}
