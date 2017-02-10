/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;
import org.elasticsearch.xpack.persistent.TestPersistentActionPlugin.Status;
import org.elasticsearch.xpack.persistent.TestPersistentActionPlugin.TestPersistentAction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PersistentTasksInProgressTests extends AbstractWireSerializingTestCase<PersistentTasksInProgress> {

    @Override
    protected PersistentTasksInProgress createTestInstance() {
        int numberOfTasks = randomInt(10);
        Map<Long, PersistentTaskInProgress<?>> entries = new HashMap<>();
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTaskInProgress<?> taskInProgress = new PersistentTaskInProgress<>(
                    randomLong(), randomAsciiOfLength(10), new TestPersistentActionPlugin.TestRequest(randomAsciiOfLength(10)),
                    randomAsciiOfLength(10));
            if (randomBoolean()) {
                // From time to time update status
                taskInProgress = new PersistentTaskInProgress<>(taskInProgress, new Status(randomAsciiOfLength(10)));
            }
            entries.put(taskInProgress.getId(), taskInProgress);
        }
        return new PersistentTasksInProgress(randomLong(), entries);
    }

    @Override
    protected Writeable.Reader<PersistentTasksInProgress> instanceReader() {
        return PersistentTasksInProgress::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays.asList(
                new Entry(PersistentActionRequest.class, TestPersistentAction.NAME, TestPersistentActionPlugin.TestRequest::new),
                new Entry(Task.Status.class, Status.NAME, Status::new)
        ));
    }
}