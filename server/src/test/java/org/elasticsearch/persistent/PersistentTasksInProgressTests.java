/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.persistent.PersistentTasksInProgress.PersistentTaskInProgress;
import org.elasticsearch.persistent.TestPersistentActionPlugin.Status;
import org.elasticsearch.persistent.TestPersistentActionPlugin.TestPersistentAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PersistentTasksInProgressTests extends AbstractWireSerializingTestCase<PersistentTasksInProgress> {

    @Override
    protected PersistentTasksInProgress createTestInstance() {
        int numberOfTasks = randomInt(10);
        List<PersistentTaskInProgress<?>> entries = new ArrayList<>();
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTaskInProgress<?> taskInProgress = new PersistentTaskInProgress<>(
                    randomLong(), randomAsciiOfLength(10), new TestPersistentActionPlugin.TestRequest(randomAsciiOfLength(10)),
                    randomAsciiOfLength(10));
            if (randomBoolean()) {
                // From time to time update status
                taskInProgress = new PersistentTaskInProgress<>(taskInProgress, new Status(randomAsciiOfLength(10)));
            }
            entries.add(taskInProgress);
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
