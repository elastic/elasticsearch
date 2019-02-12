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

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ListenerTimeoutsTests extends ESTestCase {

    private final TimeValue timeout = TimeValue.timeValueMillis(10);
    private DeterministicTaskQueue taskQueue;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        taskQueue = new DeterministicTaskQueue(settings, random());
    }

    public void testListenerTimeout() {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = wrap(completed, exception);

        ActionListener<Void> wrapped = ListenerTimeouts.wrapWithTimeout(taskQueue.getThreadPool(), listener, timeout, "test");
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        wrapped.onResponse(null);

        assertFalse(completed.get());
        assertThat(exception.get(), instanceOf(ElasticsearchTimeoutException.class));
    }

    public void testFinishBeforeTimeout() {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = wrap(completed, exception);

        ActionListener<Void> wrapped = ListenerTimeouts.wrapWithTimeout(taskQueue.getThreadPool(), listener, timeout, "test");
        wrapped.onResponse(null);

        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertTrue(completed.get());
        assertNull(exception.get());
    }

    private ActionListener<Void> wrap(AtomicBoolean completed, AtomicReference<Exception> exception) {
        return new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                completed.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                exception.set(e);
            }
        };
    }
}
