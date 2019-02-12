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
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ListenerTimeoutsTests extends ESTestCase {

    private final TimeValue timeout = TimeValue.timeValueMillis(10);
    private final String generic = ThreadPool.Names.GENERIC;
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

        ActionListener<Void> wrapped = ListenerTimeouts.wrapWithTimeout(taskQueue.getThreadPool(), listener, timeout, generic, "test");
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        wrapped.onResponse(null);
        wrapped.onFailure(new IOException("incorrect exception"));

        assertFalse(completed.get());
        assertThat(exception.get(), instanceOf(ElasticsearchTimeoutException.class));
    }

    public void testFinishNormallyBeforeTimeout() {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = wrap(completed, exception);

        ActionListener<Void> wrapped = ListenerTimeouts.wrapWithTimeout(taskQueue.getThreadPool(), listener, timeout, generic, "test");
        wrapped.onResponse(null);
        wrapped.onFailure(new IOException("boom"));
        wrapped.onResponse(null);

        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertTrue(completed.get());
        assertNull(exception.get());
    }

    public void testFinishExceptionallyBeforeTimeout() {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = wrap(completed, exception);

        ActionListener<Void> wrapped = ListenerTimeouts.wrapWithTimeout(taskQueue.getThreadPool(), listener, timeout, generic, "test");
        wrapped.onFailure(new IOException("boom"));

        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertFalse(completed.get());
        assertThat(exception.get(), instanceOf(IOException.class));
    }

    private ActionListener<Void> wrap(AtomicBoolean completed, AtomicReference<Exception> exception) {
        return new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                assert completed.get() == false : "Should not be called twice";
                completed.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                assert exception.get() == null : "Should not be called twice";
                exception.set(e);
            }
        };
    }
}
