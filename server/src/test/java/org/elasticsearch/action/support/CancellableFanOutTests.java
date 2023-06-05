/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CancellableFanOutTests extends ESTestCase {

    public void testFanOutWithoutCancellation() {
        final var task = randomFrom(
            new Task(1, "test", "test", "", TaskId.EMPTY_TASK_ID, Map.of()),
            new CancellableTask(1, "test", "test", "", TaskId.EMPTY_TASK_ID, Map.of()),
            null
        );
        final var future = new PlainActionFuture<String>();

        final var itemListeners = new HashMap<String, ActionListener<String>>();
        final var finalFailure = randomBoolean();

        new CancellableFanOut<String, String, String>() {
            int counter;

            @Override
            protected void sendItemRequest(String item, ActionListener<String> listener) {
                itemListeners.put(item, listener);
            }

            @Override
            protected void onItemResponse(String item, String itemResponse) {
                assertThat(item, Matchers.oneOf("a", "c"));
                assertEquals(item + "-response", itemResponse);
                counter += 1;
            }

            @Override
            protected void onItemFailure(String item, Exception e) {
                assertEquals("b", item);
                counter += 1;
            }

            @Override
            protected String onCompletion() {
                assertEquals(3, counter);
                if (finalFailure) {
                    throw new ElasticsearchException("failed");
                } else {
                    return "completed";
                }
            }
        }.run(task, List.of("a", "b", "c").iterator(), future);

        itemListeners.remove("a").onResponse("a-response");
        assertFalse(future.isDone());
        itemListeners.remove("b").onFailure(new ElasticsearchException("b-response"));
        assertFalse(future.isDone());
        itemListeners.remove("c").onResponse("c-response");
        assertTrue(future.isDone());
        if (finalFailure) {
            assertEquals("failed", expectThrows(ElasticsearchException.class, future::actionGet).getMessage());
        } else {
            assertEquals("completed", future.actionGet());
        }
    }

    public void testReleaseOnCancellation() {
        final var task = new CancellableTask(1, "test", "test", "", TaskId.EMPTY_TASK_ID, Map.of());
        final var future = new PlainActionFuture<String>();

        final var itemListeners = new HashMap<String, ActionListener<String>>();
        final var handledItemResponse = new AtomicBoolean();

        final var reachabilityChecker = new ReachabilityChecker();
        reachabilityChecker.register(new CancellableFanOut<String, String, String>() {
            @Override
            protected void sendItemRequest(String item, ActionListener<String> listener) {
                itemListeners.put(item, listener);
            }

            @Override
            protected void onItemResponse(String item, String itemResponse) {
                assertEquals("a", item);
                assertEquals("a-response", itemResponse);
                assertTrue(handledItemResponse.compareAndSet(false, true));
            }

            @Override
            protected void onItemFailure(String item, Exception e) {
                fail(item);
            }

            @Override
            protected String onCompletion() {
                throw new AssertionError("onCompletion");
            }
        }).run(task, List.of("a", "b", "c").iterator(), future);

        itemListeners.remove("a").onResponse("a-response");
        assertTrue(handledItemResponse.get());
        reachabilityChecker.checkReachable();

        TaskCancelHelper.cancel(task, "test");
        reachabilityChecker.ensureUnreachable(); // even though we're still holding on to some item listeners.
        assertFalse(future.isDone());

        itemListeners.remove("b").onResponse("b-response");
        assertFalse(future.isDone());

        itemListeners.remove("c").onFailure(new ElasticsearchException("c-response"));
        assertTrue(itemListeners.isEmpty());
        assertTrue(future.isDone());
        expectThrows(TaskCancelledException.class, future::actionGet);
    }
}
