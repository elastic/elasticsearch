/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.Transport;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.mockito.Mockito.mock;

public class ActionTestUtils {

    private ActionTestUtils() { /* no construction */ }

    public static <Request extends ActionRequest, Response extends ActionResponse>
    Response executeBlocking(TransportAction<Request, Response> action, Request request) {
        PlainActionFuture<Response> future = newFuture();
        Task task = mock(Task.class);
        action.execute(task, request, future);
        return future.actionGet();
    }

    public static <Request extends ActionRequest, Response extends ActionResponse>
    Response executeBlockingWithTask(TaskManager taskManager, Transport.Connection localConnection,
                                     TransportAction<Request, Response> action, Request request) {
        PlainActionFuture<Response> future = newFuture();
        taskManager.registerAndExecute("transport", action, request, localConnection,
            (t, r) -> future.onResponse(r), (t, e) -> future.onFailure(e));
        return future.actionGet();
    }

    /**
     * Executes the given action.
     *
     * This is a shim method to make execution publicly available in tests.
     */
    public static <Request extends ActionRequest, Response extends ActionResponse>
    void execute(TransportAction<Request, Response> action, Task task, Request request, ActionListener<Response> listener) {
        action.execute(task, request, listener);
    }

    public static <T> ActionListener<T> assertNoFailureListener(CheckedConsumer<T, Exception> consumer) {
        return ActionListener.wrap(consumer, e -> {
            throw new AssertionError(e);
        });
    }
}
