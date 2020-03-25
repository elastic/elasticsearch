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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

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
    Response executeBlockingWithTask(TaskManager taskManager, TransportAction<Request, Response> action, Request request) {
        PlainActionFuture<Response> future = newFuture();
        taskManager.registerAndExecute("transport", action, request, (t, r) -> future.onResponse(r), (t, e) -> future.onFailure(e));
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
