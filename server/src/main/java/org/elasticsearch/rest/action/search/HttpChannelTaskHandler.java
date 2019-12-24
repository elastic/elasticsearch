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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class executes a request and associates the corresponding {@link Task} with the {@link HttpChannel} that it was originated from,
 * so that the tasks associated with a certain channel get cancelled when the underlying connection gets closed.
 */
public final class HttpChannelTaskHandler {

    public static final HttpChannelTaskHandler INSTANCE = new HttpChannelTaskHandler();
    //package private for testing
    final Map<HttpChannel, CloseListener> httpChannels = new ConcurrentHashMap<>();

    private HttpChannelTaskHandler() {
    }

    <Response extends ActionResponse> void execute(NodeClient client, HttpChannel httpChannel, ActionRequest request,
                                                   ActionType<Response> actionType, ActionListener<Response> listener) {

        CloseListener closeListener = httpChannels.computeIfAbsent(httpChannel, channel -> new CloseListener(client));
        TaskHolder taskHolder = new TaskHolder();
        Task task = client.executeLocally(actionType, request,
            new ActionListener<>() {
                @Override
                public void onResponse(Response searchResponse) {
                    try {
                        closeListener.unregisterTask(taskHolder);
                    } finally {
                        listener.onResponse(searchResponse);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        closeListener.unregisterTask(taskHolder);
                    } finally {
                        listener.onFailure(e);
                    }
                }
            });
        closeListener.registerTask(taskHolder, new TaskId(client.getLocalNodeId(), task.getId()));
        closeListener.maybeRegisterChannel(httpChannel);
    }

    public int getNumChannels() {
        return httpChannels.size();
    }

    final class CloseListener implements ActionListener<Void> {
        private final Client client;
        private final AtomicReference<HttpChannel> channel = new AtomicReference<>();
        private final Set<TaskId> taskIds = new HashSet<>();

        CloseListener(Client client) {
            this.client = client;
        }

        int getNumTasks() {
            return taskIds.size();
        }

        void maybeRegisterChannel(HttpChannel httpChannel) {
            if (channel.compareAndSet(null, httpChannel)) {
                //In case the channel is already closed when we register the listener, the listener will be immediately executed which will
                //remove the channel from the map straight-away. That is why we first create the CloseListener and later we associate it
                //with the channel. This guarantees that the close listener is already in the map when the it gets registered to its
                //corresponding channel, hence it is always found in the map when it gets invoked if the channel gets closed.
                httpChannel.addCloseListener(this);
            }
        }

        synchronized void registerTask(TaskHolder taskHolder, TaskId taskId) {
            taskHolder.taskId = taskId;
            if (taskHolder.completed == false) {
                this.taskIds.add(taskId);
            }
        }

        synchronized void unregisterTask(TaskHolder taskHolder) {
            if (taskHolder.taskId != null) {
                this.taskIds.remove(taskHolder.taskId);
            }
            taskHolder.completed = true;
        }

        @Override
        public synchronized void onResponse(Void aVoid) {
            //When the channel gets closed it won't be reused: we can remove it from the map and forget about it.
            CloseListener closeListener = httpChannels.remove(channel.get());
            assert closeListener != null : "channel not found in the map of tracked channels";
            for (TaskId taskId : taskIds) {
                ThreadContext threadContext = client.threadPool().getThreadContext();
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    // we stash any context here since this is an internal execution and should not leak any existing context information
                    threadContext.markAsSystemContext();
                    ContextPreservingActionListener<CancelTasksResponse> contextPreservingListener = new ContextPreservingActionListener<>(
                        threadContext.newRestorableContext(false),  ActionListener.wrap(r -> {}, e -> {}));
                    CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                    cancelTasksRequest.setTaskId(taskId);
                    //We don't wait for cancel tasks to come back. Task cancellation is just best effort.
                    client.admin().cluster().cancelTasks(cancelTasksRequest, contextPreservingListener);
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            onResponse(null);
        }
    }

    private static class TaskHolder {
        private TaskId taskId;
        private boolean completed = false;
    }
}
