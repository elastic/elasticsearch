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
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class keeps track of which tasks came in from which {@link HttpChannel}, by allowing to associate
 * an {@link HttpChannel} with a {@link TaskId}, and also removing the link once the task is complete.
 * Additionally, it accepts a consumer that gets called whenever an http channel gets closed, which
 * can be used to cancel the associated task when the underlying connection gets closed.
 */
final class HttpChannelTaskHandler {
    final Map<HttpChannel, CloseListener> httpChannels = new ConcurrentHashMap<>();

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

        closeListener.registerTask(httpChannel, taskHolder, new TaskId(client.getLocalNodeId(), task.getId()));

        //TODO check that no tasks are left behind through assertions at node close. Not sure how. Couldn't there be in-flight requests
        //causing channels to be in the map when a node gets closed?
    }

    final class CloseListener implements ActionListener<Void> {
        private final Client client;
        final Set<TaskId> taskIds = new HashSet<>();
        private HttpChannel channel;

        CloseListener(Client client) {
            this.client = client;
        }

        synchronized void registerTask(HttpChannel httpChannel, TaskHolder taskHolder, TaskId taskId) {
            if (channel == null) {
                channel = httpChannel;
                //In case the channel is already closed when we register the listener, the listener will be immediately executed which will
                //remove the channel from the map straight-away. That is why we first create the CloseListener and later we associate it
                //with the channel. This guarantees that the close listener is already in the map when the it gets registered to its
                //corresponding channel, hence it is always found in the map when it gets invoked if the channel gets closed.
                httpChannel.addCloseListener(this);
            }
            taskHolder.taskId = taskId;
            if (taskHolder.completed == false) {
                this.taskIds.add(taskId);
            }
        }

        private synchronized void unregisterTask(TaskHolder taskHolder) {
            if (taskHolder.taskId != null) {
                this.taskIds.remove(taskHolder.taskId);
            }
            taskHolder.completed = true;
        }

        @Override
        public synchronized void onResponse(Void aVoid) {
            //When the channel gets closed it won't be reused: we can remove it from the map as there is no chance we will
            //register another close listener to it later.
            //The channel reference may be null, if the connection gets closed before it got set.
            //TODO Could it be that not all tasks have been registered yet when the close listener is notified. We remove the channel
            // and cancel the tasks that are known up until then. if new tasks come in from the same channel, the channel will be added
            // again to the map, but the close listener will be registered another time to it which is not good.
            if (channel != null) {
                httpChannels.remove(channel);
                for (TaskId previousTaskId : taskIds) {
                    CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                    cancelTasksRequest.setTaskId(previousTaskId);
                    //We don't wait for cancel tasks to come back. Task cancellation is just best effort.
                    //Note that cancel tasks fails if the user sending the search request does not have the permissions to call it.
                    client.admin().cluster().cancelTasks(cancelTasksRequest, ActionListener.wrap(r -> {}, e -> {}));
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
