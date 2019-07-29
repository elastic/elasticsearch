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
import org.elasticsearch.tasks.TaskListener;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        //0: initial state, 1: either linked or already unlinked without being linked first, 2: first linked and then unlinked
        //link can only be done if it's the first thing that happens. unlink will only happen if link was done first.
        AtomicInteger link = new AtomicInteger(0);
        Task task = client.executeLocally(actionType, request,
            new TaskListener<>() {
                @Override
                public void onResponse(Task task, Response searchResponse) {
                    unlink(task);
                    listener.onResponse(searchResponse);
                }

                @Override
                public void onFailure(Task task, Exception e) {
                    unlink(task);
                    listener.onFailure(e);
                }

                private void unlink(Task task) {
                    //the synchronized blocks are to make sure that only link or unlink for a specific task can happen at a given time,
                    //they can't happen concurrently. The link flag is needed because unlink can still be called before link, which would
                    //lead to piling up task ids that are never removed from the map.
                    //It may look like only either synchronized or the flag are needed but they both are. In fact, the flag is needed to
                    //ensure that we don't link a task if we have already unlinked it. But it's not enough as, once we start the linking,
                    //we do want its corresponding unlinking to happen, but only once the linking is completed. With only
                    //the flag, we would just miss unlinking for some tasks that are being linked while onResponse is called.
                    synchronized(task) {
                        try {
                            //nothing to do if link was not called yet: we would not find the task anyways.
                            if (link.getAndIncrement() > 0) {
                                CloseListener closeListener = httpChannels.get(httpChannel);
                                TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());
                                closeListener.unregisterTask(taskId);
                            }
                        } catch(Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }
            });

        CloseListener closeListener = httpChannels.computeIfAbsent(httpChannel, channel -> new CloseListener(client));
        synchronized (task) {
            //the task will only be registered if it's not completed yet, meaning if its TaskListener has not been called yet.
            //otherwise, given that its listener has already been called, the task id would never be removed.
            if (link.getAndIncrement() == 0) {
                TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());
                closeListener.registerTask(httpChannel, taskId);
            }
        }

        //TODO check that no tasks are left behind through assertions at node close. Not sure how. Couldn't there be in-flight requests
        //causing channels to be in the map when a node gets closed?
    }

    final class CloseListener implements ActionListener<Void> {
        private final Client client;
        final Set<TaskId> taskIds = new CopyOnWriteArraySet<>();
        private final AtomicReference<HttpChannel> channel = new AtomicReference<>();

        CloseListener(Client client) {
            this.client = client;
        }

        void registerTask(HttpChannel httpChannel, TaskId taskId) {
            if (channel.compareAndSet(null, httpChannel)) {
                //In case the channel is already closed when we register the listener, the listener will be immediately executed which will
                //remove the channel from the map straight-away. That is why we first create the CloseListener and later we associate it
                //with the channel. This guarantees that the close listener is already in the map when the it gets registered to its
                //corresponding channel, hence it is always found in the map when it gets invoked if the channel gets closed.
                httpChannel.addCloseListener(this);
            }
            this.taskIds.add(taskId);
        }

        private void unregisterTask(TaskId taskId) {
            this.taskIds.remove(taskId);
        }

        @Override
        public void onResponse(Void aVoid) {
            cancelTasks();
        }

        @Override
        public void onFailure(Exception e) {
            cancelTasks();
        }

        private void cancelTasks() {
            //When the channel gets closed it won't be reused: we can remove it from the map as there is no chance we will
            //register another close listener to it later.
            //The channel reference may be null, if the connection gets closed before it got set.
            HttpChannel channel = this.channel.get();
            //TODO is this enough to make sure that we only cancel tasks once? it could be that not all tasks have been registered yet
            //when the close listener is notified. We remove the channel and cancel the tasks that are known up until then.
            //if new tasks come in from the same channel, the channel will be added again to the map, but the close listener will
            //be registered another time to it which is no good. tasks should not be left behind though.
            if (channel != null && httpChannels.remove(channel) != null) {
                for (TaskId previousTaskId : taskIds) {
                    CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                    cancelTasksRequest.setTaskId(previousTaskId);
                    //We don't wait for cancel tasks to come back. Task cancellation is just best effort.
                    //Note that cancel tasks fails if the user sending the search request does not have the permissions to call it.
                    client.admin().cluster().cancelTasks(cancelTasksRequest, ActionListener.wrap(r -> {}, e -> {}));
                }
            }
        }
    }
}
