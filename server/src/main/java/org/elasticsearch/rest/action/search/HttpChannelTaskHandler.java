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
import org.elasticsearch.client.Client;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.TaskId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This class keeps track of which tasks came in from which {@link HttpChannel}, by allowing to associate
 * an {@link HttpChannel} with a {@link TaskId}, and also removing the link once the task is complete.
 * Additionally, it accepts a consumer that gets called whenever an http channel gets closed, which
 * can be used to cancel the associated task when the underlying connection gets closed.
 */
final class HttpChannelTaskHandler {
    private final Map<HttpChannel, Set<TaskId>> httpChannels = new ConcurrentHashMap<>();
    private final BiConsumer<Client, TaskId> onChannelClose;

    HttpChannelTaskHandler(BiConsumer<Client, TaskId> onChannelClose) {
        this.onChannelClose = onChannelClose;
    }

    void linkChannelWithTask(HttpChannel httpChannel, Client client, TaskId taskId) {
        Set<TaskId> taskIds = httpChannels.computeIfAbsent(httpChannel, channel -> {
            //Register the listener only once we see each channel for the first time.
            //In case the channel is already closed when we register the listener, the listener will be immediately executed, which
            //is fine as we have already added the channel to the map, hence the task will be cancelled straight-away
            httpChannel.addCloseListener(ActionListener.wrap(
                response -> {
                    //When the channel gets closed it won't be reused: we can remove it from the map as there is no chance we will
                    //register another close listener to it later.
                    Set<TaskId> previousTaskIds = httpChannels.remove(httpChannel);
                    assert previousTaskIds != null : "channel not found in the map, already closed?";
                    for (TaskId previousTaskId : previousTaskIds) {
                        onChannelClose.accept(client, previousTaskId);
                    }
                },
                exception -> {}));
            return new CopyOnWriteArraySet<>();
        });
        taskIds.add(taskId);
    }

    void unlinkChannelFromTask(HttpChannel httpChannel, TaskId taskId) {
        //Execution completed: leave the channel in the map as it may be reused later and we need to make sure that we don't register
        //another close listener to it. Unset its value as the corresponding task is completed
        Set<TaskId> taskIds = httpChannels.get(httpChannel);
        //It could happen that the task is completed even before it gets associated with its channel, in which case it won't be in the map
        if (taskIds != null) {
            taskIds.remove(taskId);
        }
    }
}
