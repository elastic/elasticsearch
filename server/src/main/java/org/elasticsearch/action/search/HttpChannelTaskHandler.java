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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * This class keeps track of which task came in from which {@link HttpChannel}, by allowing to associate
 * an {@link HttpChannel} with a {@link TaskId}, and also removing the link once the task is complete.
 * Additionally, it accepts a consumer that gets called whenever an http channel gets closed, which
 * can be used to cancel the associated task when the underlying connection gets closed.
 */
final class HttpChannelTaskHandler {
    private final Map<HttpChannel, TaskId> httpChannels = new ConcurrentHashMap<>();
    private final Consumer<TaskId> onChannelClose;

    HttpChannelTaskHandler(Consumer<TaskId> onChannelClose) {
        this.onChannelClose = onChannelClose;
    }

    void linkChannelWithTask(HttpChannel httpChannel, TaskId taskId) {
        TaskId previous = httpChannels.put(httpChannel, taskId);
        if (previous == null) {
            //Register the listener only once we see each channel for the first time.
            //In case the channel is already closed when we register the listener, the listener will be immediately executed, which
            //is fine as we have already added the channel to the map, hence the task will be cancelled straight-away
            httpChannel.addCloseListener(ActionListener.wrap(
                response -> {
                    //Assumption: when the channel gets closed it won't be reused, then we can remove it from the map
                    // as there is no chance we will register another close listener again to the same http channel
                    TaskId previousTaskId = httpChannels.remove(httpChannel);
                    assert previousTaskId != null : "channel not found in the map, already closed?";
                    if (previousTaskId != TaskId.EMPTY_TASK_ID) {
                        onChannelClose.accept(previousTaskId);
                    }
                },
                exception -> {}));
        } else {
            //Known channel: the close listener is already registered to it
            assert previous == TaskId.EMPTY_TASK_ID : "http channel not expected to be running another task [" + previous + "]";
            //TODO if an already seen channel gets closed very quickly, before we have associated it with its task, its close listener
            // may find the EMPTY_TASK_ID in the map in which case it will not cancel anything...
        }
    }

    void unlinkChannelFromTask(HttpChannel httpChannel, TaskId taskId) {
        //Execution completed: leave the channel in the map, but unset its value as the corresponding task is completed
        TaskId previous = httpChannels.put(httpChannel, TaskId.EMPTY_TASK_ID);
        //It could happen that the task gets completed despite it was cancelled, in which case its channel will already have been removed
        assert previous == null || taskId == previous : "channel was associated with task [" + previous + "] instead of [" + taskId + "]";
    }
}
