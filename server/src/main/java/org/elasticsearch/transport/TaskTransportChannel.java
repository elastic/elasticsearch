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

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.LeakTracker;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;

public class TaskTransportChannel implements TransportChannel {

    private static final LeakTracker leakTracker = new LeakTracker(TaskTransportChannel.class);

    private final Task task;

    private final TaskManager taskManager;
    private final TransportChannel channel;

    private final LeakTracker.Leak<TaskTransportChannel> leak;

    TaskTransportChannel(TaskManager taskManager, Task task, TransportChannel channel) {
        this.channel = channel;
        this.task = task;
        this.taskManager = taskManager;
        if (task.getAction().contains("search")) {
            leak = leakTracker.track(this);
        } else {
            leak = null;
        }
    }

    @Override
    public String getProfileName() {
        return channel.getProfileName();
    }

    @Override
    public String getChannelType() {
        return channel.getChannelType();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        endTask();
        channel.sendResponse(response);
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        endTask();
        channel.sendResponse(exception);
    }

    @Override
    public Version getVersion() {
        return channel.getVersion();
    }

    public TransportChannel getChannel() {
        return channel;
    }

    public void endTask() {
        if (leak != null) {
            leak.close();
        }
        taskManager.unregister(task);
    }
}
