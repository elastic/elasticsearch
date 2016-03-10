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

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.tasks.MockTaskManagerListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * MockTaskManagerListener that records all task registration/unregistration events
 */
public class RecordingTaskManagerListener implements MockTaskManagerListener {

    private String[] actionMasks;
    private DiscoveryNode localNode;

    private List<Tuple<Boolean, TaskInfo>> events  = new ArrayList<>();

    public RecordingTaskManagerListener(DiscoveryNode localNode, String... actionMasks) {
        this.actionMasks = actionMasks;
        this.localNode = localNode;
    }

    @Override
    public synchronized void onTaskRegistered(Task task) {
        if (Regex.simpleMatch(actionMasks, task.getAction())) {
            events.add(new Tuple<>(true, task.taskInfo(localNode, true)));
        }
    }

    @Override
    public synchronized void onTaskUnregistered(Task task) {
        if (Regex.simpleMatch(actionMasks, task.getAction())) {
            events.add(new Tuple<>(false, task.taskInfo(localNode, true)));
        }
    }

    public synchronized List<Tuple<Boolean, TaskInfo>> getEvents() {
        return Collections.unmodifiableList(new ArrayList<>(events));
    }

    public synchronized List<TaskInfo> getRegistrationEvents() {
        List<TaskInfo> events = new ArrayList<>();
        for (Tuple<Boolean, TaskInfo> event : this.events) {
            if(event.v1()) {
                events.add(event.v2());
            }
        }
        return Collections.unmodifiableList(events);
    }

    public synchronized List<TaskInfo> getUnregistrationEvents() {
        List<TaskInfo> events = new ArrayList<>();
        for (Tuple<Boolean, TaskInfo> event : this.events) {
            if(event.v1() == false) {
                events.add(event.v2());
            }
        }
        return Collections.unmodifiableList(events);
    }

    public synchronized void reset() {
        events.clear();
    }

}
