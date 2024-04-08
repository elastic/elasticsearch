/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.tasks.MockTaskManagerListener;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * MockTaskManagerListener that records all task registration/unregistration events
 */
public class RecordingTaskManagerListener implements MockTaskManagerListener {

    private String[] actionMasks;
    private String localNodeId;

    private List<Tuple<Boolean, TaskInfo>> events = new ArrayList<>();

    public RecordingTaskManagerListener(String localNodeId, String... actionMasks) {
        this.actionMasks = actionMasks;
        this.localNodeId = localNodeId;
    }

    @Override
    public synchronized void onTaskRegistered(Task task) {
        if (Regex.simpleMatch(actionMasks, task.getAction())) {
            events.add(new Tuple<>(true, task.taskInfo(localNodeId, true)));
        }
    }

    @Override
    public synchronized void onTaskUnregistered(Task task) {
        if (Regex.simpleMatch(actionMasks, task.getAction())) {
            events.add(new Tuple<>(false, task.taskInfo(localNodeId, true)));
        }
    }

    public synchronized List<Tuple<Boolean, TaskInfo>> getEvents() {
        return List.copyOf(events);
    }

    public synchronized List<TaskInfo> getRegistrationEvents() {
        return this.events.stream().filter(Tuple::v1).map(Tuple::v2).toList();
    }

    public synchronized List<TaskInfo> getUnregistrationEvents() {
        return this.events.stream().filter(Predicate.not(Tuple::v1)).map(Tuple::v2).toList();
    }

    public synchronized void reset() {
        events.clear();
    }

}
