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

import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Task storing information about a currently running search request in the coordinating node. Same as {@link SearchTask} but it also
 * holds some specific {@link org.elasticsearch.tasks.Task.Status} to report progress of the search as part of the task execution
 */
public final class MainSearchTask extends SearchTask {

    private final MainSearchTaskStatus mainSearchTaskStatus = new MainSearchTaskStatus();

    public MainSearchTask(long id, String type, String action, Supplier<String> description,
                          TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public MainSearchTaskStatus getStatus() {
        return mainSearchTaskStatus;
    }
}
