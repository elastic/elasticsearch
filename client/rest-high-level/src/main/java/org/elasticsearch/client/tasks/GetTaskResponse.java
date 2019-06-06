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

package org.elasticsearch.client.tasks;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.TaskInfo;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetTaskResponse {
    private final boolean completed;
    private final TaskInfo taskInfo;
    public static final ParseField COMPLETED = new ParseField("completed");
    public static final ParseField TASK = new ParseField("task");
    
    public GetTaskResponse(boolean completed, TaskInfo taskInfo) {
        this.completed = completed;
        this.taskInfo = taskInfo;
    }
    
    public boolean isCompleted() {
        return completed;
    }
    
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    private static final ConstructingObjectParser<GetTaskResponse, Void> PARSER = new ConstructingObjectParser<>("get_task",
            true, a -> new GetTaskResponse((boolean) a[0],  (TaskInfo) a[1]));
    static {
        PARSER.declareBoolean(constructorArg(), COMPLETED);
        PARSER.declareObject(constructorArg(), (p, c) -> TaskInfo.fromXContent(p), TASK);
    }    

    public static GetTaskResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }    
}
