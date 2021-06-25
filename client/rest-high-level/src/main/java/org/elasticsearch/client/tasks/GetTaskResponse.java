/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.tasks;

import org.elasticsearch.common.xcontent.ParseField;
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
