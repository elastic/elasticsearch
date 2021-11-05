/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.get;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for retrieving a list of currently running tasks
 */
public class GetTaskAction extends ActionType<GetTaskResponse> {
    public static final String TASKS_ORIGIN = "tasks";

    public static final GetTaskAction INSTANCE = new GetTaskAction();
    public static final String NAME = "cluster:monitor/task/get";

    private GetTaskAction() {
        super(NAME, GetTaskResponse::new);
    }
}
