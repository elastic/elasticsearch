/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for retrieving a list of currently running tasks
 */
public class ListTasksAction extends ActionType<ListTasksResponse> {

    public static final ListTasksAction INSTANCE = new ListTasksAction();
    public static final String NAME = "cluster:monitor/tasks/lists";

    private ListTasksAction() {
        super(NAME, ListTasksResponse::new);
    }

}
