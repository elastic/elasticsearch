/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for cancelling running tasks
 */
public class CancelTasksAction extends ActionType<CancelTasksResponse> {

    public static final CancelTasksAction INSTANCE = new CancelTasksAction();
    public static final String NAME = "cluster:admin/tasks/cancel";

    private CancelTasksAction() {
        super(NAME, CancelTasksResponse::new);
    }
}

