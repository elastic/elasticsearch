/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;

public class RethrottleAction extends ActionType<ListTasksResponse> {
    public static final RethrottleAction INSTANCE = new RethrottleAction();
    public static final String NAME = "cluster:admin/reindex/rethrottle";

    private RethrottleAction() {
        super(NAME, ListTasksResponse::new);
    }
}
