/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.ActionType;

public class NodesHotThreadsAction extends ActionType<NodesHotThreadsResponse> {

    public static final NodesHotThreadsAction INSTANCE = new NodesHotThreadsAction();
    public static final String NAME = "cluster:monitor/nodes/hot_threads";

    private NodesHotThreadsAction() {
        super(NAME, NodesHotThreadsResponse::new);
    }
}
