/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.ActionType;

public class NodesInfoAction extends ActionType<NodesInfoResponse> {

    public static final NodesInfoAction INSTANCE = new NodesInfoAction();
    public static final String NAME = "cluster:monitor/nodes/info";

    private NodesInfoAction() {
        super(NAME, NodesInfoResponse::new);
    }
}
