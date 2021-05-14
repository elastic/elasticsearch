/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.ActionType;

public class ClusterHealthAction extends ActionType<ClusterHealthResponse> {

    public static final ClusterHealthAction INSTANCE = new ClusterHealthAction();
    public static final String NAME = "cluster:monitor/health";

    private ClusterHealthAction() {
        super(NAME, ClusterHealthResponse::new);
    }
}
