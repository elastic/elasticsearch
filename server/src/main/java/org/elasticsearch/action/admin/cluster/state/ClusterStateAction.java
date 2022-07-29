/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.ActionType;

public class ClusterStateAction extends ActionType<ClusterStateResponse> {

    public static final ClusterStateAction INSTANCE = new ClusterStateAction();
    public static final String NAME = "cluster:monitor/state";

    private ClusterStateAction() {
        super(NAME, ClusterStateResponse::new);
    }
}
