/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.globalstate;

import org.elasticsearch.action.ActionType;

/**
 * Snapshots global state action
 */
public class SnapshotGlobalStateAction extends ActionType<SnapshotGlobalStateResponse> {

    public static final SnapshotGlobalStateAction INSTANCE = new SnapshotGlobalStateAction();
    public static final String NAME = "cluster:admin/snapshot/get/global_state";

    private SnapshotGlobalStateAction() {
        super(NAME, SnapshotGlobalStateResponse::new);
    }
}
