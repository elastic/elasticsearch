/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.ActionType;

/**
 * Snapshots status action
 */
public class SnapshotsStatusAction extends ActionType<SnapshotsStatusResponse> {

    public static final SnapshotsStatusAction INSTANCE = new SnapshotsStatusAction();
    public static final String NAME = "cluster:admin/snapshot/status";

    private SnapshotsStatusAction() {
        super(NAME, SnapshotsStatusResponse::new);
    }
}
