/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.ActionType;

/**
 * Create snapshot action
 */
public class CreateSnapshotAction extends ActionType<CreateSnapshotResponse> {

    public static final CreateSnapshotAction INSTANCE = new CreateSnapshotAction();
    public static final String NAME = "cluster:admin/snapshot/create";

    private CreateSnapshotAction() {
        super(NAME, CreateSnapshotResponse::new);
    }
}
