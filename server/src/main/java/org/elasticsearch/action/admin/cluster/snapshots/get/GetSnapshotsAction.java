/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.action.ActionType;

/**
 * Get snapshots action
 */
public class GetSnapshotsAction extends ActionType<GetSnapshotsResponse> {

    public static final GetSnapshotsAction INSTANCE = new GetSnapshotsAction();
    public static final String NAME = "cluster:admin/snapshot/get";

    private GetSnapshotsAction() {
        super(NAME, GetSnapshotsResponse::new);
    }

}
