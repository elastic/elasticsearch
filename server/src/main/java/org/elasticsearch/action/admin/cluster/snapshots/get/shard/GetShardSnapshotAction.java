/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get.shard;

import org.elasticsearch.action.ActionType;

public class GetShardSnapshotAction extends ActionType<GetShardSnapshotResponse> {

    public static final GetShardSnapshotAction INSTANCE = new GetShardSnapshotAction();
    public static final String NAME = "internal:admin/snapshot/get_shard";

    public GetShardSnapshotAction() {
        super(NAME, GetShardSnapshotResponse::new);
    }
}
