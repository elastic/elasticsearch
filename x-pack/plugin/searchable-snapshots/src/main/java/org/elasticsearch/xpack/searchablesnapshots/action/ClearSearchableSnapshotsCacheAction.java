/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionType;

public class ClearSearchableSnapshotsCacheAction extends ActionType<ClearSearchableSnapshotsCacheResponse> {

    public static final ClearSearchableSnapshotsCacheAction INSTANCE = new ClearSearchableSnapshotsCacheAction();
    static final String NAME = "cluster:admin/xpack/searchable_snapshots/cache/clear";

    private ClearSearchableSnapshotsCacheAction() {
        super(NAME, ClearSearchableSnapshotsCacheResponse::new);
    }
}
