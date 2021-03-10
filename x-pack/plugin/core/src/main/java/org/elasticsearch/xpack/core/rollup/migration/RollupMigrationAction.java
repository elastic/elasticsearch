/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.migration;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

public class RollupMigrationAction extends ActionType<BulkByScrollResponse> {

    public static final RollupMigrationAction INSTANCE = new RollupMigrationAction();
    public static final String NAME = "indices:admin/xpack/rollup/migrate";

    private RollupMigrationAction() {
        super(NAME, BulkByScrollResponse::new);
    }
}
