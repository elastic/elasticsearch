/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action.bulk;

import org.elasticsearch.action.ActionType;

public class BulkShardOperationsAction extends ActionType<BulkShardOperationsResponse> {

    public static final BulkShardOperationsAction INSTANCE = new BulkShardOperationsAction();
    public static final String NAME = "indices:data/write/bulk_shard_operations[s]";

    private BulkShardOperationsAction() {
        super(NAME, BulkShardOperationsResponse::new);
    }
}
