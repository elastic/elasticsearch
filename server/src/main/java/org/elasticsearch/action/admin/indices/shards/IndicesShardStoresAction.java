/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for {@link TransportIndicesShardStoresAction}
 *
 * Exposes shard store information for requested indices.
 * Shard store information reports which nodes hold shard copies, how recent they are
 * and any exceptions on opening the shard index or from previous engine failures
 */
public class IndicesShardStoresAction extends ActionType<IndicesShardStoresResponse> {

    public static final IndicesShardStoresAction INSTANCE = new IndicesShardStoresAction();
    public static final String NAME = "indices:monitor/shard_stores";

    private IndicesShardStoresAction() {
        super(NAME, IndicesShardStoresResponse::new);
    }
}
