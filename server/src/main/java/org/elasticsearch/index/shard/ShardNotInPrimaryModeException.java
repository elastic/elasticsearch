/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class ShardNotInPrimaryModeException extends IllegalIndexShardStateException {

    public ShardNotInPrimaryModeException(final ShardId shardId, final IndexShardState currentState) {
        super(shardId, currentState, "shard is not in primary mode");
    }

    public ShardNotInPrimaryModeException(final StreamInput in) throws IOException {
        super(in);
    }

}
