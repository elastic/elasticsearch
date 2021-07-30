/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class EngineException extends ElasticsearchException {

    public EngineException(ShardId shardId, String msg, Object... params) {
        this(shardId, msg, null, params);
    }

    public EngineException(ShardId shardId, String msg, Throwable cause, Object... params) {
        super(msg, cause, params);
        setShard(shardId);
    }

    public EngineException(StreamInput in) throws IOException {
        super(in);
    }
}
