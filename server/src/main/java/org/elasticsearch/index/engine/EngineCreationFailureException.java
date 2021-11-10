/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * An exception indicating that an {@link Engine} creation failed.
 *
 *
 */
public class EngineCreationFailureException extends EngineException {

    public EngineCreationFailureException(ShardId shardId, String msg, Throwable cause) {
        super(shardId, msg, cause);
    }

    public EngineCreationFailureException(StreamInput in) throws IOException {
        super(in);
    }

}
