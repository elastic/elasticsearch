/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown when some action cannot be performed because the primary shard of
 * some shard group in an index has not been allocated post api action.
 */
public final class IndexPrimaryShardNotAllocatedException extends ElasticsearchException {
    public IndexPrimaryShardNotAllocatedException(StreamInput in) throws IOException {
        super(in);
    }

    public IndexPrimaryShardNotAllocatedException(Index index) {
        super("primary not allocated post api");
        setIndex(index);
    }

    @Override
    public RestStatus status() {
        return RestStatus.INTERNAL_SERVER_ERROR;
    }
}
