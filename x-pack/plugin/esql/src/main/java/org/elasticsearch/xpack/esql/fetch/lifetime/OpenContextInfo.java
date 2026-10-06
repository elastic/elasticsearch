/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.io.IOException;

/**
 * A reader context that a data node kept open for the fetch phase of a query, as its response lists it. The coordinator
 * uses it only to free the context. A document reference never needs it, because the reference names its context itself.
 */
public record OpenContextInfo(ShardId shardId, ShardSearchContextId contextId) implements Writeable {
    public OpenContextInfo(StreamInput in) throws IOException {
        this(new ShardId(in), new ShardSearchContextId(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        contextId.writeTo(out);
    }
}
