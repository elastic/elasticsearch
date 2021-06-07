/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action.cache;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class FrozenCacheInfoResponse extends ActionResponse {

    private final boolean hasFrozenCache;

    FrozenCacheInfoResponse(boolean hasFrozenCache) {
        this.hasFrozenCache = hasFrozenCache;
    }

    FrozenCacheInfoResponse(StreamInput in) throws IOException {
        super(in);
        hasFrozenCache = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(hasFrozenCache);
    }

    public boolean hasFrozenCache() {
        return hasFrozenCache;
    }
}
