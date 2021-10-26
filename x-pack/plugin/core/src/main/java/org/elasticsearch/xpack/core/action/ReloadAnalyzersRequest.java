/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Request for reloading index search analyzers
 */
public class ReloadAnalyzersRequest extends BroadcastRequest<ReloadAnalyzersRequest> {

    /**
     * Constructs a new request for reloading index search analyzers for one or more indices
     */
    public ReloadAnalyzersRequest(String... indices) {
        super(indices);
    }

    public ReloadAnalyzersRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReloadAnalyzersRequest that = (ReloadAnalyzersRequest) o;
        return Objects.equals(indicesOptions(), that.indicesOptions())
                && Arrays.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesOptions(), Arrays.hashCode(indices));
    }

}
