/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;

public class NodeSearchRequest extends TransportRequest {
    private final List<ShardSearchRequest> shardRequests;

    public NodeSearchRequest(StreamInput in) throws IOException {
        super(in);
        shardRequests = in.readCollectionAsList(ShardSearchRequest::new);
    }

    public NodeSearchRequest(List<ShardSearchRequest> shardRequests) {
        this.shardRequests = shardRequests;
    }

    public List<ShardSearchRequest> getShardRequests() {
        return shardRequests;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(shardRequests);
    }
}
