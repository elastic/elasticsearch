/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.find;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Used when querying every node in the cluster for a specific dangling index.
 */
public class NodeFindDanglingIndexRequest extends TransportRequest {
    private final String indexUUID;

    public NodeFindDanglingIndexRequest(String indexUUID) {
        this.indexUUID = indexUUID;
    }

    public NodeFindDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readString();
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexUUID);
    }
}
