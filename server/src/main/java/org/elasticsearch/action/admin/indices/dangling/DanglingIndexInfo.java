/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Contains information about a dangling index, i.e. an index that Elasticsearch has found
 * on-disk but is not present in the cluster state.
 */
public class DanglingIndexInfo implements Writeable {
    private final String nodeId;
    private final String indexName;
    private final String indexUUID;
    private final long creationDateMillis;

    public DanglingIndexInfo(String nodeId, String indexName, String indexUUID, long creationDateMillis) {
        this.nodeId = nodeId;
        this.indexName = indexName;
        this.indexUUID = indexUUID;
        this.creationDateMillis = creationDateMillis;
    }

    public DanglingIndexInfo(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.indexName = in.readString();
        this.indexUUID = in.readString();
        this.creationDateMillis = in.readLong();
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public long getCreationDateMillis() {
        return creationDateMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeString(this.indexName);
        out.writeString(this.indexUUID);
        out.writeLong(this.creationDateMillis);
    }
}
