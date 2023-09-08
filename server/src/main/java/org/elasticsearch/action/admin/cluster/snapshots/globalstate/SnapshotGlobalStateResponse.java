/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.globalstate;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class SnapshotGlobalStateResponse extends ActionResponse implements ToXContentObject {

    private final Metadata metadata;

    public SnapshotGlobalStateResponse(StreamInput in) throws IOException {
        super(in);
        metadata = in.readOptionalWriteable(Metadata::readFrom);
    }

    public SnapshotGlobalStateResponse(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(metadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("clusterUUID", metadata.clusterUUID());
        builder.field("clusterCommittedUUID", metadata.clusterUUIDCommitted());
        builder.field("indices", metadata.indices());
        builder.endObject();
        return builder;
    }
}
