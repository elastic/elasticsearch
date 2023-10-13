/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.globalstate;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;

public class SnapshotGlobalStateResponse extends ActionResponse implements ChunkedToXContentObject {

    private final Metadata metadata;

    public SnapshotGlobalStateResponse(StreamInput in) throws IOException {
        super(in);
        assert in.getTransportVersion().onOrAfter(TransportVersions.SNAPSHOT_GLOBAL_STATE_API_ADDED) : in.getTransportVersion();
        metadata = Metadata.readFrom(in);
    }

    public SnapshotGlobalStateResponse(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getTransportVersion().onOrAfter(TransportVersions.SNAPSHOT_GLOBAL_STATE_API_ADDED) : out.getTransportVersion();
        out.writeOptionalWriteable(metadata);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            metadata.toXContentChunked(params),
            ChunkedToXContentHelper.endObject()
        );
    }
}
