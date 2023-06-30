/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PutWatchResponse extends ActionResponse implements ToXContentObject {

    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private final boolean created;

    public PutWatchResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        version = in.readVLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        created = in.readBoolean();
    }

    public PutWatchResponse(String id, long version, long seqNo, long primaryTerm, boolean created) {
        this.id = id;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.created = created;
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public boolean isCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PutWatchResponse that = (PutWatchResponse) o;

        return Objects.equals(id, that.id)
            && Objects.equals(version, that.version)
            && Objects.equals(seqNo, that.seqNo)
            && Objects.equals(primaryTerm, that.primaryTerm)
            && Objects.equals(created, that.created);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, seqNo, primaryTerm, created);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVLong(version);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeBoolean(created);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("_id", id)
            .field("_version", version)
            .field("_seq_no", seqNo)
            .field("_primary_term", primaryTerm)
            .field("created", created)
            .endObject();
    }

}
