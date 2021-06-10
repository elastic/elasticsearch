/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.Objects;

public class PutWatchResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<PutWatchResponse, Void> PARSER
        = new ObjectParser<>("x_pack_put_watch_response", PutWatchResponse::new);
    static {
            PARSER.declareString(PutWatchResponse::setId, new ParseField("_id"));
            PARSER.declareLong(PutWatchResponse::setVersion, new ParseField("_version"));
            PARSER.declareLong(PutWatchResponse::setSeqNo, new ParseField("_seq_no"));
            PARSER.declareLong(PutWatchResponse::setPrimaryTerm, new ParseField("_primary_term"));
            PARSER.declareBoolean(PutWatchResponse::setCreated, new ParseField("created"));
    }

    private String id;
    private long version;
    private long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    private boolean created;

    public PutWatchResponse() {}

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

    private void setId(String id) {
        this.id = id;
    }

    private void setVersion(long version) {
        this.version = version;
    }

    private void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    private void setPrimaryTerm(long primaryTerm) {
        this.primaryTerm = primaryTerm;
    }

    private void setCreated(boolean created) {
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

        return Objects.equals(id, that.id) && Objects.equals(version, that.version)
            && Objects.equals(seqNo, that.seqNo)
            && Objects.equals(primaryTerm, that.primaryTerm) && Objects.equals(created, that.created);
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

    public static PutWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
