/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;

import java.io.IOException;
import java.util.Objects;

public class GetWatchResponse extends ActionResponse implements ToXContentObject {

    private String id;
    private WatchStatus status;
    private boolean found;
    private XContentSource source;
    private long version;
    private long seqNo;
    private long primaryTerm;

    public GetWatchResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        found = in.readBoolean();
        if (found) {
            status = new WatchStatus(in);
            source = XContentSource.readFrom(in);
            version = in.readZLong();
            seqNo = in.readZLong();
            primaryTerm = in.readVLong();
        } else {
            status = null;
            source = null;
            version = Versions.NOT_FOUND;
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
        }
    }

    /**
     * ctor for missing watch
     */
    public GetWatchResponse(String id) {
        this.id = id;
        this.status = null;
        this.found = false;
        this.source = null;
        this.version = Versions.NOT_FOUND;
        this.seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        this.primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    }

    /**
     * ctor for found watch
     */
    public GetWatchResponse(String id, long version, long seqNo, long primaryTerm, WatchStatus status, XContentSource source) {
        this.id = id;
        this.status = status;
        this.found = true;
        this.source = source;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
    }

    public String getId() {
        return id;
    }

    public WatchStatus getStatus() {
        return status;
    }

    public boolean isFound() {
        return found;
    }

    public XContentSource getSource() {
        return source;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBoolean(found);
        if (found) {
            status.writeTo(out);
            XContentSource.writeTo(source, out);
            out.writeZLong(version);
            out.writeZLong(seqNo);
            out.writeVLong(primaryTerm);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("found", found);
        builder.field("_id", id);
        if (found) {
            builder.field("_version", version);
            builder.field("_seq_no", seqNo);
            builder.field("_primary_term", primaryTerm);
            builder.field("status", status,  params);
            builder.field("watch", source, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetWatchResponse that = (GetWatchResponse) o;
        return version == that.version && seqNo == that.seqNo && primaryTerm == that.primaryTerm &&
            Objects.equals(id, that.id) &&
            Objects.equals(status, that.status) &&
            Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, status, version, seqNo, primaryTerm);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
