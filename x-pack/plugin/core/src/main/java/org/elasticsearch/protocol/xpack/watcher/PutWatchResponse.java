/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class PutWatchResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<PutWatchResponse, Void> PARSER
        = new ObjectParser<>("x_pack_put_watch_response", PutWatchResponse::new);
    static {
            PARSER.declareString(PutWatchResponse::setId, new ParseField("_id"));
            PARSER.declareLong(PutWatchResponse::setVersion, new ParseField("_version"));
            PARSER.declareBoolean(PutWatchResponse::setCreated, new ParseField("created"));
    }

    private String id;
    private long version;
    private boolean created;

    public PutWatchResponse() {
    }

    public PutWatchResponse(String id, long version, boolean created) {
        this.id = id;
        this.version = version;
        this.created = created;
    }

    private void setId(String id) {
        this.id = id;
    }

    private void setVersion(long version) {
        this.version = version;
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

    public boolean isCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PutWatchResponse that = (PutWatchResponse) o;

        return Objects.equals(id, that.id) && Objects.equals(version, that.version) && Objects.equals(created, that.created);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, created);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeVLong(version);
        out.writeBoolean(created);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        version = in.readVLong();
        created = in.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("_id", id)
            .field("_version", version)
            .field("created", created)
            .endObject();
    }

    public static PutWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
