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

public class DeleteWatchResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<DeleteWatchResponse, Void> PARSER
        = new ObjectParser<>("x_pack_delete_watch_response", DeleteWatchResponse::new);
    static {
        PARSER.declareString(DeleteWatchResponse::setId, new ParseField("_id"));
        PARSER.declareLong(DeleteWatchResponse::setVersion, new ParseField("_version"));
        PARSER.declareBoolean(DeleteWatchResponse::setFound, new ParseField("found"));
    }

    private String id;
    private long version;
    private boolean found;

    public DeleteWatchResponse() {}

    public DeleteWatchResponse(String id, long version, boolean found) {
        this.id = id;
        this.version = version;
        this.found = found;
    }

    public DeleteWatchResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        version = in.readVLong();
        found = in.readBoolean();
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public boolean isFound() {
        return found;
    }

    private void setId(String id) {
        this.id = id;
    }

    private void setVersion(long version) {
        this.version = version;
    }

    private void setFound(boolean found) {
        this.found = found;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteWatchResponse that = (DeleteWatchResponse) o;

        return Objects.equals(id, that.id) && Objects.equals(version, that.version) && Objects.equals(found, that.found);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, found);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVLong(version);
        out.writeBoolean(found);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("_id", id)
            .field("_version", version)
            .field("found", found)
            .endObject();
    }

    public static DeleteWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
