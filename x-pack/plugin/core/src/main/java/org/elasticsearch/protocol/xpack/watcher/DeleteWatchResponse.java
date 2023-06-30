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

public class DeleteWatchResponse extends ActionResponse implements ToXContentObject {

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
        return builder.startObject().field("_id", id).field("_version", version).field("found", found).endObject();
    }

}
