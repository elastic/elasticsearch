/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class DeleteWatchResponse extends ActionResponse {

    private String id;
    private long version;
    private boolean found;

    public DeleteWatchResponse() {
    }

    public DeleteWatchResponse(String id, long version, boolean found) {
        this.id = id;
        this.version = version;
        this.found = found;
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        version = in.readVLong();
        found = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeVLong(version);
        out.writeBoolean(found);
    }
}
