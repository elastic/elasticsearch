/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.support.xcontent.XContentSource;

import java.io.IOException;

public class GetWatchResponse extends ActionResponse {

    private String id;
    private long version = -1;
    private boolean found = false;
    private XContentSource source;

    GetWatchResponse() {
    }

    /**
     * ctor for missing watch
     */
    public GetWatchResponse(String id) {
        this.id = id;
        this.version = -1;
        this.found = false;
        this.source = null;
    }

    /**
     * ctor for found watch
     */
    public GetWatchResponse(String id, long version, BytesReference source, XContentType contentType) {
        this.id = id;
        this.version = version;
        this.found = true;
        this.source = new XContentSource(source, contentType);
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

    public XContentSource getSource() {
        return source;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        found = in.readBoolean();
        version = in.readLong();
        source = found ? XContentSource.readFrom(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBoolean(found);
        out.writeLong(version);
        if (found) {
            XContentSource.writeTo(source, out);
        }
    }
}
