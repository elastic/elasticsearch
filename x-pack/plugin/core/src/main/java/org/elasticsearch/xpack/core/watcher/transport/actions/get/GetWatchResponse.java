/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;

import java.io.IOException;

public class GetWatchResponse extends ActionResponse {

    private String id;
    private WatchStatus status;
    private boolean found = false;
    private XContentSource source;
    private long version;

    public GetWatchResponse() {
    }

    /**
     * ctor for missing watch
     */
    public GetWatchResponse(String id) {
        this.id = id;
        this.found = false;
        this.source = null;
        version = Versions.NOT_FOUND;
    }

    /**
     * ctor for found watch
     */
    public GetWatchResponse(String id, long version, WatchStatus status, BytesReference source, XContentType contentType) {
        this.id = id;
        this.status = status;
        this.found = true;
        this.source = new XContentSource(source, contentType);
        this.version = version;
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        found = in.readBoolean();
        if (found) {
            status = WatchStatus.read(in);
            source = XContentSource.readFrom(in);
            version = in.readZLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBoolean(found);
        if (found) {
            status.writeTo(out);
            XContentSource.writeTo(source, out);
            out.writeZLong(version);
        }
    }
}
