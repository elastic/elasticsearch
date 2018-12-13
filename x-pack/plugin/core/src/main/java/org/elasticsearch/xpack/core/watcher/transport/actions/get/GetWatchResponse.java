/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;

import java.io.IOException;
import java.util.Objects;

public class GetWatchResponse extends ActionResponse implements ToXContent {

    private String id;
    private WatchStatus status;
    private boolean found;
    private XContentSource source;
    private long version;

    public GetWatchResponse() {
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
    }

    /**
     * ctor for found watch
     */
    public GetWatchResponse(String id, long version, WatchStatus status, XContentSource source) {
        this.id = id;
        this.status = status;
        this.found = true;
        this.source = source;
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
        } else {
            status = null;
            source = null;
            version = Versions.NOT_FOUND;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("found", found);
        builder.field("_id", id);
        if (found) {
            builder.field("_version", version);
            builder.field("status", status,  params);
            builder.field("watch", source, params);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetWatchResponse that = (GetWatchResponse) o;
        return version == that.version &&
            Objects.equals(id, that.id) &&
            Objects.equals(status, that.status) &&
            Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, status, version);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
