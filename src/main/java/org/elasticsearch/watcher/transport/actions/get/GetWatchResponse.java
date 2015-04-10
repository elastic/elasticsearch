/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;

public class GetWatchResponse extends ActionResponse {

    private String id;
    private long version = -1;
    private boolean found = false;
    private BytesReference source;

    private Map<String, Object> sourceAsMap;

    GetWatchResponse() {
    }

    public GetWatchResponse(String id, long version, boolean found, BytesReference source) {
        this.id = id;
        this.version = version;
        this.found = found;
        this.source = source;
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

    public BytesReference getSource() {
        return source;
    }

    public Map<String, Object> getSourceAsMap() throws ElasticsearchParseException {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = SourceLookup.sourceAsMap(source);
        return sourceAsMap;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        found = in.readBoolean();
        version = in.readLong();
        source = found ? in.readBytesReference() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBoolean(found);
        out.writeLong(version);
        if (found) {
            out.writeBytesReference(source);
        }
    }
}
