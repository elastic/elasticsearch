/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;

import java.io.IOException;

public class MonitoringBulkDoc extends MonitoringDoc {

    private String index;
    private String type;
    private String id;

    private BytesReference source;

    public MonitoringBulkDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    /**
     * Read from a stream.
     */
    public MonitoringBulkDoc(StreamInput in) throws IOException {
        super(in);
        index = in.readOptionalString();
        type = in.readOptionalString();
        id = in.readOptionalString();
        source = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(index);
        out.writeOptionalString(type);
        out.writeOptionalString(id);
        out.writeBytesReference(source);
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public BytesReference getSource() {
        return source;
    }

    public void setSource(BytesReference source) {
        this.source = source;
    }
}
