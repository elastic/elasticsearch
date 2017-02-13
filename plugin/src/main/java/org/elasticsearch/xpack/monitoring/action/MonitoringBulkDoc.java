/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.io.IOException;

public class MonitoringBulkDoc extends MonitoringDoc {

    private MonitoringIndex index = MonitoringIndex.TIMESTAMPED;
    private String type;
    private String id;
    private BytesReference source;
    private XContentType xContentType;

    public MonitoringBulkDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public MonitoringBulkDoc(String monitoringId, String monitoringVersion,
                             MonitoringIndex index, String type, String id,
                             BytesReference source, XContentType xContentType) {
        super(monitoringId, monitoringVersion);
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
        this.xContentType = xContentType;
    }

    /**
     * Read from a stream.
     */
    public MonitoringBulkDoc(StreamInput in) throws IOException {
        super(in);
        index = MonitoringIndex.readFrom(in);
        type = in.readOptionalString();
        id = in.readOptionalString();
        source = in.readBytesReference();
        if (source != BytesArray.EMPTY && in.getVersion().onOrAfter(Version.V_5_3_0_UNRELEASED)) {
            xContentType = XContentType.readFrom(in);
        } else {
            xContentType = XContentFactory.xContentType(source);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        index.writeTo(out);
        out.writeOptionalString(type);
        out.writeOptionalString(id);
        out.writeBytesReference(source);
        if (source != null && source != BytesArray.EMPTY && out.getVersion().onOrAfter(Version.V_5_3_0_UNRELEASED)) {
            xContentType.writeTo(out);
        }
    }

    public MonitoringIndex getIndex() {
        return index;
    }

    public void setIndex(MonitoringIndex index) {
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

    public void setSource(BytesReference source, XContentType xContentType) {
        this.source = source;
        this.xContentType = xContentType;
    }

    public XContentType getXContentType() {
        return xContentType;
    }
}
