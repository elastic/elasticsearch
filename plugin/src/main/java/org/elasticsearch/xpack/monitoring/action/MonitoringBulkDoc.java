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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.io.IOException;

// TODO remove inherance from MonitoringDoc once all resolvers are removed
public class MonitoringBulkDoc extends MonitoringDoc implements Writeable {

    private MonitoringIndex index;
    private String type;
    private String id;
    private BytesReference source;
    private XContentType xContentType;

    public MonitoringBulkDoc(String monitoringId, String monitoringVersion,
                              MonitoringIndex index, String type, String id,
                              String clusterUUID, long timestamp, MonitoringDoc.Node sourceNode,
                              BytesReference source, XContentType xContentType) {
        super(monitoringId, monitoringVersion, type, id, clusterUUID, timestamp, sourceNode);
        this.index = index != null ? index : MonitoringIndex.TIMESTAMPED;
        this.type = type;
        this.id = id;
        this.source = source;
        this.xContentType = xContentType;
    }

    public MonitoringBulkDoc(String monitoringId, String monitoringVersion,
                             MonitoringIndex index, String type, String id,
                             BytesReference source, XContentType xContentType) {
        this(monitoringId, monitoringVersion, index, type, id, null, 0, null, source, xContentType);
    }

    /**
     * Read from a stream.
     *
     * Here we use a static helper method to read a serialized {@link MonitoringBulkDoc} instance
     * instead of the usual <code>MonitoringBulkDoc(StreamInput in)</code> constructor because
     * MonitoringBulkDoc still inherits from MonitoringDoc and using ctors would make things
     * cumbersome. This will be replaced by a ctor <code>MonitoringBulkDoc(StreamInput in)</code>
     * once MonitoringBulkDoc does not inherit from MonitoringDoc anymore.
     */
    public static MonitoringBulkDoc readFrom(StreamInput in) throws IOException {
        String monitoringId = in.readOptionalString();
        String monitoringVersion = in.readOptionalString();
        String clusterUUID = in.readOptionalString();
        long timestamp = in.readVLong();
        MonitoringDoc.Node sourceNode = in.readOptionalWriteable(MonitoringDoc.Node::new);
        MonitoringIndex index = MonitoringIndex.readFrom(in);
        String type = in.readOptionalString();
        String id = in.readOptionalString();
        BytesReference source = in.readBytesReference();
        XContentType xContentType;
        if (source != BytesArray.EMPTY && in.getVersion().onOrAfter(Version.V_5_3_0_UNRELEASED)) {
            xContentType = XContentType.readFrom(in);
        } else {
            xContentType = XContentFactory.xContentType(source);
        }
        return new MonitoringBulkDoc(monitoringId, monitoringVersion, index, type, id, clusterUUID,
                timestamp, sourceNode, source, xContentType);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(getMonitoringId());
        out.writeOptionalString(getMonitoringVersion());
        out.writeOptionalString(getClusterUUID());
        out.writeVLong(getTimestamp());
        out.writeOptionalWriteable(getSourceNode());
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
