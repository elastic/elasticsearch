/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.monitoring.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;
import java.util.Objects;

public class MonitoringBulkDoc implements Writeable {

    private final MonitoredSystem system;
    private final String type;
    private final String id;
    private final long timestamp;
    private final long intervalMillis;
    private final BytesReference source;
    private final XContentType xContentType;

    public MonitoringBulkDoc(final MonitoredSystem system,
                             final String type,
                             @Nullable final String id,
                             final long timestamp,
                             final long intervalMillis,
                             final BytesReference source,
                             final XContentType xContentType) {

        this.system = Objects.requireNonNull(system);
        this.type = Objects.requireNonNull(type);
        // We allow strings to be "" because Logstash 5.2 - 5.3 would submit empty _id values for time-based documents
        this.id = Strings.isNullOrEmpty(id) ? null : id;
        this.timestamp = timestamp;
        this.intervalMillis = intervalMillis;
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    /**
     * Read from a stream.
     */
    public static MonitoringBulkDoc readFrom(StreamInput in) throws IOException {
        final MonitoredSystem system = MonitoredSystem.fromSystem(in.readOptionalString());

        if (in.getVersion().before(Version.V_6_0_0_rc1)) {
            in.readOptionalString(); // Monitoring version, removed in 6.0 rc1
            in.readOptionalString(); // Cluster UUID, removed in 6.0 rc1
        }

        final long timestamp = in.readVLong();

        if (in.getVersion().before(Version.V_6_0_0_rc1)) {
            in.readOptionalWriteable(MonitoringDoc.Node::new);// Source node, removed in 6.0 rc1
            MonitoringIndex.readFrom(in);// Monitoring index, removed in 6.0 rc1
        }

        final String type = in.readOptionalString();
        final String id = in.readOptionalString();
        final BytesReference source = in.readBytesReference();
        final XContentType xContentType = (source != BytesArray.EMPTY) ? in.readEnum(XContentType.class) : XContentType.JSON;

        long interval = 0L;
        if (in.getVersion().onOrAfter(Version.V_6_0_0_rc1)) {
            interval = in.readVLong();
        }
        return new MonitoringBulkDoc(system, type, id, timestamp, interval, source, xContentType);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(system.getSystem());
        if (out.getVersion().before(Version.V_6_0_0_rc1)) {
            out.writeOptionalString(MonitoringTemplateUtils.TEMPLATE_VERSION);
            out.writeOptionalString(null);
        }
        out.writeVLong(timestamp);
        if (out.getVersion().before(Version.V_6_0_0_rc1)) {
            out.writeOptionalWriteable(null);
            MonitoringIndex.IGNORED_DATA.writeTo(out);
        }
        out.writeOptionalString(type);
        out.writeOptionalString(id);
        out.writeBytesReference(source);
        if (source != BytesArray.EMPTY) {
            out.writeEnum(xContentType);
        }
        if (out.getVersion().onOrAfter(Version.V_6_0_0_rc1)) {
            out.writeVLong(intervalMillis);
        }
    }

    public MonitoredSystem getSystem() {
        return system;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getIntervalMillis() {
        return intervalMillis;
    }

    public BytesReference getSource() {
        return source;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MonitoringBulkDoc that = (MonitoringBulkDoc) o;
        return timestamp == that.timestamp
                && intervalMillis == that.intervalMillis
                && system == that.system
                && Objects.equals(type, that.type)
                && Objects.equals(id, that.id)
                && Objects.equals(source, that.source)
                && xContentType == that.xContentType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(system, type, id, timestamp, intervalMillis, source, xContentType);
    }
}
