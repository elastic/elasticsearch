/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * {@link BytesReferenceMonitoringDoc} is a {@link MonitoringDoc} that prints out a {@link BytesReference}
 * source when its XContent is rendered.
 */
public class BytesReferenceMonitoringDoc extends MonitoringDoc {

    private final XContentType xContentType;
    private final BytesReference source;

    public BytesReferenceMonitoringDoc(final String cluster,
                                       final long timestamp,
                                       final long intervalMillis,
                                       @Nullable final Node node,
                                       final MonitoredSystem system,
                                       final String type,
                                       @Nullable final String id,
                                       final XContentType xContentType,
                                       final BytesReference source) {
        super(cluster, timestamp, intervalMillis, node, system, type, id);
        this.xContentType = Objects.requireNonNull(xContentType);
        this.source = Objects.requireNonNull(source);
    }

    XContentType getXContentType() {
        return xContentType;
    }

    BytesReference getSource() {
        return source;
    }

    @Override
    protected void innerToXContent(final XContentBuilder builder, final Params params) throws IOException {
        if (source.length() > 0) {
            try (InputStream stream = source.streamInput()) {
                builder.rawField(getType(), stream, xContentType);
            }
        } else {
            builder.nullField(getType());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BytesReferenceMonitoringDoc that = (BytesReferenceMonitoringDoc) o;
        return xContentType == that.xContentType && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), xContentType, source);
    }
}
