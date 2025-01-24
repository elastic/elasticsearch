/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.monitoring;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class MonitoringFeatureSetUsage extends XPackFeatureUsage {

    @Nullable
    private Boolean collectionEnabled;
    @Nullable
    private Map<String, Object> exporters;

    public MonitoringFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        exporters = in.readGenericMap();
        collectionEnabled = in.readOptionalBoolean();
    }

    public MonitoringFeatureSetUsage(boolean collectionEnabled, Map<String, Object> exporters) {
        super(XPackField.MONITORING, true, true);
        this.exporters = exporters;
        this.collectionEnabled = collectionEnabled;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    public Map<String, Object> getExporters() {
        return exporters == null ? Collections.emptyMap() : Collections.unmodifiableMap(exporters);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(exporters);
        out.writeOptionalBoolean(collectionEnabled);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (collectionEnabled != null) {
            builder.field("collection_enabled", collectionEnabled);
        }
        if (exporters != null) {
            builder.field("enabled_exporters", exporters);
        }
    }
}
