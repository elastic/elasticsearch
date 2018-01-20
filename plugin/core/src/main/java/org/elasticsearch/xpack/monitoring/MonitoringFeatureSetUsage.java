/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackField;

import java.io.IOException;
import java.util.Map;

public class MonitoringFeatureSetUsage extends XPackFeatureSet.Usage {

    private static final String ENABLED_EXPORTERS_XFIELD = "enabled_exporters";

    @Nullable
    private Map<String, Object> exporters;

    public MonitoringFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        exporters = in.readMap();
    }

    public MonitoringFeatureSetUsage(boolean available, boolean enabled, Map<String, Object> exporters) {
        super(XPackField.MONITORING, available, enabled);
        this.exporters = exporters;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(exporters);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (exporters != null) {
            builder.field(ENABLED_EXPORTERS_XFIELD, exporters);
        }
    }
}
