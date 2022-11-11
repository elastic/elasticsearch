/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Models the health api usage section in the XPack usage response
 */
public class HealthApiFeatureSetUsage extends XPackFeatureSet.Usage {

    private final Map<String, Object> stats;

    public HealthApiFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        stats = in.readMap();
    }

    public HealthApiFeatureSetUsage(boolean available, boolean enabled, Map<String, Object> stats) {
        super(XPackField.HEALTH, available, enabled);
        this.stats = stats;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_6_0;
    }

    public Map<String, Object> stats() {
        return stats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        for (Map.Entry<String, Object> entry : stats.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(stats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HealthApiFeatureSetUsage that = (HealthApiFeatureSetUsage) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }
}
