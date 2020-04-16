/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.analytics;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class AnalyticsFeatureSetUsage extends XPackFeatureSet.Usage {

    private final Map<String, Object> stats;

    public AnalyticsFeatureSetUsage(boolean available, boolean enabled, Map<String, Object> stats) {
        super(XPackField.ANALYTICS, available, enabled);
        this.stats = stats;
    }

    public AnalyticsFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        if (input.getVersion().onOrAfter(Version.V_7_8_0)) {
            stats = input.readMap();
        } else {
            stats = Collections.emptyMap();
        }
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_4_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            out.writeMap(stats);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, stats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AnalyticsFeatureSetUsage other = (AnalyticsFeatureSetUsage) obj;
        return Objects.equals(available, other.available) &&
            Objects.equals(enabled, other.enabled) &&
            Objects.equals(stats, other.stats);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (enabled) {
            builder.startObject("stats");
            for (Map.Entry<String, Object> entry : stats.entrySet()) {
                builder.field(entry.getKey() + "_usage", entry.getValue());
            }
            builder.endObject();
        }
    }
}
