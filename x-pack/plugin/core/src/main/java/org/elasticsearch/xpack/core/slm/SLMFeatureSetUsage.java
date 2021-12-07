/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class SLMFeatureSetUsage extends XPackFeatureSet.Usage {
    @Nullable
    private final SnapshotLifecycleStats slmStats;

    public SLMFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.slmStats = in.readOptionalWriteable(SnapshotLifecycleStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(this.slmStats);
    }

    public SLMFeatureSetUsage(@Nullable SnapshotLifecycleStats slmStats) {
        super(XPackField.SNAPSHOT_LIFECYCLE, true, true);
        this.slmStats = slmStats;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_5_0;
    }

    public SnapshotLifecycleStats getStats() {
        return this.slmStats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (slmStats != null) {
            builder.field("policy_count", slmStats.getMetrics().size());
            builder.field("policy_stats", slmStats);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, slmStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SLMFeatureSetUsage other = (SLMFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(slmStats, other.slmStats);
    }

}
