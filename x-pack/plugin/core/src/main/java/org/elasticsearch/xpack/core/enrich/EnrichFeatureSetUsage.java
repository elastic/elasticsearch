/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutionStats;

public class EnrichFeatureSetUsage extends XPackFeatureSet.Usage {

    private ExecutionStats executionStats;
    private List<CoordinatorStats> coordinatorStats;

    public EnrichFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.executionStats = input.readOptionalWriteable(ExecutionStats::new);
        if (input.readBoolean()) {
            this.coordinatorStats = input.readList(CoordinatorStats::new);
        }
    }

    public EnrichFeatureSetUsage(boolean available, boolean enabled) {
        this(available, enabled, null, null);
    }

    public EnrichFeatureSetUsage(boolean available, boolean enabled, ExecutionStats executionStats,
                                 List<CoordinatorStats> coordinatorStats) {
        super(XPackField.ENRICH, available, enabled);
        this.executionStats = executionStats;
        this.coordinatorStats = coordinatorStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(executionStats);
        if (coordinatorStats == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(coordinatorStats);
        }
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (executionStats != null) {
            builder.startObject("execution_stats");
            executionStats.toXContent(builder, params);
            builder.endObject();
        }
        if (coordinatorStats != null) {
            builder.startArray("coordinator_stats");
            for (CoordinatorStats entry : coordinatorStats) {
                builder.startObject();
                entry.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    public ExecutionStats getExecutionStats() {
        return executionStats;
    }

    public List<CoordinatorStats> getCoordinatorStats() {
        return coordinatorStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichFeatureSetUsage that = (EnrichFeatureSetUsage) o;
        return Objects.equals(executionStats, that.executionStats) &&
            Objects.equals(coordinatorStats, that.coordinatorStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionStats, coordinatorStats);
    }
}
