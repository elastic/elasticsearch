/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet.Usage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.dataframe.job.DataFrameIndexerJobStats;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class DataFrameFeatureSetUsage extends Usage {

    private final Map<String, Long> jobCountByState;
    private final DataFrameIndexerJobStats accumulatedStats;

    public DataFrameFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.jobCountByState = in.readMap(StreamInput::readString, StreamInput::readLong);
        this.accumulatedStats = new DataFrameIndexerJobStats(in);
    }

    public DataFrameFeatureSetUsage(boolean available, boolean enabled, Map<String, Long> jobCountByState,
            DataFrameIndexerJobStats accumulatedStats) {
        super(XPackField.DATA_FRAME, available, enabled);
        this.jobCountByState = Objects.requireNonNull(jobCountByState);
        this.accumulatedStats = Objects.requireNonNull(accumulatedStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(jobCountByState, StreamOutput::writeString, StreamOutput::writeLong);
        accumulatedStats.writeTo(out);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (jobCountByState.isEmpty() == false) {
            builder.startObject(DataFrameField.JOBS.getPreferredName());
            long all = 0L;
            for (Entry<String, Long> entry : jobCountByState.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
                all+=entry.getValue();
            }
            builder.field(MetaData.ALL, all);
            builder.endObject();

            // if there are no jobs, do not show any stats
            builder.field(DataFrameField.STATS_FIELD.getPreferredName(), accumulatedStats);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, available, jobCountByState, accumulatedStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DataFrameFeatureSetUsage other = (DataFrameFeatureSetUsage) obj;
        return Objects.equals(name, other.name) && available == other.available && enabled == other.enabled
                && Objects.equals(jobCountByState, other.jobCountByState)
                && Objects.equals(accumulatedStats, other.accumulatedStats);
    }
}
