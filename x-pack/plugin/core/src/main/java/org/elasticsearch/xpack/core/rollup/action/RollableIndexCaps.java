/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Represents the rollup capabilities of a non-rollup index.  E.g. what values/aggregations
 * were rolled up for this index, in what rollup jobs that data is stored and where those
 * concrete rollup indices exist
 *
 * The index name can either be a single index, or an index pattern (logstash-*)
 */
public class RollableIndexCaps implements Writeable, ToXContentFragment {
    static ParseField ROLLUP_JOBS = new ParseField("rollup_jobs");

    private String indexName;
    private List<RollupJobCaps> jobCaps;

    public RollableIndexCaps(String indexName) {
        this.indexName = indexName;
        this.jobCaps = new ArrayList<>();
    }

    public RollableIndexCaps(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.jobCaps = in.readList(RollupJobCaps::new);
    }

    public void addJobCap(RollupJobCaps jobCap) {
        jobCaps.add(jobCap);
    }

    public String getIndexName() {
        return indexName;
    }

    public List<RollupJobCaps> getJobCaps() {
        return jobCaps;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeList(jobCaps);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(indexName);
        jobCaps.sort(Comparator.comparing(RollupJobCaps::getJobID));
        builder.field(ROLLUP_JOBS.getPreferredName(), jobCaps);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        RollableIndexCaps that = (RollableIndexCaps) other;

        return Objects.equals(this.jobCaps, that.jobCaps)
                && Objects.equals(this.indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobCaps, indexName);
    }
}
