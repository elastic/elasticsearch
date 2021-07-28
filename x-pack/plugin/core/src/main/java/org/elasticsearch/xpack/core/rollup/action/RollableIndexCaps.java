/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the rollup capabilities of a non-rollup index.  E.g. what values/aggregations
 * were rolled up for this index, in what rollup jobs that data is stored and where those
 * concrete rollup indices exist
 *
 * The index name can either be a single index, or an index pattern (logstash-*)
 */
public class RollableIndexCaps implements Writeable, ToXContentObject {
    private static final ParseField ROLLUP_JOBS = new ParseField("rollup_jobs");

    private final String indexName;
    private final List<RollupJobCaps> jobCaps;

    public RollableIndexCaps(String indexName, List<RollupJobCaps> caps) {
        this.indexName = indexName;
        this.jobCaps = Collections.unmodifiableList(Objects.requireNonNull(caps)
            .stream()
            .sorted(Comparator.comparing(RollupJobCaps::getJobID))
            .collect(Collectors.toList()));
    }

    public RollableIndexCaps(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.jobCaps = in.readList(RollupJobCaps::new);
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
        {
            builder.field(ROLLUP_JOBS.getPreferredName(), jobCaps);
        }
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
