/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class PrelertMetadata implements MetaData.Custom {

    private static final ParseField JOBS_FIELD = new ParseField("jobs");
    private static final ParseField ALLOCATIONS_FIELD = new ParseField("allocations");

    public static final String TYPE = "prelert";
    public static final PrelertMetadata PROTO = new PrelertMetadata(Collections.emptySortedMap(), Collections.emptySortedMap());

    private static final ObjectParser<Builder, ParseFieldMatcherSupplier> PRELERT_METADATA_PARSER = new ObjectParser<>("prelert_metadata",
            Builder::new);

    static {
        PRELERT_METADATA_PARSER.declareObjectArray(Builder::putJobs, (p, c) -> Job.PARSER.apply(p, c).build(), JOBS_FIELD);
        PRELERT_METADATA_PARSER.declareObjectArray(Builder::putAllocations, Allocation.PARSER, ALLOCATIONS_FIELD);
    }

    // NORELEASE: A few fields of job details change frequently and this needs to be stored elsewhere
    // performance issue will occur if we don't change that
    private final SortedMap<String, Job> jobs;
    private final SortedMap<String, Allocation> allocations;

    private PrelertMetadata(SortedMap<String, Job> jobs, SortedMap<String, Allocation> allocations) {
        this.jobs = Collections.unmodifiableSortedMap(jobs);
        this.allocations = Collections.unmodifiableSortedMap(allocations);
    }

    public Map<String, Job> getJobs() {
        // NORELEASE jobs should be immutable or a job can be modified in the
        // cluster state of a single node without a cluster state update
        return jobs;
    }

    public SortedMap<String, Allocation> getAllocations() {
        return allocations;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public MetaData.Custom fromXContent(XContentParser parser) throws IOException {
        return PRELERT_METADATA_PARSER.parse(parser, () -> ParseFieldMatcher.STRICT).build();
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        // NORELEASE: Also include SNAPSHOT, but then we need to split the allocations from here and add them
        // as ClusterState.Custom metadata, because only the job definitions should be stored in snapshots.
        return MetaData.API_AND_GATEWAY;
    }

    @Override
    public Diff<MetaData.Custom> diff(MetaData.Custom previousState) {
        return new PrelertMetadataDiff((PrelertMetadata) previousState, this);
    }

    @Override
    public Diff<MetaData.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new PrelertMetadataDiff(in);
    }

    @Override
    public MetaData.Custom readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        TreeMap<String, Job> jobs = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            jobs.put(in.readString(), new Job(in));
        }
        size = in.readVInt();
        TreeMap<String, Allocation> allocations = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            allocations.put(in.readString(), Allocation.PROTO.readFrom(in));
        }
        return new PrelertMetadata(jobs, allocations);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(jobs.size());
        for (Map.Entry<String, Job> entry : jobs.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
        out.writeVInt(allocations.size());
        for (Map.Entry<String, Allocation> entry : allocations.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(JOBS_FIELD.getPreferredName());
        for (Job job : jobs.values()) {
            builder.value(job);
        }
        builder.endArray();
        builder.startArray(ALLOCATIONS_FIELD.getPreferredName());
        for (Map.Entry<String, Allocation> entry : allocations.entrySet()) {
            builder.value(entry.getValue());
        }
        builder.endArray();
        return builder;
    }

    static class PrelertMetadataDiff implements Diff<MetaData.Custom> {

        final Diff<Map<String, Job>> jobs;
        final Diff<Map<String, Allocation>> allocations;

        PrelertMetadataDiff(PrelertMetadata before, PrelertMetadata after) {
            this.jobs = DiffableUtils.diff(before.jobs, after.jobs, DiffableUtils.getStringKeySerializer());
            this.allocations = DiffableUtils.diff(before.allocations, after.allocations, DiffableUtils.getStringKeySerializer());
        }

        PrelertMetadataDiff(StreamInput in) throws IOException {
            jobs = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Job.PROTO);
            allocations = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Allocation.PROTO);
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            TreeMap<String, Job> newJobs = new TreeMap<>(jobs.apply(((PrelertMetadata) part).jobs));
            TreeMap<String, Allocation> newAllocations = new TreeMap<>(allocations.apply(((PrelertMetadata) part).allocations));
            return new PrelertMetadata(newJobs, newAllocations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            jobs.writeTo(out);
            allocations.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PrelertMetadata that = (PrelertMetadata) o;
        return Objects.equals(jobs, that.jobs) && Objects.equals(allocations, that.allocations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs, allocations);
    }

    public static class Builder {

        private TreeMap<String, Job> jobs;
        private TreeMap<String, Allocation> allocations;

        public Builder() {
            this.jobs = new TreeMap<>();
            this.allocations = new TreeMap<>();
        }

        public Builder(PrelertMetadata previous) {
            jobs = new TreeMap<>(previous.jobs);
            allocations = new TreeMap<>(previous.allocations);
        }

        public Builder putJob(Job job, boolean overwrite) {
            if (jobs.containsKey(job.getId()) && overwrite == false) {
                throw ExceptionsHelper.jobAlreadyExists(job.getId());
            }
            this.jobs.put(job.getId(), job);
            return this;
        }

        public Builder removeJob(String jobId) {
            if (jobs.remove(jobId) == null) {
                throw new ResourceNotFoundException("job [" + jobId + "] does not exist");
            }
            this.allocations.remove(jobId);
            return this;
        }

        public Builder putAllocation(String nodeId, String jobId) {
            Allocation.Builder builder = new Allocation.Builder();
            builder.setJobId(jobId);
            builder.setNodeId(nodeId);
            this.allocations.put(jobId, builder.build());
            return this;
        }

        public Builder updateAllocation(String jobId, Allocation updated) {
            Allocation previous = this.allocations.put(jobId, updated);
            if (previous == null) {
                throw new IllegalStateException("Expected that job [" + jobId + "] was already allocated");
            }
            return this;
        }

        // only for parsing
        private Builder putAllocations(Collection<Allocation.Builder> allocations) {
            for (Allocation.Builder allocationBuilder : allocations) {
                Allocation allocation = allocationBuilder.build();
                this.allocations.put(allocation.getJobId(), allocation);
            }
            return this;
        }

        private Builder putJobs(Collection<Job> jobs) {
            for (Job job : jobs) {
                putJob(job, true);
            }
            return this;
        }

        public PrelertMetadata build() {
            return new PrelertMetadata(jobs, allocations);
        }
    }

}
