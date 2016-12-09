/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.SchedulerStatus;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PrelertMetadata implements MetaData.Custom {

    private static final ParseField JOBS_FIELD = new ParseField("jobs");
    private static final ParseField ALLOCATIONS_FIELD = new ParseField("allocations");

    public static final String TYPE = "prelert";
    public static final PrelertMetadata PROTO = new PrelertMetadata(Collections.emptySortedMap(), Collections.emptySortedMap(),
            Collections.emptySortedMap());

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
    private final SortedMap<String, SchedulerStatus> schedulerStatuses;

    private PrelertMetadata(SortedMap<String, Job> jobs, SortedMap<String, Allocation> allocations,
                            SortedMap<String, SchedulerStatus> schedulerStatuses) {
        this.jobs = Collections.unmodifiableSortedMap(jobs);
        this.allocations = Collections.unmodifiableSortedMap(allocations);
        this.schedulerStatuses = Collections.unmodifiableSortedMap(schedulerStatuses);
    }

    public Map<String, Job> getJobs() {
        // NORELEASE jobs should be immutable or a job can be modified in the
        // cluster state of a single node without a cluster state update
        return jobs;
    }

    public SortedMap<String, Allocation> getAllocations() {
        return allocations;
    }

    public SortedMap<String, SchedulerStatus> getSchedulerStatuses() {
        return schedulerStatuses;
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
        size = in.readVInt();
        TreeMap<String, SchedulerStatus> schedulerStatuses = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            schedulerStatuses.put(in.readString(), SchedulerStatus.fromStream(in));
        }
        return new PrelertMetadata(jobs, allocations, schedulerStatuses);
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
        out.writeVInt(schedulerStatuses.size());
        for (Map.Entry<String, SchedulerStatus> entry : schedulerStatuses.entrySet()) {
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
        final Diff<Map<String, SchedulerStatusDiff>> schedulerStatuses;

        PrelertMetadataDiff(PrelertMetadata before, PrelertMetadata after) {
            this.jobs = DiffableUtils.diff(before.jobs, after.jobs, DiffableUtils.getStringKeySerializer());
            this.allocations = DiffableUtils.diff(before.allocations, after.allocations, DiffableUtils.getStringKeySerializer());
            this.schedulerStatuses = DiffableUtils.diff(
                    toSchedulerDiff(before.schedulerStatuses),
                    toSchedulerDiff(after.schedulerStatuses),
                    DiffableUtils.getStringKeySerializer());
        }

        PrelertMetadataDiff(StreamInput in) throws IOException {
            jobs = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Job.PROTO);
            allocations = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Allocation.PROTO);
            schedulerStatuses = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), SchedulerStatusDiff.PROTO);
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            TreeMap<String, Job> newJobs = new TreeMap<>(jobs.apply(((PrelertMetadata) part).jobs));
            TreeMap<String, Allocation> newAllocations = new TreeMap<>(allocations.apply(((PrelertMetadata) part).allocations));

            Map<String, SchedulerStatusDiff> newSchedulerStatuses =
                    schedulerStatuses.apply(toSchedulerDiff((((PrelertMetadata) part)).schedulerStatuses));
            return new PrelertMetadata(newJobs, newAllocations, new TreeMap<>(toSchedulerStatusMap(newSchedulerStatuses)));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            jobs.writeTo(out);
            allocations.writeTo(out);
            schedulerStatuses.writeTo(out);
        }

        private static Map<String, SchedulerStatusDiff> toSchedulerDiff(Map<String, SchedulerStatus> from) {
            return from.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, (entry) -> new SchedulerStatusDiff(entry.getValue())));
        }

        private static Map<String, SchedulerStatus> toSchedulerStatusMap(Map<String, SchedulerStatusDiff> from) {
            return from.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, (entry) -> entry.getValue().status));
        }

        // SchedulerStatus is enum and that can't extend from anything
        static class SchedulerStatusDiff extends AbstractDiffable<SchedulerStatusDiff> implements Writeable {

            static SchedulerStatusDiff PROTO = new SchedulerStatusDiff(null);

            private final SchedulerStatus status;

            SchedulerStatusDiff(SchedulerStatus status) {
                this.status = status;
            }

            @Override
            public SchedulerStatusDiff readFrom(StreamInput in) throws IOException {
                return new SchedulerStatusDiff(SchedulerStatus.fromStream(in));
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                status.writeTo(out);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                SchedulerStatusDiff that = (SchedulerStatusDiff) o;
                return status == that.status;
            }

            @Override
            public int hashCode() {
                return Objects.hash(status);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PrelertMetadata that = (PrelertMetadata) o;
        return Objects.equals(jobs, that.jobs) &&
                Objects.equals(allocations, that.allocations) &&
                Objects.equals(schedulerStatuses, that.schedulerStatuses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs, allocations, schedulerStatuses);
    }

    public static class Builder {

        private TreeMap<String, Job> jobs;
        private TreeMap<String, Allocation> allocations;
        private TreeMap<String, SchedulerStatus> schedulerStatuses;

        public Builder() {
            this.jobs = new TreeMap<>();
            this.allocations = new TreeMap<>();
            this.schedulerStatuses = new TreeMap<>();
        }

        public Builder(PrelertMetadata previous) {
            jobs = new TreeMap<>(previous.jobs);
            allocations = new TreeMap<>(previous.allocations);
            schedulerStatuses = new TreeMap<>(previous.schedulerStatuses);
        }

        public Builder putJob(Job job, boolean overwrite) {
            if (jobs.containsKey(job.getId()) && overwrite == false) {
                throw ExceptionsHelper.jobAlreadyExists(job.getId());
            }
            this.jobs.put(job.getId(), job);

            Allocation allocation = allocations.get(job.getId());
            if (allocation == null) {
                Allocation.Builder builder = new Allocation.Builder(job);
                builder.setStatus(JobStatus.CLOSED);
                allocations.put(job.getId(), builder.build());
            }
            if (job.getSchedulerConfig() != null) {
                schedulerStatuses.put(job.getId(), SchedulerStatus.STOPPED);
            }
            return this;
        }

        public Builder removeJob(String jobId) {
            if (jobs.remove(jobId) == null) {
                throw new ResourceNotFoundException("job [" + jobId + "] does not exist");
            }
            Allocation previousAllocation = this.allocations.remove(jobId);
            if (previousAllocation != null) {
                if (!previousAllocation.getStatus().isAnyOf(JobStatus.CLOSED, JobStatus.FAILED)) {
                    throw ExceptionsHelper.conflictStatusException(Messages.getMessage(
                            Messages.JOB_CANNOT_DELETE_WHILE_RUNNING, jobId, previousAllocation.getStatus()));
                }
            }
            SchedulerStatus previousStatus = this.schedulerStatuses.remove(jobId);
            if (previousStatus != null) {
                if (previousStatus != SchedulerStatus.STOPPED) {
                    String message = Messages.getMessage(Messages.JOB_CANNOT_DELETE_WHILE_SCHEDULER_RUNS, jobId);
                    throw ExceptionsHelper.conflictStatusException(message);
                }
            }
            return this;
        }

        public Builder updateAllocation(String jobId, Allocation updated) {
            Allocation previous = this.allocations.put(jobId, updated);
            if (previous == null) {
                throw new IllegalStateException("Expected that job [" + jobId + "] was already allocated");
            }
            if (previous.getStatus() != updated.getStatus() && updated.getStatus() == JobStatus.CLOSED) {
                Job.Builder job = new Job.Builder(this.jobs.get(jobId));
                job.setFinishedTime(new Date());
                this.jobs.put(job.getId(), job.build());
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
            return new PrelertMetadata(jobs, allocations, schedulerStatuses);
        }

        public Builder assignToNode(String jobId, String nodeId) {
            Allocation allocation = allocations.get(jobId);
            if (allocation == null) {
                throw new IllegalStateException("[" + jobId + "] no allocation to assign to node [" + nodeId + "]");
            }
            Allocation.Builder builder = new Allocation.Builder(allocation);
            builder.setNodeId(nodeId);
            allocations.put(jobId, builder.build());
            return this;
        }

        public Builder updateStatus(String jobId, JobStatus jobStatus, @Nullable String reason) {
            Allocation previous = allocations.get(jobId);
            if (previous == null) {
                throw new IllegalStateException("[" + jobId + "] no allocation exist to update the status to [" + jobStatus + "]");
            }
            Allocation.Builder builder = new Allocation.Builder(previous);
            builder.setStatus(jobStatus);
            if (reason != null) {
                builder.setStatusReason(reason);
            }
            if (previous.getStatus() != jobStatus && jobStatus == JobStatus.CLOSED) {
                Job.Builder job = new Job.Builder(this.jobs.get(jobId));
                job.setFinishedTime(new Date());
                this.jobs.put(job.getId(), job.build());
            }
            allocations.put(jobId, builder.build());
            return this;
        }

        public Builder setIgnoreDowntime(String jobId) {
            Allocation allocation = allocations.get(jobId);
            if (allocation == null) {
                throw new IllegalStateException("[" + jobId + "] no allocation to ignore downtime");
            }
            Allocation.Builder builder = new Allocation.Builder(allocation);
            builder.setIgnoreDowntime(true);
            allocations.put(jobId, builder.build());
            return this;
        }

        public Builder updateSchedulerStatus(String jobId, SchedulerStatus newStatus) {
            SchedulerStatus currentStatus = schedulerStatuses.get(jobId);
            if (currentStatus == null) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_SCHEDULER_NO_SUCH_SCHEDULED_JOB, jobId));
            }

            switch (newStatus) {
                case STARTED:
                    if (currentStatus != SchedulerStatus.STOPPED) {
                        String msg = Messages.getMessage(Messages.JOB_SCHEDULER_CANNOT_START, jobId, newStatus);
                        throw ExceptionsHelper.conflictStatusException(msg);
                    }
                    break;
                case STOPPED:
                    if (currentStatus != SchedulerStatus.STARTED) {
                        String msg = Messages.getMessage(Messages.JOB_SCHEDULER_CANNOT_STOP_IN_CURRENT_STATE, jobId, newStatus);
                        throw ExceptionsHelper.conflictStatusException(msg);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("[" + jobId + "] invalid requested job scheduler status [" + newStatus + "]");
            }
            schedulerStatuses.put(jobId, newStatus);
            return this;
        }
    }

}
