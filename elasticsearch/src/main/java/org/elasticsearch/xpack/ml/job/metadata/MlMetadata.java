/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.metadata;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.JobStatus;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.scheduler.ScheduledJobValidator;
import org.elasticsearch.xpack.ml.scheduler.Scheduler;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.scheduler.SchedulerStatus;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

public class MlMetadata implements MetaData.Custom {

    private static final ParseField JOBS_FIELD = new ParseField("jobs");
    private static final ParseField ALLOCATIONS_FIELD = new ParseField("allocations");
    private static final ParseField SCHEDULERS_FIELD = new ParseField("schedulers");

    public static final String TYPE = "ml";
    public static final MlMetadata EMPTY_METADATA = new MlMetadata(Collections.emptySortedMap(),
            Collections.emptySortedMap(), Collections.emptySortedMap());

    public static final ObjectParser<Builder, Void> ML_METADATA_PARSER = new ObjectParser<>("ml_metadata",
            Builder::new);

    static {
        ML_METADATA_PARSER.declareObjectArray(Builder::putJobs, (p, c) -> Job.PARSER.apply(p, c).build(), JOBS_FIELD);
        ML_METADATA_PARSER.declareObjectArray(Builder::putAllocations, Allocation.PARSER, ALLOCATIONS_FIELD);
        ML_METADATA_PARSER.declareObjectArray(Builder::putSchedulers, Scheduler.PARSER, SCHEDULERS_FIELD);
    }

    private final SortedMap<String, Job> jobs;
    private final SortedMap<String, Allocation> allocations;
    private final SortedMap<String, Scheduler> schedulers;

    private MlMetadata(SortedMap<String, Job> jobs, SortedMap<String, Allocation> allocations,
                            SortedMap<String, Scheduler> schedulers) {
        this.jobs = Collections.unmodifiableSortedMap(jobs);
        this.allocations = Collections.unmodifiableSortedMap(allocations);
        this.schedulers = Collections.unmodifiableSortedMap(schedulers);
    }

    public Map<String, Job> getJobs() {
        return jobs;
    }

    public SortedMap<String, Allocation> getAllocations() {
        return allocations;
    }

    public SortedMap<String, Scheduler> getSchedulers() {
        return schedulers;
    }

    public Scheduler getScheduler(String schedulerId) {
        return schedulers.get(schedulerId);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        // NORELEASE: Also include SNAPSHOT, but then we need to split the allocations from here and add them
        // as ClusterState.Custom metadata, because only the job definitions should be stored in snapshots.
        return MetaData.API_AND_GATEWAY;
    }

    @Override
    public Diff<MetaData.Custom> diff(MetaData.Custom previousState) {
        return new MlMetadataDiff((MlMetadata) previousState, this);
    }

    public MlMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        TreeMap<String, Job> jobs = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            jobs.put(in.readString(), new Job(in));
        }
        this.jobs = jobs;
        size = in.readVInt();
        TreeMap<String, Allocation> allocations = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            allocations.put(in.readString(), new Allocation(in));
        }
        this.allocations = allocations;
        size = in.readVInt();
        TreeMap<String, Scheduler> schedulers = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            schedulers.put(in.readString(), new Scheduler(in));
        }
        this.schedulers = schedulers;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeMap(jobs, out);
        writeMap(allocations, out);
        writeMap(schedulers, out);
    }

    private static <T extends Writeable> void writeMap(Map<String, T> map, StreamOutput out) throws IOException {
        out.writeVInt(map.size());
        for (Map.Entry<String, T> entry : map.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        mapValuesToXContent(JOBS_FIELD, jobs, builder, params);
        mapValuesToXContent(ALLOCATIONS_FIELD, allocations, builder, params);
        mapValuesToXContent(SCHEDULERS_FIELD, schedulers, builder, params);
        return builder;
    }

    private static <T extends ToXContent> void mapValuesToXContent(ParseField field, Map<String, T> map, XContentBuilder builder,
                                                                   Params params) throws IOException {
        builder.startArray(field.getPreferredName());
        for (Map.Entry<String, T> entry : map.entrySet()) {
            entry.getValue().toXContent(builder, params);
        }
        builder.endArray();
    }

    static class MlMetadataDiff implements Diff<MetaData.Custom> {

        final Diff<Map<String, Job>> jobs;
        final Diff<Map<String, Allocation>> allocations;
        final Diff<Map<String, Scheduler>> schedulers;

        MlMetadataDiff(MlMetadata before, MlMetadata after) {
            this.jobs = DiffableUtils.diff(before.jobs, after.jobs, DiffableUtils.getStringKeySerializer());
            this.allocations = DiffableUtils.diff(before.allocations, after.allocations, DiffableUtils.getStringKeySerializer());
            this.schedulers = DiffableUtils.diff(before.schedulers, after.schedulers, DiffableUtils.getStringKeySerializer());
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            TreeMap<String, Job> newJobs = new TreeMap<>(jobs.apply(((MlMetadata) part).jobs));
            TreeMap<String, Allocation> newAllocations = new TreeMap<>(allocations.apply(((MlMetadata) part).allocations));
            TreeMap<String, Scheduler> newSchedulers = new TreeMap<>(schedulers.apply(((MlMetadata) part).schedulers));
            return new MlMetadata(newJobs, newAllocations, newSchedulers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            jobs.writeTo(out);
            allocations.writeTo(out);
            schedulers.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MlMetadata that = (MlMetadata) o;
        return Objects.equals(jobs, that.jobs) &&
                Objects.equals(allocations, that.allocations) &&
                Objects.equals(schedulers, that.schedulers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs, allocations, schedulers);
    }

    public static class Builder {

        private TreeMap<String, Job> jobs;
        private TreeMap<String, Allocation> allocations;
        private TreeMap<String, Scheduler> schedulers;

        public Builder() {
            this.jobs = new TreeMap<>();
            this.allocations = new TreeMap<>();
            this.schedulers = new TreeMap<>();
        }

        public Builder(MlMetadata previous) {
            jobs = new TreeMap<>(previous.jobs);
            allocations = new TreeMap<>(previous.allocations);
            schedulers = new TreeMap<>(previous.schedulers);
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
            return this;
        }

        public Builder deleteJob(String jobId) {

            Job job = jobs.remove(jobId);
            if (job == null) {
                throw new ResourceNotFoundException("job [" + jobId + "] does not exist");
            }

            Optional<Scheduler> scheduler = getSchedulerByJobId(jobId);
            if (scheduler.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] while scheduler ["
                        + scheduler.get().getId() + "] refers to it");
            }

            Allocation previousAllocation = this.allocations.remove(jobId);
            if (previousAllocation != null) {
                if (!previousAllocation.getStatus().equals(JobStatus.DELETING)) {
                    throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] because it is in ["
                            + previousAllocation.getStatus() + "] state. Must be in [" + JobStatus.DELETING + "] state.");
                }
            } else {
                throw new ResourceNotFoundException("No Cluster State found for job [" + jobId + "]");
            }

            return this;
        }

        public Builder putScheduler(SchedulerConfig schedulerConfig) {
            if (schedulers.containsKey(schedulerConfig.getId())) {
                throw new ResourceAlreadyExistsException("A scheduler with id [" + schedulerConfig.getId() + "] already exists");
            }
            String jobId = schedulerConfig.getJobId();
            Job job = jobs.get(jobId);
            if (job == null) {
                throw ExceptionsHelper.missingJobException(jobId);
            }
            Optional<Scheduler> existingScheduler = getSchedulerByJobId(jobId);
            if (existingScheduler.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("A scheduler [" + existingScheduler.get().getId()
                        + "] already exists for job [" + jobId + "]");
            }
            ScheduledJobValidator.validate(schedulerConfig, job);

            return putScheduler(new Scheduler(schedulerConfig, SchedulerStatus.STOPPED));
        }

        private Builder putScheduler(Scheduler scheduler) {
            schedulers.put(scheduler.getId(), scheduler);
            return this;
        }

        public Builder removeScheduler(String schedulerId) {
            Scheduler scheduler = schedulers.get(schedulerId);
            if (scheduler == null) {
                throw ExceptionsHelper.missingSchedulerException(schedulerId);
            }
            if (scheduler.getStatus() != SchedulerStatus.STOPPED) {
                String msg = Messages.getMessage(Messages.SCHEDULER_CANNOT_DELETE_IN_CURRENT_STATE, schedulerId, scheduler.getStatus());
                throw ExceptionsHelper.conflictStatusException(msg);
            }
            schedulers.remove(schedulerId);
            return this;
        }

        private Optional<Scheduler> getSchedulerByJobId(String jobId) {
            return schedulers.values().stream().filter(s -> s.getJobId().equals(jobId)).findFirst();
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

        private Builder putSchedulers(Collection<Scheduler> schedulers) {
            for (Scheduler scheduler : schedulers) {
                putScheduler(scheduler);
            }
            return this;
        }

        public MlMetadata build() {
            return new MlMetadata(jobs, allocations, schedulers);
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
            if (jobs.containsKey(jobId) == false) {
                throw ExceptionsHelper.missingJobException(jobId);
            }

            Allocation previous = allocations.get(jobId);
            if (previous == null) {
                throw new IllegalStateException("[" + jobId + "] no allocation exist to update the status to [" + jobStatus + "]");
            }

            // Cannot update the status to DELETING if there are schedulers attached
            if (jobStatus.equals(JobStatus.DELETING)) {
                Optional<Scheduler> scheduler = getSchedulerByJobId(jobId);
                if (scheduler.isPresent()) {
                    throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] while scheduler ["
                            + scheduler.get().getId() + "] refers to it");
                }
            }

            if (previous.getStatus().equals(JobStatus.DELETING)) {
                // If we're already Deleting there's nothing to do
                if (jobStatus.equals(JobStatus.DELETING)) {
                    return this;
                }

                // Once a job goes into Deleting, it cannot be changed
                throw new ElasticsearchStatusException("Cannot change status of job [" + jobId + "] to [" + jobStatus + "] because " +
                        "it is currently in [" + JobStatus.DELETING + "] status.", RestStatus.CONFLICT);
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
            if (jobs.containsKey(jobId) == false) {
                throw ExceptionsHelper.missingJobException(jobId);
            }

            Allocation allocation = allocations.get(jobId);
            if (allocation == null) {
                throw new IllegalStateException("[" + jobId + "] no allocation to ignore downtime");
            }
            Allocation.Builder builder = new Allocation.Builder(allocation);
            builder.setIgnoreDowntime(true);
            allocations.put(jobId, builder.build());
            return this;
        }

        public Builder updateSchedulerStatus(String schedulerId, SchedulerStatus newStatus) {
            Scheduler scheduler = schedulers.get(schedulerId);
            if (scheduler == null) {
                throw ExceptionsHelper.missingSchedulerException(schedulerId);
            }

            SchedulerStatus currentStatus = scheduler.getStatus();
            switch (newStatus) {
                case STARTED:
                    if (currentStatus != SchedulerStatus.STOPPED) {
                        String msg = Messages.getMessage(Messages.SCHEDULER_CANNOT_START, schedulerId, newStatus);
                        throw ExceptionsHelper.conflictStatusException(msg);
                    }
                    break;
                case STOPPED:
                    if (currentStatus != SchedulerStatus.STARTED) {
                        String msg = Messages.getMessage(Messages.SCHEDULER_CANNOT_STOP_IN_CURRENT_STATE, schedulerId, newStatus);
                        throw ExceptionsHelper.conflictStatusException(msg);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("[" + schedulerId + "] requested invalid scheduler status [" + newStatus + "]");
            }
            schedulers.put(schedulerId, new Scheduler(scheduler.getConfig(), newStatus));
            return this;
        }
    }

}