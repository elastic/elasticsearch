/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.metadata;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobStatus;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.ml.datafeed.Datafeed;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedStatus;
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
    private static final ParseField DATAFEEDS_FIELD = new ParseField("datafeeds");

    public static final String TYPE = "ml";
    public static final MlMetadata EMPTY_METADATA = new MlMetadata(Collections.emptySortedMap(),
            Collections.emptySortedMap(), Collections.emptySortedMap());

    public static final ObjectParser<Builder, Void> ML_METADATA_PARSER = new ObjectParser<>("ml_metadata",
            Builder::new);

    static {
        ML_METADATA_PARSER.declareObjectArray(Builder::putJobs, (p, c) -> Job.PARSER.apply(p, c).build(), JOBS_FIELD);
        ML_METADATA_PARSER.declareObjectArray(Builder::putAllocations, Allocation.PARSER, ALLOCATIONS_FIELD);
        ML_METADATA_PARSER.declareObjectArray(Builder::putDatafeeds, Datafeed.PARSER, DATAFEEDS_FIELD);
    }

    private final SortedMap<String, Job> jobs;
    private final SortedMap<String, Allocation> allocations;
    private final SortedMap<String, Datafeed> datafeeds;

    private MlMetadata(SortedMap<String, Job> jobs, SortedMap<String, Allocation> allocations,
                            SortedMap<String, Datafeed> datafeeds) {
        this.jobs = Collections.unmodifiableSortedMap(jobs);
        this.allocations = Collections.unmodifiableSortedMap(allocations);
        this.datafeeds = Collections.unmodifiableSortedMap(datafeeds);
    }

    public Map<String, Job> getJobs() {
        return jobs;
    }

    public SortedMap<String, Allocation> getAllocations() {
        return allocations;
    }

    public SortedMap<String, Datafeed> getDatafeeds() {
        return datafeeds;
    }

    public Datafeed getDatafeed(String datafeedId) {
        return datafeeds.get(datafeedId);
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
        TreeMap<String, Datafeed> datafeeds = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            datafeeds.put(in.readString(), new Datafeed(in));
        }
        this.datafeeds = datafeeds;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeMap(jobs, out);
        writeMap(allocations, out);
        writeMap(datafeeds, out);
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
        mapValuesToXContent(DATAFEEDS_FIELD, datafeeds, builder, params);
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

    public static class MlMetadataDiff implements NamedDiff<MetaData.Custom> {

        final Diff<Map<String, Job>> jobs;
        final Diff<Map<String, Allocation>> allocations;
        final Diff<Map<String, Datafeed>> datafeeds;

        MlMetadataDiff(MlMetadata before, MlMetadata after) {
            this.jobs = DiffableUtils.diff(before.jobs, after.jobs, DiffableUtils.getStringKeySerializer());
            this.allocations = DiffableUtils.diff(before.allocations, after.allocations, DiffableUtils.getStringKeySerializer());
            this.datafeeds = DiffableUtils.diff(before.datafeeds, after.datafeeds, DiffableUtils.getStringKeySerializer());
        }

        public MlMetadataDiff(StreamInput in) throws IOException {
            this.jobs =  DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Job::new,
                    MlMetadataDiff::readJobDiffFrom);
            this.allocations =  DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Allocation::new,
                    MlMetadataDiff::readAllocationDiffFrom);
            this.datafeeds =  DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Datafeed::new,
                    MlMetadataDiff::readSchedulerDiffFrom);
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            TreeMap<String, Job> newJobs = new TreeMap<>(jobs.apply(((MlMetadata) part).jobs));
            TreeMap<String, Allocation> newAllocations = new TreeMap<>(allocations.apply(((MlMetadata) part).allocations));
            TreeMap<String, Datafeed> newDatafeeds = new TreeMap<>(datafeeds.apply(((MlMetadata) part).datafeeds));
            return new MlMetadata(newJobs, newAllocations, newDatafeeds);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            jobs.writeTo(out);
            allocations.writeTo(out);
            datafeeds.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        static Diff<Job> readJobDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(Job::new, in);
        }

        static Diff<Allocation> readAllocationDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(Allocation::new, in);
        }

        static Diff<Datafeed> readSchedulerDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(Datafeed::new, in);
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
                Objects.equals(datafeeds, that.datafeeds);
    }

    @Override
    public final String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (Exception e) {
            // So we have a stack trace logged somewhere
            return "{ \"error\" : \"" + org.elasticsearch.ExceptionsHelper.detailedMessage(e) + "\"}";
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs, allocations, datafeeds);
    }

    public static class Builder {

        private TreeMap<String, Job> jobs;
        private TreeMap<String, Allocation> allocations;
        private TreeMap<String, Datafeed> datafeeds;

        public Builder() {
            this.jobs = new TreeMap<>();
            this.allocations = new TreeMap<>();
            this.datafeeds = new TreeMap<>();
        }

        public Builder(MlMetadata previous) {
            jobs = new TreeMap<>(previous.jobs);
            allocations = new TreeMap<>(previous.allocations);
            datafeeds = new TreeMap<>(previous.datafeeds);
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

            Optional<Datafeed> datafeed = getDatafeedByJobId(jobId);
            if (datafeed.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] while datafeed ["
                        + datafeed.get().getId() + "] refers to it");
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

        public Builder putDatafeed(DatafeedConfig datafeedConfig) {
            if (datafeeds.containsKey(datafeedConfig.getId())) {
                throw new ResourceAlreadyExistsException("A datafeed with id [" + datafeedConfig.getId() + "] already exists");
            }
            String jobId = datafeedConfig.getJobId();
            Job job = jobs.get(jobId);
            if (job == null) {
                throw ExceptionsHelper.missingJobException(jobId);
            }
            Optional<Datafeed> existingDatafeed = getDatafeedByJobId(jobId);
            if (existingDatafeed.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("A datafeed [" + existingDatafeed.get().getId()
                        + "] already exists for job [" + jobId + "]");
            }
            DatafeedJobValidator.validate(datafeedConfig, job);

            return putDatafeed(new Datafeed(datafeedConfig, DatafeedStatus.STOPPED));
        }

        private Builder putDatafeed(Datafeed datafeed) {
            datafeeds.put(datafeed.getId(), datafeed);
            return this;
        }

        public Builder removeDatafeed(String datafeedId) {
            Datafeed datafeed = datafeeds.get(datafeedId);
            if (datafeed == null) {
                throw ExceptionsHelper.missingDatafeedException(datafeedId);
            }
            if (datafeed.getStatus() != DatafeedStatus.STOPPED) {
                String msg = Messages.getMessage(Messages.DATAFEED_CANNOT_DELETE_IN_CURRENT_STATE, datafeedId, datafeed.getStatus());
                throw ExceptionsHelper.conflictStatusException(msg);
            }
            datafeeds.remove(datafeedId);
            return this;
        }

        private Optional<Datafeed> getDatafeedByJobId(String jobId) {
            return datafeeds.values().stream().filter(s -> s.getJobId().equals(jobId)).findFirst();
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

        private Builder putDatafeeds(Collection<Datafeed> datafeeds) {
            for (Datafeed datafeed : datafeeds) {
                putDatafeed(datafeed);
            }
            return this;
        }

        public MlMetadata build() {
            return new MlMetadata(jobs, allocations, datafeeds);
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

            // Cannot update the status to DELETING if there are datafeeds attached
            if (jobStatus.equals(JobStatus.DELETING)) {
                Optional<Datafeed> datafeed = getDatafeedByJobId(jobId);
                if (datafeed.isPresent()) {
                    throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] while datafeed ["
                            + datafeed.get().getId() + "] refers to it");
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

        public Builder updateDatafeedStatus(String datafeedId, DatafeedStatus newStatus) {
            Datafeed datafeed = datafeeds.get(datafeedId);
            if (datafeed == null) {
                throw ExceptionsHelper.missingDatafeedException(datafeedId);
            }

            DatafeedStatus currentStatus = datafeed.getStatus();
            switch (newStatus) {
                case STARTED:
                    if (currentStatus != DatafeedStatus.STOPPED) {
                        String msg = Messages.getMessage(Messages.DATAFEED_CANNOT_START, datafeedId, newStatus);
                        throw ExceptionsHelper.conflictStatusException(msg);
                    }
                    break;
                case STOPPED:
                    if (currentStatus != DatafeedStatus.STARTED) {
                        String msg = Messages.getMessage(Messages.DATAFEED_CANNOT_STOP_IN_CURRENT_STATE, datafeedId, newStatus);
                        throw ExceptionsHelper.conflictStatusException(msg);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("[" + datafeedId + "] requested invalid datafeed status [" + newStatus + "]");
            }
            datafeeds.put(datafeedId, new Datafeed(datafeed.getConfig(), newStatus));
            return this;
        }
    }

}
