/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.metadata;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class MlMetadata implements MetaData.Custom {

    private static final ParseField JOBS_FIELD = new ParseField("jobs");
    private static final ParseField DATAFEEDS_FIELD = new ParseField("datafeeds");

    public static final String TYPE = "ml";
    public static final MlMetadata EMPTY_METADATA = new MlMetadata(Collections.emptySortedMap(), Collections.emptySortedMap());
    public static final ObjectParser<Builder, Void> ML_METADATA_PARSER = new ObjectParser<>("ml_metadata", Builder::new);

    static {
        ML_METADATA_PARSER.declareObjectArray(Builder::putJobs, (p, c) -> Job.PARSER.apply(p, c).build(), JOBS_FIELD);
        ML_METADATA_PARSER.declareObjectArray(Builder::putDatafeeds, (p, c) -> DatafeedConfig.PARSER.apply(p, c).build(), DATAFEEDS_FIELD);
    }

    private final SortedMap<String, Job> jobs;
    private final SortedMap<String, DatafeedConfig> datafeeds;

    private MlMetadata(SortedMap<String, Job> jobs, SortedMap<String, DatafeedConfig> datafeeds) {
        this.jobs = Collections.unmodifiableSortedMap(jobs);
        this.datafeeds = Collections.unmodifiableSortedMap(datafeeds);
    }

    public Map<String, Job> getJobs() {
        return jobs;
    }

    public SortedMap<String, DatafeedConfig> getDatafeeds() {
        return datafeeds;
    }

    public DatafeedConfig getDatafeed(String datafeedId) {
        return datafeeds.get(datafeedId);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_5_4_0_UNRELEASED;
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
        TreeMap<String, DatafeedConfig> datafeeds = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            datafeeds.put(in.readString(), new DatafeedConfig(in));
        }
        this.datafeeds = datafeeds;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeMap(jobs, out);
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
        final Diff<Map<String, DatafeedConfig>> datafeeds;

        MlMetadataDiff(MlMetadata before, MlMetadata after) {
            this.jobs = DiffableUtils.diff(before.jobs, after.jobs, DiffableUtils.getStringKeySerializer());
            this.datafeeds = DiffableUtils.diff(before.datafeeds, after.datafeeds, DiffableUtils.getStringKeySerializer());
        }

        public MlMetadataDiff(StreamInput in) throws IOException {
            this.jobs =  DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Job::new,
                    MlMetadataDiff::readJobDiffFrom);
            this.datafeeds =  DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), DatafeedConfig::new,
                    MlMetadataDiff::readSchedulerDiffFrom);
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            TreeMap<String, Job> newJobs = new TreeMap<>(jobs.apply(((MlMetadata) part).jobs));
            TreeMap<String, DatafeedConfig> newDatafeeds = new TreeMap<>(datafeeds.apply(((MlMetadata) part).datafeeds));
            return new MlMetadata(newJobs, newDatafeeds);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            jobs.writeTo(out);
            datafeeds.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        static Diff<Job> readJobDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(Job::new, in);
        }

        static Diff<DatafeedConfig> readSchedulerDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(DatafeedConfig::new, in);
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
                Objects.equals(datafeeds, that.datafeeds);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs, datafeeds);
    }

    public static class Builder {

        private TreeMap<String, Job> jobs;
        private TreeMap<String, DatafeedConfig> datafeeds;

        public Builder() {
            this.jobs = new TreeMap<>();
            this.datafeeds = new TreeMap<>();
        }

        public Builder(MlMetadata previous) {
            jobs = new TreeMap<>(previous.jobs);
            datafeeds = new TreeMap<>(previous.datafeeds);
        }

        public Builder putJob(Job job, boolean overwrite) {
            if (jobs.containsKey(job.getId()) && overwrite == false) {
                throw ExceptionsHelper.jobAlreadyExists(job.getId());
            }
            this.jobs.put(job.getId(), job);
            return this;
        }

        public Builder deleteJob(String jobId, PersistentTasksInProgress tasks) {
            Optional<DatafeedConfig> datafeed = getDatafeedByJobId(jobId);
            if (datafeed.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] while datafeed ["
                        + datafeed.get().getId() + "] refers to it");
            }
            JobState jobState = MlMetadata.getJobState(jobId, tasks);
            if (jobState.isAnyOf(JobState.CLOSED, JobState.FAILED) == false) {
                throw ExceptionsHelper.conflictStatusException("Unexpected job state [" + jobState + "], expected [" +
                        JobState.CLOSED + "]");
            }
            Job job = jobs.remove(jobId);
            if (job == null) {
                throw new ResourceNotFoundException("job [" + jobId + "] does not exist");
            }
            if (job.isDeleted() == false) {
                throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] because it hasn't marked as deleted");
            }
            return this;
        }

        public Builder putDatafeed(DatafeedConfig datafeedConfig) {
            if (datafeeds.containsKey(datafeedConfig.getId())) {
                throw new ResourceAlreadyExistsException("A datafeed with id [" + datafeedConfig.getId() + "] already exists");
            }
            String jobId = datafeedConfig.getJobId();
            checkJobIsAvailableForDatafeed(jobId);
            Job job = jobs.get(jobId);
            DatafeedJobValidator.validate(datafeedConfig, job);
            datafeeds.put(datafeedConfig.getId(), datafeedConfig);
            return this;
        }

        private void checkJobIsAvailableForDatafeed(String jobId) {
            Job job = jobs.get(jobId);
            if (job == null) {
                throw ExceptionsHelper.missingJobException(jobId);
            }
            Optional<DatafeedConfig> existingDatafeed = getDatafeedByJobId(jobId);
            if (existingDatafeed.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("A datafeed [" + existingDatafeed.get().getId()
                        + "] already exists for job [" + jobId + "]");
            }
        }

        public Builder updateDatafeed(DatafeedUpdate update, PersistentTasksInProgress persistentTasksInProgress) {
            String datafeedId = update.getId();
            DatafeedConfig oldDatafeedConfig = datafeeds.get(datafeedId);
            if (oldDatafeedConfig == null) {
                throw ExceptionsHelper.missingDatafeedException(datafeedId);
            }
            checkDatafeedIsStopped(() -> Messages.getMessage(Messages.DATAFEED_CANNOT_UPDATE_IN_CURRENT_STATE, datafeedId,
                    DatafeedState.STARTED), datafeedId, persistentTasksInProgress);
            DatafeedConfig newDatafeedConfig = update.apply(oldDatafeedConfig);
            if (newDatafeedConfig.getJobId().equals(oldDatafeedConfig.getJobId()) == false) {
                checkJobIsAvailableForDatafeed(newDatafeedConfig.getJobId());
            }
            Job job = jobs.get(newDatafeedConfig.getJobId());
            DatafeedJobValidator.validate(newDatafeedConfig, job);
            datafeeds.put(datafeedId, newDatafeedConfig);
            return this;
        }

        public Builder removeDatafeed(String datafeedId, PersistentTasksInProgress persistentTasksInProgress) {
            DatafeedConfig datafeed = datafeeds.get(datafeedId);
            if (datafeed == null) {
                throw ExceptionsHelper.missingDatafeedException(datafeedId);
            }
            checkDatafeedIsStopped(() -> Messages.getMessage(Messages.DATAFEED_CANNOT_DELETE_IN_CURRENT_STATE, datafeedId,
                    DatafeedState.STARTED), datafeedId, persistentTasksInProgress);
            datafeeds.remove(datafeedId);
            return this;
        }

        private Optional<DatafeedConfig> getDatafeedByJobId(String jobId) {
            return datafeeds.values().stream().filter(s -> s.getJobId().equals(jobId)).findFirst();
        }

        private void checkDatafeedIsStopped(Supplier<String> msg, String datafeedId, PersistentTasksInProgress persistentTasksInProgress) {
            if (persistentTasksInProgress != null) {
                Predicate<PersistentTaskInProgress<?>> predicate = t -> {
                    StartDatafeedAction.Request storedRequest = (StartDatafeedAction.Request) t.getRequest();
                    return storedRequest.getDatafeedId().equals(datafeedId);
                };
                if (persistentTasksInProgress.tasksExist(StartDatafeedAction.NAME, predicate)) {
                    throw ExceptionsHelper.conflictStatusException(msg.get());
                }
            }
        }

        private Builder putJobs(Collection<Job> jobs) {
            for (Job job : jobs) {
                putJob(job, true);
            }
            return this;
        }

        private Builder putDatafeeds(Collection<DatafeedConfig> datafeeds) {
            for (DatafeedConfig datafeed : datafeeds) {
                this.datafeeds.put(datafeed.getId(), datafeed);
            }
            return this;
        }

        public MlMetadata build() {
            return new MlMetadata(jobs, datafeeds);
        }

        public void markJobAsDeleted(String jobId, PersistentTasksInProgress tasks) {
            Job job = jobs.get(jobId);
            if (job == null) {
                throw ExceptionsHelper.missingJobException(jobId);
            }
            if (job.isDeleted()) {
                // Job still exists
                return;
            }
            Optional<DatafeedConfig> datafeed = getDatafeedByJobId(jobId);
            if (datafeed.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] while datafeed ["
                        + datafeed.get().getId() + "] refers to it");
            }
            JobState jobState = getJobState(jobId, tasks);
            if (jobState.isAnyOf(JobState.CLOSED, JobState.FAILED) == false) {
                throw ExceptionsHelper.conflictStatusException("Unexpected job state [" + jobState + "], expected [" +
                        JobState.CLOSED + "]");
            }
            Job.Builder jobBuilder = new Job.Builder(job);
            jobBuilder.setDeleted(true);
            putJob(jobBuilder.build(), true);
        }

    }

    @Nullable
    public static PersistentTasksInProgress.PersistentTaskInProgress<?> getJobTask(String jobId,
                                                                                   @Nullable PersistentTasksInProgress tasks) {
        if (tasks != null) {
            Predicate<PersistentTasksInProgress.PersistentTaskInProgress<?>> p = t -> {
                OpenJobAction.Request storedRequest = (OpenJobAction.Request) t.getRequest();
                return storedRequest.getJobId().equals(jobId);
            };
            for (PersistentTasksInProgress.PersistentTaskInProgress<?> task : tasks.findTasks(OpenJobAction.NAME, p)) {
                return task;
            }
        }
        return null;
    }

    @Nullable
    public static PersistentTasksInProgress.PersistentTaskInProgress<?> getDatafeedTask(String datafeedId,
                                                                                        @Nullable PersistentTasksInProgress tasks) {
        if (tasks != null) {
            Predicate<PersistentTasksInProgress.PersistentTaskInProgress<?>> p = t -> {
                StartDatafeedAction.Request storedRequest = (StartDatafeedAction.Request) t.getRequest();
                return storedRequest.getDatafeedId().equals(datafeedId);
            };
            for (PersistentTasksInProgress.PersistentTaskInProgress<?> task : tasks.findTasks(StartDatafeedAction.NAME, p)) {
                return task;
            }
        }
        return null;
    }

    public static JobState getJobState(String jobId, @Nullable PersistentTasksInProgress tasks) {
        PersistentTasksInProgress.PersistentTaskInProgress<?> task = getJobTask(jobId, tasks);
        if (task != null && task.getStatus() != null) {
            return (JobState) task.getStatus();
        } else {
            // If we haven't opened a job than there will be no persistent task, which is the same as if the job was closed
            return JobState.CLOSED;
        }
    }

    public static DatafeedState getDatafeedState(String datafeedId, @Nullable PersistentTasksInProgress tasks) {
        PersistentTasksInProgress.PersistentTaskInProgress<?> task = getDatafeedTask(datafeedId, tasks);
        if (task != null && task.getStatus() != null) {
            return (DatafeedState) task.getStatus();
        } else {
            // If we haven't started a datafeed then there will be no persistent task,
            // which is the same as if the datafeed was't started
            return DatafeedState.STOPPED;
        }
    }

}
