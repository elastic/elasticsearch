/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskStatus;
import org.elasticsearch.xpack.core.ml.job.groups.GroupOrJobLookup;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NameResolver;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MlMetadata implements XPackPlugin.XPackMetaDataCustom {

    private static final ParseField JOBS_FIELD = new ParseField("jobs");
    private static final ParseField DATAFEEDS_FIELD = new ParseField("datafeeds");

    public static final MlMetadata EMPTY_METADATA = new MlMetadata(Collections.emptySortedMap(), Collections.emptySortedMap());
    // This parser follows the pattern that metadata is parsed leniently (to allow for enhancements)
    public static final ObjectParser<Builder, Void> METADATA_PARSER = new ObjectParser<>("ml_metadata", true, Builder::new);

    static {
        METADATA_PARSER.declareObjectArray(Builder::putJobs, (p, c) -> Job.METADATA_PARSER.apply(p, c).build(), JOBS_FIELD);
        METADATA_PARSER.declareObjectArray(Builder::putDatafeeds,
                (p, c) -> DatafeedConfig.METADATA_PARSER.apply(p, c).build(), DATAFEEDS_FIELD);
    }

    private final SortedMap<String, Job> jobs;
    private final SortedMap<String, DatafeedConfig> datafeeds;
    private final GroupOrJobLookup groupOrJobLookup;

    private MlMetadata(SortedMap<String, Job> jobs, SortedMap<String, DatafeedConfig> datafeeds) {
        this.jobs = Collections.unmodifiableSortedMap(jobs);
        this.datafeeds = Collections.unmodifiableSortedMap(datafeeds);
        this.groupOrJobLookup = new GroupOrJobLookup(jobs.values());
    }

    public Map<String, Job> getJobs() {
        return jobs;
    }

    public boolean isGroupOrJob(String id) {
        return groupOrJobLookup.isGroupOrJob(id);
    }

    public Set<String> expandJobIds(String expression, boolean allowNoJobs) {
        return groupOrJobLookup.expandJobIds(expression, allowNoJobs);
    }

    public boolean isJobDeleted(String jobId) {
        Job job = jobs.get(jobId);
        return job == null || job.isDeleted();
    }

    public SortedMap<String, DatafeedConfig> getDatafeeds() {
        return datafeeds;
    }

    public DatafeedConfig getDatafeed(String datafeedId) {
        return datafeeds.get(datafeedId);
    }

    public Optional<DatafeedConfig> getDatafeedByJobId(String jobId) {
        return datafeeds.values().stream().filter(s -> s.getJobId().equals(jobId)).findFirst();
    }

    public Set<String> expandDatafeedIds(String expression, boolean allowNoDatafeeds) {
        return NameResolver.newUnaliased(datafeeds.keySet(), ExceptionsHelper::missingDatafeedException)
                .expand(expression, allowNoDatafeeds);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_5_4_0;
    }

    @Override
    public String getWriteableName() {
        return MLMetadataField.TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.ALL_CONTEXTS;
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

        this.groupOrJobLookup = new GroupOrJobLookup(jobs.values());
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
        DelegatingMapParams extendedParams =
                new DelegatingMapParams(Collections.singletonMap(ToXContentParams.FOR_CLUSTER_STATE, "true"), params);
        mapValuesToXContent(JOBS_FIELD, jobs, builder, extendedParams);
        mapValuesToXContent(DATAFEEDS_FIELD, datafeeds, builder, extendedParams);
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
            this.jobs = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Job::new,
                    MlMetadataDiff::readJobDiffFrom);
            this.datafeeds = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), DatafeedConfig::new,
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
            return MLMetadataField.TYPE;
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
            jobs = new TreeMap<>();
            datafeeds = new TreeMap<>();
        }

        public Builder(@Nullable MlMetadata previous) {
            if (previous == null) {
                jobs = new TreeMap<>();
                datafeeds = new TreeMap<>();
            } else {
                jobs = new TreeMap<>(previous.jobs);
                datafeeds = new TreeMap<>(previous.datafeeds);
            }
        }

        public Builder putJob(Job job, boolean overwrite) {
            if (jobs.containsKey(job.getId()) && overwrite == false) {
                throw ExceptionsHelper.jobAlreadyExists(job.getId());
            }
            this.jobs.put(job.getId(), job);
            return this;
        }

        public Builder deleteJob(String jobId, PersistentTasksCustomMetaData tasks) {
            checkJobHasNoDatafeed(jobId);

            JobState jobState = MlMetadata.getJobState(jobId, tasks);
            if (jobState.isAnyOf(JobState.CLOSED, JobState.FAILED) == false) {
                throw ExceptionsHelper.conflictStatusException("Unexpected job state [" + jobState + "], expected [" +
                        JobState.CLOSED + " or " + JobState.FAILED + "]");
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

        public Builder putDatafeed(DatafeedConfig datafeedConfig, ThreadContext threadContext) {
            if (datafeeds.containsKey(datafeedConfig.getId())) {
                throw new ResourceAlreadyExistsException("A datafeed with id [" + datafeedConfig.getId() + "] already exists");
            }
            String jobId = datafeedConfig.getJobId();
            checkJobIsAvailableForDatafeed(jobId);
            Job job = jobs.get(jobId);
            DatafeedJobValidator.validate(datafeedConfig, job);

            if (threadContext != null) {
                // Adjust the request, adding security headers from the current thread context
                DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedConfig);
                Map<String, String> headers = threadContext.getHeaders().entrySet().stream()
                        .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                builder.setHeaders(headers);
                datafeedConfig = builder.build();
            }

            datafeeds.put(datafeedConfig.getId(), datafeedConfig);
            return this;
        }

        private void checkJobIsAvailableForDatafeed(String jobId) {
            Job job = jobs.get(jobId);
            if (job == null || job.isDeleted()) {
                throw ExceptionsHelper.missingJobException(jobId);
            }
            Optional<DatafeedConfig> existingDatafeed = getDatafeedByJobId(jobId);
            if (existingDatafeed.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("A datafeed [" + existingDatafeed.get().getId()
                        + "] already exists for job [" + jobId + "]");
            }
        }

        public Builder updateDatafeed(DatafeedUpdate update, PersistentTasksCustomMetaData persistentTasks, ThreadContext threadContext) {
            String datafeedId = update.getId();
            DatafeedConfig oldDatafeedConfig = datafeeds.get(datafeedId);
            if (oldDatafeedConfig == null) {
                throw ExceptionsHelper.missingDatafeedException(datafeedId);
            }
            checkDatafeedIsStopped(() -> Messages.getMessage(Messages.DATAFEED_CANNOT_UPDATE_IN_CURRENT_STATE, datafeedId,
                    DatafeedState.STARTED), datafeedId, persistentTasks);
            DatafeedConfig newDatafeedConfig = update.apply(oldDatafeedConfig, threadContext);
            if (newDatafeedConfig.getJobId().equals(oldDatafeedConfig.getJobId()) == false) {
                checkJobIsAvailableForDatafeed(newDatafeedConfig.getJobId());
            }
            Job job = jobs.get(newDatafeedConfig.getJobId());
            DatafeedJobValidator.validate(newDatafeedConfig, job);
            datafeeds.put(datafeedId, newDatafeedConfig);
            return this;
        }

        public Builder removeDatafeed(String datafeedId, PersistentTasksCustomMetaData persistentTasks) {
            DatafeedConfig datafeed = datafeeds.get(datafeedId);
            if (datafeed == null) {
                throw ExceptionsHelper.missingDatafeedException(datafeedId);
            }
            checkDatafeedIsStopped(() -> Messages.getMessage(Messages.DATAFEED_CANNOT_DELETE_IN_CURRENT_STATE, datafeedId,
                    DatafeedState.STARTED), datafeedId, persistentTasks);
            datafeeds.remove(datafeedId);
            return this;
        }

        private Optional<DatafeedConfig> getDatafeedByJobId(String jobId) {
            return datafeeds.values().stream().filter(s -> s.getJobId().equals(jobId)).findFirst();
        }

        private void checkDatafeedIsStopped(Supplier<String> msg, String datafeedId, PersistentTasksCustomMetaData persistentTasks) {
            if (persistentTasks != null) {
                if (persistentTasks.getTask(MLMetadataField.datafeedTaskId(datafeedId)) != null) {
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

        public void markJobAsDeleted(String jobId, PersistentTasksCustomMetaData tasks, boolean allowDeleteOpenJob) {
            Job job = jobs.get(jobId);
            if (job == null) {
                throw ExceptionsHelper.missingJobException(jobId);
            }
            if (job.isDeleted()) {
                // Job still exists but is already being deleted
                throw new JobAlreadyMarkedAsDeletedException();
            }

            checkJobHasNoDatafeed(jobId);

            if (allowDeleteOpenJob == false) {
                PersistentTask<?> jobTask = getJobTask(jobId, tasks);
                if (jobTask != null) {
                    JobTaskStatus jobTaskStatus = (JobTaskStatus) jobTask.getStatus();
                    throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] because the job is "
                            + ((jobTaskStatus == null) ? JobState.OPENING : jobTaskStatus.getState()));
                }
            }
            Job.Builder jobBuilder = new Job.Builder(job);
            jobBuilder.setDeleted(true);
            putJob(jobBuilder.build(), true);
        }

        void checkJobHasNoDatafeed(String jobId) {
            Optional<DatafeedConfig> datafeed = getDatafeedByJobId(jobId);
            if (datafeed.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] because datafeed ["
                        + datafeed.get().getId() + "] refers to it");
            }
        }
    }

    /**
     * Namespaces the task ids for jobs.
     * A datafeed id can be used as a job id, because they are stored separately in cluster state.
     */
    public static String jobTaskId(String jobId) {
        return "job-" + jobId;
    }

    @Nullable
    public static PersistentTask<?> getJobTask(String jobId, @Nullable PersistentTasksCustomMetaData tasks) {
        if (tasks == null) {
            return null;
        }
        return tasks.getTask(jobTaskId(jobId));
    }

    @Nullable
    public static PersistentTask<?> getDatafeedTask(String datafeedId, @Nullable PersistentTasksCustomMetaData tasks) {
        if (tasks == null) {
            return null;
        }
        return tasks.getTask(MLMetadataField.datafeedTaskId(datafeedId));
    }

    public static JobState getJobState(String jobId, @Nullable PersistentTasksCustomMetaData tasks) {
        PersistentTask<?> task = getJobTask(jobId, tasks);
        if (task != null) {
            JobTaskStatus jobTaskState = (JobTaskStatus) task.getStatus();
            if (jobTaskState == null) {
                return JobState.OPENING;
            }
            return jobTaskState.getState();
        }
        // If we haven't opened a job than there will be no persistent task, which is the same as if the job was closed
        return JobState.CLOSED;
    }

    public static DatafeedState getDatafeedState(String datafeedId, @Nullable PersistentTasksCustomMetaData tasks) {
        PersistentTask<?> task = getDatafeedTask(datafeedId, tasks);
        if (task != null && task.getStatus() != null) {
            return (DatafeedState) task.getStatus();
        } else {
            // If we haven't started a datafeed then there will be no persistent task,
            // which is the same as if the datafeed was't started
            return DatafeedState.STOPPED;
        }
    }

    public static MlMetadata getMlMetadata(ClusterState state) {
        MlMetadata mlMetadata = (state == null) ? null : state.getMetaData().custom(MLMetadataField.TYPE);
        if (mlMetadata == null) {
            return EMPTY_METADATA;
        }
        return mlMetadata;
    }

    public static class JobAlreadyMarkedAsDeletedException extends RuntimeException {
    }
}
