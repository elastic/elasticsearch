/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.groups.GroupOrJobLookup;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.filterSecurityHeaders;

public class MlMetadata implements Metadata.Custom {

    public static final String TYPE = "ml";
    private static final ParseField JOBS_FIELD = new ParseField("jobs");
    private static final ParseField DATAFEEDS_FIELD = new ParseField("datafeeds");
    public static final ParseField UPGRADE_MODE = new ParseField("upgrade_mode");
    public static final ParseField RESET_MODE = new ParseField("reset_mode");

    public static final MlMetadata EMPTY_METADATA = new MlMetadata(
        Collections.emptySortedMap(),
        Collections.emptySortedMap(),
        false,
        false
    );
    // This parser follows the pattern that metadata is parsed leniently (to allow for enhancements)
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = new ObjectParser<>("ml_metadata", true, Builder::new);

    static {
        LENIENT_PARSER.declareObjectArray(Builder::putJobs, (p, c) -> Job.LENIENT_PARSER.apply(p, c).build(), JOBS_FIELD);
        LENIENT_PARSER.declareObjectArray(Builder::putDatafeeds,
                (p, c) -> DatafeedConfig.LENIENT_PARSER.apply(p, c).build(), DATAFEEDS_FIELD);
        LENIENT_PARSER.declareBoolean(Builder::isUpgradeMode, UPGRADE_MODE);
        LENIENT_PARSER.declareBoolean(Builder::isResetMode, RESET_MODE);
    }

    private final SortedMap<String, Job> jobs;
    private final SortedMap<String, DatafeedConfig> datafeeds;
    private final boolean upgradeMode;
    private final boolean resetMode;
    private final GroupOrJobLookup groupOrJobLookup;

    private MlMetadata(SortedMap<String, Job> jobs, SortedMap<String, DatafeedConfig> datafeeds, boolean upgradeMode, boolean resetMode) {
        this.jobs = Collections.unmodifiableSortedMap(jobs);
        this.datafeeds = Collections.unmodifiableSortedMap(datafeeds);
        this.groupOrJobLookup = new GroupOrJobLookup(jobs.values());
        this.upgradeMode = upgradeMode;
        this.resetMode = resetMode;
    }

    public Map<String, Job> getJobs() {
        return jobs;
    }

    public Set<String> expandJobIds(String expression, boolean allowNoMatch) {
        return groupOrJobLookup.expandJobIds(expression, allowNoMatch);
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

    public Map<String, DatafeedConfig> getDatafeedsByJobIds(Set<String> jobIds) {
        return datafeeds.values()
            .stream()
            .filter(df -> jobIds.contains(df.getJobId()))
            .collect(Collectors.toMap(DatafeedConfig::getJobId, Function.identity()));
    }

    public Set<String> expandDatafeedIds(String expression, boolean allowNoMatch) {
        return NameResolver.newUnaliased(datafeeds.keySet(), ExceptionsHelper::missingDatafeedException)
                .expand(expression, allowNoMatch);
    }

    public boolean isUpgradeMode() {
        return upgradeMode;
    }

    public boolean isResetMode() {
        return resetMode;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumIndexCompatibilityVersion();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
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
        this.upgradeMode = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            this.resetMode = in.readBoolean();
        } else {
            this.resetMode = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeMap(jobs, out);
        writeMap(datafeeds, out);
        out.writeBoolean(upgradeMode);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeBoolean(resetMode);
        }
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
                new DelegatingMapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"), params);
        mapValuesToXContent(JOBS_FIELD, jobs, builder, extendedParams);
        mapValuesToXContent(DATAFEEDS_FIELD, datafeeds, builder, extendedParams);
        builder.field(UPGRADE_MODE.getPreferredName(), upgradeMode);
        builder.field(RESET_MODE.getPreferredName(), resetMode);
        return builder;
    }

    private static <T extends ToXContent> void mapValuesToXContent(ParseField field, Map<String, T> map, XContentBuilder builder,
                                                                   Params params) throws IOException {
        if (map.isEmpty()) {
            return;
        }

        builder.startArray(field.getPreferredName());
        for (Map.Entry<String, T> entry : map.entrySet()) {
            entry.getValue().toXContent(builder, params);
        }
        builder.endArray();
    }

    public static class MlMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, Job>> jobs;
        final Diff<Map<String, DatafeedConfig>> datafeeds;
        final boolean upgradeMode;
        final boolean resetMode;

        MlMetadataDiff(MlMetadata before, MlMetadata after) {
            this.jobs = DiffableUtils.diff(before.jobs, after.jobs, DiffableUtils.getStringKeySerializer());
            this.datafeeds = DiffableUtils.diff(before.datafeeds, after.datafeeds, DiffableUtils.getStringKeySerializer());
            this.upgradeMode = after.upgradeMode;
            this.resetMode = after.resetMode;
        }

        public MlMetadataDiff(StreamInput in) throws IOException {
            this.jobs = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Job::new,
                    MlMetadataDiff::readJobDiffFrom);
            this.datafeeds = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), DatafeedConfig::new,
                    MlMetadataDiff::readDatafeedDiffFrom);
            upgradeMode = in.readBoolean();
            if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
                resetMode = in.readBoolean();
            } else {
                resetMode = false;
            }
        }

        /**
         * Merge the diff with the ML metadata.
         * @param part The current ML metadata.
         * @return The new ML metadata.
         */
        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            TreeMap<String, Job> newJobs = new TreeMap<>(jobs.apply(((MlMetadata) part).jobs));
            TreeMap<String, DatafeedConfig> newDatafeeds = new TreeMap<>(datafeeds.apply(((MlMetadata) part).datafeeds));
            return new MlMetadata(newJobs, newDatafeeds, upgradeMode, resetMode);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            jobs.writeTo(out);
            datafeeds.writeTo(out);
            out.writeBoolean(upgradeMode);
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                out.writeBoolean(resetMode);
            }
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        static Diff<Job> readJobDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(Job::new, in);
        }

        static Diff<DatafeedConfig> readDatafeedDiffFrom(StreamInput in) throws IOException {
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
                Objects.equals(datafeeds, that.datafeeds) &&
                upgradeMode == that.upgradeMode &&
                resetMode == that.resetMode;
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs, datafeeds, upgradeMode, resetMode);
    }

    public static class Builder {

        private TreeMap<String, Job> jobs;
        private TreeMap<String, DatafeedConfig> datafeeds;
        private boolean upgradeMode;
        private boolean resetMode;

        public static Builder from(@Nullable MlMetadata previous) {
            return new Builder(previous);
        }

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
                upgradeMode = previous.upgradeMode;
                resetMode = previous.resetMode;
            }
        }

        public Builder putJob(Job job, boolean overwrite) {
            if (jobs.containsKey(job.getId()) && overwrite == false) {
                throw ExceptionsHelper.jobAlreadyExists(job.getId());
            }
            this.jobs.put(job.getId(), job);
            return this;
        }

        public Builder putJobs(Collection<Job> jobs) {
            for (Job job : jobs) {
                putJob(job, true);
            }
            return this;
        }

        public Builder putDatafeed(DatafeedConfig datafeedConfig, Map<String, String> headers, NamedXContentRegistry xContentRegistry) {
            if (datafeeds.containsKey(datafeedConfig.getId())) {
                throw ExceptionsHelper.datafeedAlreadyExists(datafeedConfig.getId());
            }

            String jobId = datafeedConfig.getJobId();
            checkJobIsAvailableForDatafeed(jobId);
            Job job = jobs.get(jobId);
            DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry);

            if (headers.isEmpty() == false) {
                // Adjust the request, adding security headers from the current thread context
                datafeedConfig = new DatafeedConfig.Builder(datafeedConfig)
                    .setHeaders(filterSecurityHeaders(headers))
                    .build();
            }

            datafeeds.put(datafeedConfig.getId(), datafeedConfig);
            return this;
        }

        private void checkJobIsAvailableForDatafeed(String jobId) {
            Job job = jobs.get(jobId);
            if (job == null || job.isDeleting()) {
                throw ExceptionsHelper.missingJobException(jobId);
            }
            Optional<DatafeedConfig> existingDatafeed = getDatafeedByJobId(jobId);
            if (existingDatafeed.isPresent()) {
                throw ExceptionsHelper.conflictStatusException("A datafeed [" + existingDatafeed.get().getId()
                        + "] already exists for job [" + jobId + "]");
            }
        }

        private Optional<DatafeedConfig> getDatafeedByJobId(String jobId) {
            return datafeeds.values().stream().filter(s -> s.getJobId().equals(jobId)).findFirst();
        }

        public Builder putDatafeeds(Collection<DatafeedConfig> datafeeds) {
            for (DatafeedConfig datafeed : datafeeds) {
                this.datafeeds.put(datafeed.getId(), datafeed);
            }
            return this;
        }

        public Builder isUpgradeMode(boolean upgradeMode) {
            this.upgradeMode = upgradeMode;
            return this;
        }

        public Builder isResetMode(boolean resetMode) {
            this.resetMode = resetMode;
            return this;
        }

        public MlMetadata build() {
            return new MlMetadata(jobs, datafeeds, upgradeMode, resetMode);
        }
    }

    public static MlMetadata getMlMetadata(ClusterState state) {
        MlMetadata mlMetadata = (state == null) ? null : state.getMetadata().custom(TYPE);
        if (mlMetadata == null) {
            return EMPTY_METADATA;
        }
        return mlMetadata;
    }
}
