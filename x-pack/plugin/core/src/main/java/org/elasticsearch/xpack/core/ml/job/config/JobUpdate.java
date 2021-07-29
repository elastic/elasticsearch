/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class JobUpdate implements Writeable, ToXContentObject {
    public static final ParseField DETECTORS = new ParseField("detectors");
    public static final ParseField CLEAR_JOB_FINISH_TIME = new ParseField("clear_job_finish_time");

    // For internal updates
    static final ConstructingObjectParser<Builder, Void> INTERNAL_PARSER = new ConstructingObjectParser<>(
            "job_update", args -> new Builder((String) args[0]));

    // For parsing REST requests
    public static final ConstructingObjectParser<Builder, Void> EXTERNAL_PARSER = new ConstructingObjectParser<>(
            "job_update", args -> new Builder((String) args[0]));

    static {
        for (ConstructingObjectParser<Builder, Void> parser : Arrays.asList(INTERNAL_PARSER, EXTERNAL_PARSER)) {
            parser.declareString(ConstructingObjectParser.optionalConstructorArg(), Job.ID);
            parser.declareStringArray(Builder::setGroups, Job.GROUPS);
            parser.declareStringOrNull(Builder::setDescription, Job.DESCRIPTION);
            parser.declareObjectArray(Builder::setDetectorUpdates, DetectorUpdate.PARSER, DETECTORS);
            parser.declareObject(Builder::setModelPlotConfig, ModelPlotConfig.STRICT_PARSER, Job.MODEL_PLOT_CONFIG);
            parser.declareObject(Builder::setAnalysisLimits, AnalysisLimits.STRICT_PARSER, Job.ANALYSIS_LIMITS);
            parser.declareString((builder, val) -> builder.setBackgroundPersistInterval(
                    TimeValue.parseTimeValue(val, Job.BACKGROUND_PERSIST_INTERVAL.getPreferredName())), Job.BACKGROUND_PERSIST_INTERVAL);
            parser.declareLong(Builder::setRenormalizationWindowDays, Job.RENORMALIZATION_WINDOW_DAYS);
            parser.declareLong(Builder::setResultsRetentionDays, Job.RESULTS_RETENTION_DAYS);
            parser.declareLong(Builder::setSystemAnnotationsRetentionDays, Job.SYSTEM_ANNOTATIONS_RETENTION_DAYS);
            parser.declareLong(Builder::setModelSnapshotRetentionDays, Job.MODEL_SNAPSHOT_RETENTION_DAYS);
            parser.declareLong(Builder::setDailyModelSnapshotRetentionAfterDays, Job.DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS);
            parser.declareStringArray(Builder::setCategorizationFilters, AnalysisConfig.CATEGORIZATION_FILTERS);
            parser.declareObject(Builder::setPerPartitionCategorizationConfig, PerPartitionCategorizationConfig.STRICT_PARSER,
                    AnalysisConfig.PER_PARTITION_CATEGORIZATION);
            parser.declareField(Builder::setCustomSettings, (p, c) -> p.map(), Job.CUSTOM_SETTINGS, ObjectParser.ValueType.OBJECT);
            parser.declareBoolean(Builder::setAllowLazyOpen, Job.ALLOW_LAZY_OPEN);
        }
        // These fields should not be set by a REST request
        INTERNAL_PARSER.declareString(Builder::setModelSnapshotId, Job.MODEL_SNAPSHOT_ID);
        INTERNAL_PARSER.declareString(Builder::setModelSnapshotMinVersion, Job.MODEL_SNAPSHOT_MIN_VERSION);
        INTERNAL_PARSER.declareString(Builder::setJobVersion, Job.JOB_VERSION);
        INTERNAL_PARSER.declareBoolean(Builder::setClearFinishTime, CLEAR_JOB_FINISH_TIME);
        INTERNAL_PARSER.declareObject(Builder::setBlocked, Blocked.STRICT_PARSER, Job.BLOCKED);
    }

    private final String jobId;
    private final List<String> groups;
    private final String description;
    private final List<DetectorUpdate> detectorUpdates;
    private final ModelPlotConfig modelPlotConfig;
    private final AnalysisLimits analysisLimits;
    private final Long renormalizationWindowDays;
    private final TimeValue backgroundPersistInterval;
    private final Long modelSnapshotRetentionDays;
    private final Long dailyModelSnapshotRetentionAfterDays;
    private final Long resultsRetentionDays;
    private final Long systemAnnotationsRetentionDays;
    private final List<String> categorizationFilters;
    private final PerPartitionCategorizationConfig perPartitionCategorizationConfig;
    private final Map<String, Object> customSettings;
    private final String modelSnapshotId;
    private final Version modelSnapshotMinVersion;
    private final Version jobVersion;
    private final Boolean clearJobFinishTime;
    private final Boolean allowLazyOpen;
    private final Blocked blocked;

    private JobUpdate(String jobId, @Nullable List<String> groups, @Nullable String description,
                      @Nullable List<DetectorUpdate> detectorUpdates, @Nullable ModelPlotConfig modelPlotConfig,
                      @Nullable AnalysisLimits analysisLimits, @Nullable TimeValue backgroundPersistInterval,
                      @Nullable Long renormalizationWindowDays, @Nullable Long resultsRetentionDays,
                      @Nullable Long systemAnnotationsRetentionDays, @Nullable Long modelSnapshotRetentionDays,
                      @Nullable Long dailyModelSnapshotRetentionAfterDays, @Nullable List<String> categorizationFilters,
                      @Nullable PerPartitionCategorizationConfig perPartitionCategorizationConfig,
                      @Nullable Map<String, Object> customSettings, @Nullable String modelSnapshotId,
                      @Nullable Version modelSnapshotMinVersion, @Nullable Version jobVersion, @Nullable Boolean clearJobFinishTime,
                      @Nullable Boolean allowLazyOpen, @Nullable Blocked blocked) {
        this.jobId = jobId;
        this.groups = groups;
        this.description = description;
        this.detectorUpdates = detectorUpdates;
        this.modelPlotConfig = modelPlotConfig;
        this.analysisLimits = analysisLimits;
        this.renormalizationWindowDays = renormalizationWindowDays;
        this.backgroundPersistInterval = backgroundPersistInterval;
        this.modelSnapshotRetentionDays = modelSnapshotRetentionDays;
        this.dailyModelSnapshotRetentionAfterDays = dailyModelSnapshotRetentionAfterDays;
        this.resultsRetentionDays = resultsRetentionDays;
        this.systemAnnotationsRetentionDays = systemAnnotationsRetentionDays;
        this.categorizationFilters = categorizationFilters;
        this.perPartitionCategorizationConfig = perPartitionCategorizationConfig;
        this.customSettings = customSettings;
        this.modelSnapshotId = modelSnapshotId;
        this.modelSnapshotMinVersion = modelSnapshotMinVersion;
        this.jobVersion = jobVersion;
        this.clearJobFinishTime = clearJobFinishTime;
        this.allowLazyOpen = allowLazyOpen;
        this.blocked = blocked;
    }

    public JobUpdate(StreamInput in) throws IOException {
        jobId = in.readString();
        if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
            String[] groupsArray = in.readOptionalStringArray();
            groups = groupsArray == null ? null : Arrays.asList(groupsArray);
        } else {
            groups = null;
        }
        description = in.readOptionalString();
        if (in.readBoolean()) {
            detectorUpdates = in.readList(DetectorUpdate::new);
        } else {
            detectorUpdates = null;
        }
        modelPlotConfig = in.readOptionalWriteable(ModelPlotConfig::new);
        analysisLimits = in.readOptionalWriteable(AnalysisLimits::new);
        renormalizationWindowDays = in.readOptionalLong();
        backgroundPersistInterval = in.readOptionalTimeValue();
        modelSnapshotRetentionDays = in.readOptionalLong();
        if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
            dailyModelSnapshotRetentionAfterDays = in.readOptionalLong();
        } else {
            dailyModelSnapshotRetentionAfterDays = null;
        }
        resultsRetentionDays = in.readOptionalLong();
        if (in.getVersion().onOrAfter(Version.V_7_15_0)) {
            systemAnnotationsRetentionDays = in.readOptionalLong();
        } else {
            systemAnnotationsRetentionDays = null;
        }
        if (in.readBoolean()) {
            categorizationFilters = in.readStringList();
        } else {
            categorizationFilters = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            perPartitionCategorizationConfig = in.readOptionalWriteable(PerPartitionCategorizationConfig::new);
        } else {
            perPartitionCategorizationConfig = null;
        }
        customSettings = in.readMap();
        modelSnapshotId = in.readOptionalString();
        // was establishedModelMemory
        if (in.getVersion().onOrAfter(Version.V_6_1_0) && in.getVersion().before(Version.V_7_0_0)) {
            in.readOptionalLong();
        }
        if (in.getVersion().onOrAfter(Version.V_6_3_0) && in.readBoolean()) {
            jobVersion = Version.readVersion(in);
        } else {
            jobVersion = null;
        }
        if (in.getVersion().onOrAfter(Version.V_6_6_0)) {
            clearJobFinishTime = in.readOptionalBoolean();
        } else {
            clearJobFinishTime = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_0_0) && in.readBoolean()) {
            modelSnapshotMinVersion = Version.readVersion(in);
        } else {
            modelSnapshotMinVersion = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_5_0)) {
            allowLazyOpen = in.readOptionalBoolean();
        } else {
            allowLazyOpen = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_14_0)) {
            blocked = in.readOptionalWriteable(Blocked::new);
        } else {
            blocked = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        String[] groupsArray = groups == null ? null : groups.toArray(new String[0]);
        out.writeOptionalStringArray(groupsArray);
        out.writeOptionalString(description);
        out.writeBoolean(detectorUpdates != null);
        if (detectorUpdates != null) {
            out.writeList(detectorUpdates);
        }
        out.writeOptionalWriteable(modelPlotConfig);
        out.writeOptionalWriteable(analysisLimits);
        out.writeOptionalLong(renormalizationWindowDays);
        out.writeOptionalTimeValue(backgroundPersistInterval);
        out.writeOptionalLong(modelSnapshotRetentionDays);
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            out.writeOptionalLong(dailyModelSnapshotRetentionAfterDays);
        }
        out.writeOptionalLong(resultsRetentionDays);
        if (out.getVersion().onOrAfter(Version.V_7_15_0)) {
            out.writeOptionalLong(systemAnnotationsRetentionDays);
        }
        out.writeBoolean(categorizationFilters != null);
        if (categorizationFilters != null) {
            out.writeStringCollection(categorizationFilters);
        }
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeOptionalWriteable(perPartitionCategorizationConfig);
        }
        out.writeMap(customSettings);
        out.writeOptionalString(modelSnapshotId);
        // was establishedModelMemory
        if (out.getVersion().onOrAfter(Version.V_6_1_0) && out.getVersion().before(Version.V_7_0_0)) {
            out.writeOptionalLong(null);
        }
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            if (jobVersion != null) {
                out.writeBoolean(true);
                Version.writeVersion(jobVersion, out);
            } else {
                out.writeBoolean(false);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_6_6_0)) {
            out.writeOptionalBoolean(clearJobFinishTime);
        }
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            if (modelSnapshotMinVersion != null) {
                out.writeBoolean(true);
                Version.writeVersion(modelSnapshotMinVersion, out);
            } else {
                out.writeBoolean(false);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_7_5_0)) {
            out.writeOptionalBoolean(allowLazyOpen);
        }
        if (out.getVersion().onOrAfter(Version.V_7_14_0)) {
            out.writeOptionalWriteable(blocked);
        }
    }

    public String getJobId() {
        return jobId;
    }

    public List<String> getGroups() {
        return groups;
    }

    public String getDescription() {
        return description;
    }

    public List<DetectorUpdate> getDetectorUpdates() {
        return detectorUpdates;
    }

    public ModelPlotConfig getModelPlotConfig() {
        return modelPlotConfig;
    }

    public AnalysisLimits getAnalysisLimits() {
        return analysisLimits;
    }

    public Long getRenormalizationWindowDays() {
        return renormalizationWindowDays;
    }

    public TimeValue getBackgroundPersistInterval() {
        return backgroundPersistInterval;
    }

    public Long getModelSnapshotRetentionDays() {
        return modelSnapshotRetentionDays;
    }

    public Long getDailyModelSnapshotRetentionAfterDays() {
        return dailyModelSnapshotRetentionAfterDays;
    }

    public Long getResultsRetentionDays() {
        return resultsRetentionDays;
    }

    public Long getSystemAnnotationsRetentionDays() {
        return systemAnnotationsRetentionDays;
    }

    public List<String> getCategorizationFilters() {
        return categorizationFilters;
    }

    public PerPartitionCategorizationConfig getPerPartitionCategorizationConfig() {
        return perPartitionCategorizationConfig;
    }

    public Map<String, Object> getCustomSettings() {
        return customSettings;
    }

    public String getModelSnapshotId() {
        return modelSnapshotId;
    }

    public Version getModelSnapshotMinVersion() {
        return modelSnapshotMinVersion;
    }

    public Version getJobVersion() {
        return jobVersion;
    }

    public Boolean getClearJobFinishTime() {
        return clearJobFinishTime;
    }

    public Boolean getAllowLazyOpen() {
        return allowLazyOpen;
    }

    public boolean isAutodetectProcessUpdate() {
        return modelPlotConfig != null || perPartitionCategorizationConfig != null || detectorUpdates != null || groups != null;
    }

    public Blocked getBlocked() {
        return blocked;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        if (groups != null) {
            builder.field(Job.GROUPS.getPreferredName(), groups);
        }
        if (description != null) {
            builder.field(Job.DESCRIPTION.getPreferredName(), description);
        }
        if (detectorUpdates != null) {
            builder.field(DETECTORS.getPreferredName(), detectorUpdates);
        }
        if (modelPlotConfig != null) {
            builder.field(Job.MODEL_PLOT_CONFIG.getPreferredName(), modelPlotConfig);
        }
        if (analysisLimits != null) {
            builder.field(Job.ANALYSIS_LIMITS.getPreferredName(), analysisLimits);
        }
        if (renormalizationWindowDays != null) {
            builder.field(Job.RENORMALIZATION_WINDOW_DAYS.getPreferredName(), renormalizationWindowDays);
        }
        if (backgroundPersistInterval != null) {
            builder.field(Job.BACKGROUND_PERSIST_INTERVAL.getPreferredName(), backgroundPersistInterval);
        }
        if (modelSnapshotRetentionDays != null) {
            builder.field(Job.MODEL_SNAPSHOT_RETENTION_DAYS.getPreferredName(), modelSnapshotRetentionDays);
        }
        if (dailyModelSnapshotRetentionAfterDays != null) {
            builder.field(Job.DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS.getPreferredName(), dailyModelSnapshotRetentionAfterDays);
        }
        if (resultsRetentionDays != null) {
            builder.field(Job.RESULTS_RETENTION_DAYS.getPreferredName(), resultsRetentionDays);
        }
        if (systemAnnotationsRetentionDays != null) {
            builder.field(Job.SYSTEM_ANNOTATIONS_RETENTION_DAYS.getPreferredName(), systemAnnotationsRetentionDays);
        }
        if (categorizationFilters != null) {
            builder.field(AnalysisConfig.CATEGORIZATION_FILTERS.getPreferredName(), categorizationFilters);
        }
        if (perPartitionCategorizationConfig != null) {
            builder.field(AnalysisConfig.PER_PARTITION_CATEGORIZATION.getPreferredName(), perPartitionCategorizationConfig);
        }
        if (customSettings != null) {
            builder.field(Job.CUSTOM_SETTINGS.getPreferredName(), customSettings);
        }
        if (modelSnapshotId != null) {
            builder.field(Job.MODEL_SNAPSHOT_ID.getPreferredName(), modelSnapshotId);
        }
        if (modelSnapshotMinVersion != null) {
            builder.field(Job.MODEL_SNAPSHOT_MIN_VERSION.getPreferredName(), modelSnapshotMinVersion);
        }
        if (jobVersion != null) {
            builder.field(Job.JOB_VERSION.getPreferredName(), jobVersion);
        }
        if (clearJobFinishTime != null) {
            builder.field(CLEAR_JOB_FINISH_TIME.getPreferredName(), clearJobFinishTime);
        }
        if (allowLazyOpen != null) {
            builder.field(Job.ALLOW_LAZY_OPEN.getPreferredName(), allowLazyOpen);
        }
        if (blocked != null) {
            builder.field(Job.BLOCKED.getPreferredName(), blocked);
        }
        builder.endObject();
        return builder;
    }

    public Set<String> getUpdateFields() {
        Set<String> updateFields = new TreeSet<>();
        if (groups != null) {
            updateFields.add(Job.GROUPS.getPreferredName());
        }
        if (description != null) {
            updateFields.add(Job.DESCRIPTION.getPreferredName());
        }
        if (detectorUpdates != null) {
            updateFields.add(DETECTORS.getPreferredName());
        }
        if (modelPlotConfig != null) {
            updateFields.add(Job.MODEL_PLOT_CONFIG.getPreferredName());
        }
        if (analysisLimits != null) {
            updateFields.add(Job.ANALYSIS_LIMITS.getPreferredName());
        }
        if (renormalizationWindowDays != null) {
            updateFields.add(Job.RENORMALIZATION_WINDOW_DAYS.getPreferredName());
        }
        if (backgroundPersistInterval != null) {
            updateFields.add(Job.BACKGROUND_PERSIST_INTERVAL.getPreferredName());
        }
        if (modelSnapshotRetentionDays != null) {
            updateFields.add(Job.MODEL_SNAPSHOT_RETENTION_DAYS.getPreferredName());
        }
        if (dailyModelSnapshotRetentionAfterDays != null) {
            updateFields.add(Job.DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS.getPreferredName());
        }
        if (resultsRetentionDays != null) {
            updateFields.add(Job.RESULTS_RETENTION_DAYS.getPreferredName());
        }
        if (systemAnnotationsRetentionDays != null) {
            updateFields.add(Job.SYSTEM_ANNOTATIONS_RETENTION_DAYS.getPreferredName());
        }
        if (categorizationFilters != null) {
            updateFields.add(AnalysisConfig.CATEGORIZATION_FILTERS.getPreferredName());
        }
        if (perPartitionCategorizationConfig != null) {
            updateFields.add(AnalysisConfig.PER_PARTITION_CATEGORIZATION.getPreferredName());
        }
        if (customSettings != null) {
            updateFields.add(Job.CUSTOM_SETTINGS.getPreferredName());
        }
        if (modelSnapshotId != null) {
            updateFields.add(Job.MODEL_SNAPSHOT_ID.getPreferredName());
        }
        if (modelSnapshotMinVersion != null) {
            updateFields.add(Job.MODEL_SNAPSHOT_MIN_VERSION.getPreferredName());
        }
        if (jobVersion != null) {
            updateFields.add(Job.JOB_VERSION.getPreferredName());
        }
        if (allowLazyOpen != null) {
            updateFields.add(Job.ALLOW_LAZY_OPEN.getPreferredName());
        }
        return updateFields;
    }

    /**
     * Updates {@code source} with the new values in this object returning a new {@link Job}.
     *
     * @param source              Source job to be updated
     * @param maxModelMemoryLimit The maximum model memory allowed
     * @return A new job equivalent to {@code source} updated.
     */
    public Job mergeWithJob(Job source, ByteSizeValue maxModelMemoryLimit) {
        Job.Builder builder = new Job.Builder(source);
        AnalysisConfig currentAnalysisConfig = source.getAnalysisConfig();
        AnalysisConfig.Builder newAnalysisConfig = new AnalysisConfig.Builder(currentAnalysisConfig);

        if (groups != null) {
            builder.setGroups(groups);
        }
        if (description != null) {
            builder.setDescription(description);
        }
        if (detectorUpdates != null && detectorUpdates.isEmpty() == false) {
            int numDetectors = currentAnalysisConfig.getDetectors().size();
            for (DetectorUpdate dd : detectorUpdates) {
                if (dd.getDetectorIndex() >= numDetectors) {
                    throw ExceptionsHelper.badRequestException("Supplied detector_index [{}] is >= the number of detectors [{}]",
                            dd.getDetectorIndex(), numDetectors);
                }

                Detector.Builder detectorBuilder = new Detector.Builder(currentAnalysisConfig.getDetectors().get(dd.getDetectorIndex()));
                if (dd.getDescription() != null) {
                    detectorBuilder.setDetectorDescription(dd.getDescription());
                }
                if (dd.getRules() != null) {
                    detectorBuilder.setRules(dd.getRules());
                }

                newAnalysisConfig.setDetector(dd.getDetectorIndex(), detectorBuilder.build());
            }
        }
        if (modelPlotConfig != null) {
            builder.setModelPlotConfig(modelPlotConfig);
        }
        if (analysisLimits != null) {
            AnalysisLimits validatedLimits = AnalysisLimits.validateAndSetDefaults(analysisLimits, maxModelMemoryLimit,
                    AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB);
            builder.setAnalysisLimits(validatedLimits);
        }
        if (renormalizationWindowDays != null) {
            builder.setRenormalizationWindowDays(renormalizationWindowDays);
        }
        if (backgroundPersistInterval != null) {
            builder.setBackgroundPersistInterval(backgroundPersistInterval);
        }
        if (modelSnapshotRetentionDays != null) {
            builder.setModelSnapshotRetentionDays(modelSnapshotRetentionDays);
        }
        if (dailyModelSnapshotRetentionAfterDays != null) {
            builder.setDailyModelSnapshotRetentionAfterDays(dailyModelSnapshotRetentionAfterDays);
        }
        if (resultsRetentionDays != null) {
            builder.setResultsRetentionDays(resultsRetentionDays);
        }
        if (systemAnnotationsRetentionDays != null) {
            builder.setSystemAnnotationsRetentionDays(systemAnnotationsRetentionDays);
        }
        if (categorizationFilters != null) {
            newAnalysisConfig.setCategorizationFilters(categorizationFilters);
        }
        if (perPartitionCategorizationConfig != null) {
            // Whether per-partition categorization is enabled cannot be changed, only the lower level details
            if (perPartitionCategorizationConfig.isEnabled() !=
                    currentAnalysisConfig.getPerPartitionCategorizationConfig().isEnabled()) {
                throw ExceptionsHelper.badRequestException("analysis_config.per_partition_categorization.enabled cannot be updated");
            }
            newAnalysisConfig.setPerPartitionCategorizationConfig(perPartitionCategorizationConfig);
        }
        if (customSettings != null) {
            builder.setCustomSettings(customSettings);
        }
        if (modelSnapshotId != null) {
            builder.setModelSnapshotId(ModelSnapshot.isTheEmptySnapshot(modelSnapshotId) ? null : modelSnapshotId);
        }
        if (modelSnapshotMinVersion != null) {
            builder.setModelSnapshotMinVersion(modelSnapshotMinVersion);
        }
        if (jobVersion != null) {
            builder.setJobVersion(jobVersion);
        }
        if (clearJobFinishTime != null && clearJobFinishTime) {
            builder.setFinishedTime(null);
        }
        if (allowLazyOpen != null) {
            builder.setAllowLazyOpen(allowLazyOpen);
        }
        if (blocked != null) {
            builder.setBlocked(blocked);
        }

        builder.setAnalysisConfig(newAnalysisConfig);
        return builder.build();
    }

    boolean isNoop(Job job) {
        return (groups == null || Objects.equals(groups, job.getGroups()))
                && (description == null || Objects.equals(description, job.getDescription()))
                && (modelPlotConfig == null || Objects.equals(modelPlotConfig, job.getModelPlotConfig()))
                && (analysisLimits == null || Objects.equals(analysisLimits, job.getAnalysisLimits()))
                && updatesDetectors(job) == false
                && (renormalizationWindowDays == null || Objects.equals(renormalizationWindowDays, job.getRenormalizationWindowDays()))
                && (backgroundPersistInterval == null || Objects.equals(backgroundPersistInterval, job.getBackgroundPersistInterval()))
                && (modelSnapshotRetentionDays == null || Objects.equals(modelSnapshotRetentionDays, job.getModelSnapshotRetentionDays()))
                && (dailyModelSnapshotRetentionAfterDays == null
                        || Objects.equals(dailyModelSnapshotRetentionAfterDays, job.getDailyModelSnapshotRetentionAfterDays()))
                && (resultsRetentionDays == null || Objects.equals(resultsRetentionDays, job.getResultsRetentionDays()))
                && (systemAnnotationsRetentionDays == null
                        || Objects.equals(systemAnnotationsRetentionDays, job.getSystemAnnotationsRetentionDays()))
                && (categorizationFilters == null
                        || Objects.equals(categorizationFilters, job.getAnalysisConfig().getCategorizationFilters()))
                && (perPartitionCategorizationConfig == null
                        || Objects.equals(perPartitionCategorizationConfig, job.getAnalysisConfig().getPerPartitionCategorizationConfig()))
                && (customSettings == null || Objects.equals(customSettings, job.getCustomSettings()))
                && (modelSnapshotId == null || Objects.equals(modelSnapshotId, job.getModelSnapshotId()))
                && (modelSnapshotMinVersion == null || Objects.equals(modelSnapshotMinVersion, job.getModelSnapshotMinVersion()))
                && (jobVersion == null || Objects.equals(jobVersion, job.getJobVersion()))
                && (clearJobFinishTime == null || clearJobFinishTime == false || job.getFinishedTime() == null)
                && (allowLazyOpen == null || Objects.equals(allowLazyOpen, job.allowLazyOpen()))
                && (blocked == null || Objects.equals(blocked, job.getBlocked()));
    }

    boolean updatesDetectors(Job job) {
        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        if (detectorUpdates == null) {
            return false;
        }
        for (DetectorUpdate detectorUpdate : detectorUpdates) {
            if (detectorUpdate.description == null && detectorUpdate.rules == null) {
                continue;
            }
            Detector detector = analysisConfig.getDetectors().get(detectorUpdate.detectorIndex);
            if (Objects.equals(detectorUpdate.description, detector.getDetectorDescription()) == false
                    || Objects.equals(detectorUpdate.rules, detector.getRules()) == false) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof JobUpdate == false) {
            return false;
        }

        JobUpdate that = (JobUpdate) other;

        return Objects.equals(this.jobId, that.jobId)
                && Objects.equals(this.groups, that.groups)
                && Objects.equals(this.description, that.description)
                && Objects.equals(this.detectorUpdates, that.detectorUpdates)
                && Objects.equals(this.modelPlotConfig, that.modelPlotConfig)
                && Objects.equals(this.analysisLimits, that.analysisLimits)
                && Objects.equals(this.renormalizationWindowDays, that.renormalizationWindowDays)
                && Objects.equals(this.backgroundPersistInterval, that.backgroundPersistInterval)
                && Objects.equals(this.modelSnapshotRetentionDays, that.modelSnapshotRetentionDays)
                && Objects.equals(this.dailyModelSnapshotRetentionAfterDays, that.dailyModelSnapshotRetentionAfterDays)
                && Objects.equals(this.resultsRetentionDays, that.resultsRetentionDays)
                && Objects.equals(this.systemAnnotationsRetentionDays, that.systemAnnotationsRetentionDays)
                && Objects.equals(this.categorizationFilters, that.categorizationFilters)
                && Objects.equals(this.perPartitionCategorizationConfig, that.perPartitionCategorizationConfig)
                && Objects.equals(this.customSettings, that.customSettings)
                && Objects.equals(this.modelSnapshotId, that.modelSnapshotId)
                && Objects.equals(this.modelSnapshotMinVersion, that.modelSnapshotMinVersion)
                && Objects.equals(this.jobVersion, that.jobVersion)
                && Objects.equals(this.clearJobFinishTime, that.clearJobFinishTime)
                && Objects.equals(this.allowLazyOpen, that.allowLazyOpen)
                && Objects.equals(this.blocked, that.blocked);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, groups, description, detectorUpdates, modelPlotConfig, analysisLimits, renormalizationWindowDays,
                backgroundPersistInterval, modelSnapshotRetentionDays, dailyModelSnapshotRetentionAfterDays, resultsRetentionDays,
                systemAnnotationsRetentionDays, categorizationFilters, perPartitionCategorizationConfig, customSettings, modelSnapshotId,
                modelSnapshotMinVersion, jobVersion, clearJobFinishTime, allowLazyOpen, blocked);
    }

    public static class DetectorUpdate implements Writeable, ToXContentObject {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DetectorUpdate, Void> PARSER =
                new ConstructingObjectParser<>("detector_update", a -> new DetectorUpdate((int) a[0], (String) a[1],
                        (List<DetectionRule>) a[2]));

        static {
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), Detector.DETECTOR_INDEX);
            PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), Job.DESCRIPTION);
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (parser, parseFieldMatcher) ->
                    DetectionRule.STRICT_PARSER.apply(parser, parseFieldMatcher).build(), Detector.CUSTOM_RULES_FIELD);
        }

        private final int detectorIndex;
        private final String description;
        private final List<DetectionRule> rules;

        public DetectorUpdate(int detectorIndex, String description, List<DetectionRule> rules) {
            this.detectorIndex = detectorIndex;
            this.description = description;
            this.rules = rules;
        }

        public DetectorUpdate(StreamInput in) throws IOException {
            detectorIndex = in.readInt();
            description = in.readOptionalString();
            if (in.readBoolean()) {
                rules = in.readList(DetectionRule::new);
            } else {
                rules = null;
            }
        }

        public int getDetectorIndex() {
            return detectorIndex;
        }

        public String getDescription() {
            return description;
        }

        public List<DetectionRule> getRules() {
            return rules;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(detectorIndex);
            out.writeOptionalString(description);
            out.writeBoolean(rules != null);
            if (rules != null) {
                out.writeList(rules);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(Detector.DETECTOR_INDEX.getPreferredName(), detectorIndex);
            if (description != null) {
                builder.field(Job.DESCRIPTION.getPreferredName(), description);
            }
            if (rules != null) {
                builder.field(Detector.CUSTOM_RULES_FIELD.getPreferredName(), rules);
            }
            builder.endObject();

            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(detectorIndex, description, rules);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other instanceof DetectorUpdate == false) {
                return false;
            }

            DetectorUpdate that = (DetectorUpdate) other;
            return this.detectorIndex == that.detectorIndex && Objects.equals(this.description, that.description)
                    && Objects.equals(this.rules, that.rules);
        }
    }

    public static class Builder {

        private String jobId;
        private List<String> groups;
        private String description;
        private List<DetectorUpdate> detectorUpdates;
        private ModelPlotConfig modelPlotConfig;
        private AnalysisLimits analysisLimits;
        private Long renormalizationWindowDays;
        private TimeValue backgroundPersistInterval;
        private Long modelSnapshotRetentionDays;
        private Long dailyModelSnapshotRetentionAfterDays;
        private Long resultsRetentionDays;
        private Long systemAnnotationsRetentionDays;
        private List<String> categorizationFilters;
        private PerPartitionCategorizationConfig perPartitionCategorizationConfig;
        private Map<String, Object> customSettings;
        private String modelSnapshotId;
        private Version modelSnapshotMinVersion;
        private Version jobVersion;
        private Boolean clearJobFinishTime;
        private Boolean allowLazyOpen;
        private Blocked blocked;

        public Builder(String jobId) {
            this.jobId = jobId;
        }

        public Builder setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setGroups(List<String> groups) {
            this.groups = groups;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setDetectorUpdates(List<DetectorUpdate> detectorUpdates) {
            this.detectorUpdates = detectorUpdates;
            return this;
        }

        public Builder setModelPlotConfig(ModelPlotConfig modelPlotConfig) {
            this.modelPlotConfig = modelPlotConfig;
            return this;
        }

        public Builder setAnalysisLimits(AnalysisLimits analysisLimits) {
            this.analysisLimits = analysisLimits;
            return this;
        }

        public Builder setRenormalizationWindowDays(Long renormalizationWindowDays) {
            this.renormalizationWindowDays = renormalizationWindowDays;
            return this;
        }

        public Builder setBackgroundPersistInterval(TimeValue backgroundPersistInterval) {
            this.backgroundPersistInterval = backgroundPersistInterval;
            return this;
        }

        public Builder setModelSnapshotRetentionDays(Long modelSnapshotRetentionDays) {
            this.modelSnapshotRetentionDays = modelSnapshotRetentionDays;
            return this;
        }

        public Builder setDailyModelSnapshotRetentionAfterDays(Long dailyModelSnapshotRetentionAfterDays) {
            this.dailyModelSnapshotRetentionAfterDays = dailyModelSnapshotRetentionAfterDays;
            return this;
        }

        public Builder setResultsRetentionDays(Long resultsRetentionDays) {
            this.resultsRetentionDays = resultsRetentionDays;
            return this;
        }

        public Builder setSystemAnnotationsRetentionDays(Long systemAnnotationsRetentionDays) {
            this.systemAnnotationsRetentionDays = systemAnnotationsRetentionDays;
            return this;
        }

        public Builder setCategorizationFilters(List<String> categorizationFilters) {
            this.categorizationFilters = categorizationFilters;
            return this;
        }

        public Builder setPerPartitionCategorizationConfig(PerPartitionCategorizationConfig perPartitionCategorizationConfig) {
            this.perPartitionCategorizationConfig = perPartitionCategorizationConfig;
            return this;
        }

        public Builder setCustomSettings(Map<String, Object> customSettings) {
            this.customSettings = customSettings;
            return this;
        }

        public Builder setModelSnapshotId(String modelSnapshotId) {
            this.modelSnapshotId = modelSnapshotId;
            return this;
        }

        public Builder setModelSnapshotMinVersion(Version modelSnapshotMinVersion) {
            this.modelSnapshotMinVersion = modelSnapshotMinVersion;
            return this;
        }

        public Builder setModelSnapshotMinVersion(String modelSnapshotMinVersion) {
            this.modelSnapshotMinVersion = Version.fromString(modelSnapshotMinVersion);
            return this;
        }

        public Builder setJobVersion(Version version) {
            this.jobVersion = version;
            return this;
        }

        public Builder setJobVersion(String version) {
            this.jobVersion = Version.fromString(version);
            return this;
        }

        public Builder setAllowLazyOpen(boolean allowLazyOpen) {
            this.allowLazyOpen = allowLazyOpen;
            return this;
        }

        public Builder setClearFinishTime(boolean clearJobFinishTime) {
            this.clearJobFinishTime = clearJobFinishTime;
            return this;
        }

        public Builder setBlocked(Blocked blocked) {
            this.blocked = blocked;
            return this;
        }

        public JobUpdate build() {
            return new JobUpdate(jobId, groups, description, detectorUpdates, modelPlotConfig, analysisLimits, backgroundPersistInterval,
                    renormalizationWindowDays, resultsRetentionDays, systemAnnotationsRetentionDays, modelSnapshotRetentionDays,
                    dailyModelSnapshotRetentionAfterDays, categorizationFilters, perPartitionCategorizationConfig, customSettings,
                    modelSnapshotId, modelSnapshotMinVersion, jobVersion, clearJobFinishTime, allowLazyOpen, blocked);
        }
    }
}
