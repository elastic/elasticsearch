/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.JOB_CONFIG_DATAFEED_CONFIG_JOB_ID_MISMATCH;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.EXCLUDE_GENERATED;

/**
 * This class represents a configured and created Job. The creation time is set
 * to the time the object was constructed and the finished time and last
 * data time fields are {@code null} until the job has seen some data or it is
 * finished respectively.
 */
public class Job extends AbstractDiffable<Job> implements Writeable, ToXContentObject {

    public static final String TYPE = "job";

    public static final String ANOMALY_DETECTOR_JOB_TYPE = "anomaly_detector";

    /*
     * Field names used in serialization
     */
    public static final ParseField ID = new ParseField("job_id");
    public static final ParseField JOB_TYPE = new ParseField("job_type");
    public static final ParseField JOB_VERSION = new ParseField("job_version");
    public static final ParseField GROUPS = new ParseField("groups");
    public static final ParseField ANALYSIS_CONFIG = AnalysisConfig.ANALYSIS_CONFIG;
    public static final ParseField ANALYSIS_LIMITS = new ParseField("analysis_limits");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField CUSTOM_SETTINGS = new ParseField("custom_settings");
    public static final ParseField DATA_DESCRIPTION = new ParseField("data_description");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField FINISHED_TIME = new ParseField("finished_time");
    public static final ParseField MODEL_PLOT_CONFIG = new ParseField("model_plot_config");
    public static final ParseField RENORMALIZATION_WINDOW_DAYS = new ParseField("renormalization_window_days");
    public static final ParseField BACKGROUND_PERSIST_INTERVAL = new ParseField("background_persist_interval");
    public static final ParseField MODEL_SNAPSHOT_RETENTION_DAYS = new ParseField("model_snapshot_retention_days");
    public static final ParseField DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS =
        new ParseField("daily_model_snapshot_retention_after_days");
    public static final ParseField RESULTS_RETENTION_DAYS = new ParseField("results_retention_days");
    public static final ParseField MODEL_SNAPSHOT_ID = new ParseField("model_snapshot_id");
    public static final ParseField MODEL_SNAPSHOT_MIN_VERSION = new ParseField("model_snapshot_min_version");
    public static final ParseField RESULTS_INDEX_NAME = new ParseField("results_index_name");
    public static final ParseField DELETING = new ParseField("deleting");
    public static final ParseField ALLOW_LAZY_OPEN = new ParseField("allow_lazy_open");
    public static final ParseField BLOCKED = new ParseField("blocked");
    public static final ParseField DATAFEED_CONFIG = new ParseField("datafeed_config");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("jobs");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    public static final TimeValue MIN_BACKGROUND_PERSIST_INTERVAL = TimeValue.timeValueHours(1);

    /**
     * This includes the overhead of thread stacks and data structures that the program might use that
     * are not instrumented.  (For the <code>autodetect</code> process categorization is not instrumented,
     * and the <code>normalize</code> process is not instrumented at all.)  But this overhead does NOT
     * include the memory used by loading the executable code.
     */
    public static final ByteSizeValue PROCESS_MEMORY_OVERHEAD = ByteSizeValue.ofMb(10);

    public static final long DEFAULT_MODEL_SNAPSHOT_RETENTION_DAYS = 10;
    public static final long DEFAULT_DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS = 1;

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>("job_details", ignoreUnknownFields, Builder::new);

        parser.declareString(Builder::setId, ID);
        parser.declareString(Builder::setJobType, JOB_TYPE);
        parser.declareString(Builder::setJobVersion, JOB_VERSION);
        parser.declareStringArray(Builder::setGroups, GROUPS);
        parser.declareStringOrNull(Builder::setDescription, DESCRIPTION);
        parser.declareField(Builder::setCreateTime,
                p -> TimeUtils.parseTimeField(p, CREATE_TIME.getPreferredName()), CREATE_TIME, ValueType.VALUE);
        parser.declareField(Builder::setFinishedTime,
                p -> TimeUtils.parseTimeField(p, FINISHED_TIME.getPreferredName()), FINISHED_TIME, ValueType.VALUE);
        parser.declareObject(Builder::setAnalysisConfig, ignoreUnknownFields ? AnalysisConfig.LENIENT_PARSER : AnalysisConfig.STRICT_PARSER,
            ANALYSIS_CONFIG);
        parser.declareObject(Builder::setAnalysisLimits, ignoreUnknownFields ? AnalysisLimits.LENIENT_PARSER : AnalysisLimits.STRICT_PARSER,
            ANALYSIS_LIMITS);
        parser.declareObject(Builder::setDataDescription,
            ignoreUnknownFields ? DataDescription.LENIENT_PARSER : DataDescription.STRICT_PARSER, DATA_DESCRIPTION);
        parser.declareObject(Builder::setModelPlotConfig,
            ignoreUnknownFields ? ModelPlotConfig.LENIENT_PARSER : ModelPlotConfig.STRICT_PARSER, MODEL_PLOT_CONFIG);
        parser.declareLong(Builder::setRenormalizationWindowDays, RENORMALIZATION_WINDOW_DAYS);
        parser.declareString((builder, val) -> builder.setBackgroundPersistInterval(
            TimeValue.parseTimeValue(val, BACKGROUND_PERSIST_INTERVAL.getPreferredName())), BACKGROUND_PERSIST_INTERVAL);
        parser.declareLong(Builder::setResultsRetentionDays, RESULTS_RETENTION_DAYS);
        parser.declareLong(Builder::setModelSnapshotRetentionDays, MODEL_SNAPSHOT_RETENTION_DAYS);
        parser.declareLong(Builder::setDailyModelSnapshotRetentionAfterDays, DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS);
        parser.declareField(Builder::setCustomSettings, (p, c) -> p.mapOrdered(), CUSTOM_SETTINGS, ValueType.OBJECT);
        parser.declareStringOrNull(Builder::setModelSnapshotId, MODEL_SNAPSHOT_ID);
        parser.declareStringOrNull(Builder::setModelSnapshotMinVersion, MODEL_SNAPSHOT_MIN_VERSION);
        parser.declareString(Builder::setResultsIndexName, RESULTS_INDEX_NAME);
        parser.declareBoolean(Builder::setDeleting, DELETING);
        parser.declareBoolean(Builder::setAllowLazyOpen, ALLOW_LAZY_OPEN);
        parser.declareObject(Builder::setBlocked, ignoreUnknownFields ? Blocked.LENIENT_PARSER : Blocked.STRICT_PARSER, BLOCKED);
        parser.declareObject(Builder::setDatafeed,
            ignoreUnknownFields ? DatafeedConfig.LENIENT_PARSER : DatafeedConfig.STRICT_PARSER,
            DATAFEED_CONFIG);
        return parser;
    }

    private final String jobId;
    private final String jobType;

    /**
     * The version when the job was created.
     * Will be null for versions before 5.5.
     */
    @Nullable
    private final Version jobVersion;

    private final List<String> groups;
    private final String description;
    // TODO: Use java.time for the Dates here: x-pack-elasticsearch#829
    private final Date createTime;
    private final Date finishedTime;
    private final AnalysisConfig analysisConfig;
    private final AnalysisLimits analysisLimits;
    private final DataDescription dataDescription;
    private final ModelPlotConfig modelPlotConfig;
    private final Long renormalizationWindowDays;
    private final TimeValue backgroundPersistInterval;
    private final Long modelSnapshotRetentionDays;
    private final Long dailyModelSnapshotRetentionAfterDays;
    private final Long resultsRetentionDays;
    private final Map<String, Object> customSettings;
    private final String modelSnapshotId;
    private final Version modelSnapshotMinVersion;
    private final String resultsIndexName;
    private final boolean deleting;
    private final boolean allowLazyOpen;
    private final Blocked blocked;
    private final DatafeedConfig datafeedConfig;

    private Job(String jobId, String jobType, Version jobVersion, List<String> groups, String description,
                Date createTime, Date finishedTime,
                AnalysisConfig analysisConfig, AnalysisLimits analysisLimits, DataDescription dataDescription,
                ModelPlotConfig modelPlotConfig, Long renormalizationWindowDays, TimeValue backgroundPersistInterval,
                Long modelSnapshotRetentionDays, Long dailyModelSnapshotRetentionAfterDays, Long resultsRetentionDays,
                Map<String, Object> customSettings, String modelSnapshotId, Version modelSnapshotMinVersion, String resultsIndexName,
                boolean deleting, boolean allowLazyOpen, Blocked blocked, DatafeedConfig datafeedConfig) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.jobVersion = jobVersion;
        this.groups = Collections.unmodifiableList(groups);
        this.description = description;
        this.createTime = createTime;
        this.finishedTime = finishedTime;
        this.analysisConfig = analysisConfig;
        this.analysisLimits = analysisLimits;
        this.dataDescription = dataDescription;
        this.modelPlotConfig = modelPlotConfig;
        this.renormalizationWindowDays = renormalizationWindowDays;
        this.backgroundPersistInterval = backgroundPersistInterval;
        this.modelSnapshotRetentionDays = modelSnapshotRetentionDays;
        this.dailyModelSnapshotRetentionAfterDays = dailyModelSnapshotRetentionAfterDays;
        this.resultsRetentionDays = resultsRetentionDays;
        this.customSettings = customSettings == null ? null : Collections.unmodifiableMap(customSettings);
        this.modelSnapshotId = modelSnapshotId;
        this.modelSnapshotMinVersion = modelSnapshotMinVersion;
        this.resultsIndexName = resultsIndexName;
        this.allowLazyOpen = allowLazyOpen;

        if (deleting == false && blocked.getReason() == Blocked.Reason.DELETE) {
            this.deleting = true;
        } else {
            this.deleting = deleting;
        }

        if (deleting && blocked.getReason() != Blocked.Reason.DELETE) {
            this.blocked = new Blocked(Blocked.Reason.DELETE, null);
        } else {
            this.blocked = blocked;
        }
        this.datafeedConfig = datafeedConfig;
    }

    public Job(StreamInput in) throws IOException {
        jobId = in.readString();
        jobType = in.readString();
        jobVersion = in.readBoolean() ? Version.readVersion(in) : null;
        groups = Collections.unmodifiableList(in.readStringList());
        description = in.readOptionalString();
        createTime = new Date(in.readVLong());
        finishedTime = in.readBoolean() ? new Date(in.readVLong()) : null;
        analysisConfig = new AnalysisConfig(in);
        analysisLimits = in.readOptionalWriteable(AnalysisLimits::new);
        dataDescription = in.readOptionalWriteable(DataDescription::new);
        modelPlotConfig = in.readOptionalWriteable(ModelPlotConfig::new);
        renormalizationWindowDays = in.readOptionalLong();
        backgroundPersistInterval = in.readOptionalTimeValue();
        modelSnapshotRetentionDays = in.readOptionalLong();
        dailyModelSnapshotRetentionAfterDays = in.readOptionalLong();
        resultsRetentionDays = in.readOptionalLong();
        Map<String, Object> readCustomSettings = in.readMap();
        customSettings = readCustomSettings == null ? null : Collections.unmodifiableMap(readCustomSettings);
        modelSnapshotId = in.readOptionalString();
        if (in.readBoolean()) {
            modelSnapshotMinVersion = Version.readVersion(in);
        } else {
            modelSnapshotMinVersion = null;
        }
        resultsIndexName = in.readString();
        deleting = in.readBoolean();
        allowLazyOpen = in.readBoolean();
        blocked = new Blocked(in);
        this.datafeedConfig = in.readOptionalWriteable(DatafeedConfig::new);
    }

    /**
     * Get the persisted job document name from the Job Id.
     * Throws if {@code jobId} is not a valid job Id.
     *
     * @param jobId The job id
     * @return The id of document the job is persisted in
     */
    public static String documentId(String jobId) {
        if (MlStrings.isValidId(jobId) == false) {
            throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_ID, ID.getPreferredName(), jobId));
        }
        if (MlStrings.hasValidLengthForId(jobId) == false) {
            throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_ID_TOO_LONG, MlStrings.ID_LENGTH_LIMIT));
        }

        return ANOMALY_DETECTOR_JOB_TYPE + "-" + jobId;
    }

    /**
     * Returns the job id from the doc id. Returns {@code null} if the doc id is invalid.
     */
    @Nullable
    public static String extractJobIdFromDocumentId(String docId) {
        String jobId = docId.replaceAll("^" + ANOMALY_DETECTOR_JOB_TYPE +"-", "");
        return jobId.equals(docId) ? null : jobId;
    }

    /**
     * Return the Job Id.
     *
     * @return The job Id string
     */
    public String getId() {
        return jobId;
    }

    public String getJobType() {
        return jobType;
    }

    public Version getJobVersion() {
        return jobVersion;
    }

    public List<String> getGroups() {
        return groups;
    }

    /**
     * A good starting name for the index storing the job's results.
     * This defaults to the shared results index if a specific index name is not set.
     * This method must <em>only</em> be used during initial job creation.
     * After that the read/write aliases must always be used to access the job's
     * results index, as the underlying index may roll or be reindexed.
     * @return The job's initial results index name
     */
    public String getInitialResultsIndexName() {
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + resultsIndexName;
    }

    /**
     * Get the unmodified <code>results_index_name</code> field from the job.
     * This is provided to allow a job to be copied via the builder.
     * After creation this does not necessarily reflect the actual concrete
     * index used by the job.  A job's results must always be read and written
     * using the read and write aliases.
     * @return The job's configured "index name"
     */
    private String getResultsIndexNameNoPrefix() {
        return resultsIndexName;
    }

    /**
     * The job description
     *
     * @return job description
     */
    public String getDescription() {
        return description;
    }

    /**
     * The Job creation time. This name is preferred when serialising to the
     * REST API.
     *
     * @return The date the job was created
     */
    public Date getCreateTime() {
        return createTime;
    }

    /**
     * The time the job was finished or <code>null</code> if not finished.
     *
     * @return The date the job was last retired or <code>null</code>
     */
    public Date getFinishedTime() {
        return finishedTime;
    }

    /**
     * The analysis configuration object
     *
     * @return The AnalysisConfig
     */
    public AnalysisConfig getAnalysisConfig() {
        return analysisConfig;
    }

    /**
     * The analysis options object
     *
     * @return The AnalysisLimits
     */
    public AnalysisLimits getAnalysisLimits() {
        return analysisLimits;
    }

    public ModelPlotConfig getModelPlotConfig() {
        return modelPlotConfig;
    }

    /**
     * If not set the input data is assumed to be csv with a '_time' field in
     * epoch format.
     *
     * @return A DataDescription or <code>null</code>
     * @see DataDescription
     */
    public DataDescription getDataDescription() {
        return dataDescription;
    }

    /**
     * The duration of the renormalization window in days
     *
     * @return renormalization window in days
     */
    public Long getRenormalizationWindowDays() {
        return renormalizationWindowDays;
    }

    /**
     * The background persistence interval
     *
     * @return background persistence interval
     */
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

    public Map<String, Object> getCustomSettings() {
        return customSettings;
    }

    public String getModelSnapshotId() {
        return modelSnapshotId;
    }

    public Version getModelSnapshotMinVersion() {
        return modelSnapshotMinVersion;
    }

    public boolean isDeleting() {
        return deleting;
    }

    public boolean allowLazyOpen() {
        return allowLazyOpen;
    }

    public Blocked getBlocked() {
        return blocked;
    }

    /**
     * Get all input data fields mentioned in the job configuration,
     * namely analysis fields and the time field.
     *
     * @return the collection of fields - never <code>null</code>
     */
    public Collection<String> allInputFields() {
        Set<String> allFields = new TreeSet<>();

        // analysis fields
        if (analysisConfig != null) {
            allFields.addAll(analysisConfig.analysisFields());
        }

        // time field
        if (dataDescription != null) {
            String timeField = dataDescription.getTimeField();
            if (timeField != null) {
                allFields.add(timeField);
            }
        }

        // remove empty strings
        allFields.remove("");

        // the categorisation field isn't an input field
        allFields.remove(AnalysisConfig.ML_CATEGORY_FIELD);

        return allFields;
    }

    /**
     * Returns the timestamp before which data is not accepted by the job.
     * This is the latest record timestamp minus the job latency.
     * @param dataCounts the job data counts
     * @return the timestamp before which data is not accepted by the job
     */
    public long earliestValidTimestamp(DataCounts dataCounts) {
        long currentTime = 0;
        Date latestRecordTimestamp = dataCounts.getLatestRecordTimeStamp();
        if (latestRecordTimestamp != null) {
            TimeValue latency = analysisConfig.getLatency();
            long latencyMillis = latency == null ? 0 : latency.millis();
            currentTime = latestRecordTimestamp.getTime() - latencyMillis;
        }
        return currentTime;
    }

    public Optional<DatafeedConfig> getDatafeedConfig() {
        return Optional.ofNullable(datafeedConfig);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeString(jobType);
        if (jobVersion != null) {
            out.writeBoolean(true);
            Version.writeVersion(jobVersion, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeStringCollection(groups);
        out.writeOptionalString(description);
        out.writeVLong(createTime.getTime());
        if (finishedTime != null) {
            out.writeBoolean(true);
            out.writeVLong(finishedTime.getTime());
        } else {
            out.writeBoolean(false);
        }
        analysisConfig.writeTo(out);
        out.writeOptionalWriteable(analysisLimits);
        out.writeOptionalWriteable(dataDescription);
        out.writeOptionalWriteable(modelPlotConfig);
        out.writeOptionalLong(renormalizationWindowDays);
        out.writeOptionalTimeValue(backgroundPersistInterval);
        out.writeOptionalLong(modelSnapshotRetentionDays);
        out.writeOptionalLong(dailyModelSnapshotRetentionAfterDays);
        out.writeOptionalLong(resultsRetentionDays);
        out.writeMap(customSettings);
        out.writeOptionalString(modelSnapshotId);
        if (modelSnapshotMinVersion != null) {
            out.writeBoolean(true);
            Version.writeVersion(modelSnapshotMinVersion, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeString(resultsIndexName);
        out.writeBoolean(deleting);
        out.writeBoolean(allowLazyOpen);
        blocked.writeTo(out);
        out.writeOptionalWriteable(datafeedConfig);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final String humanReadableSuffix = "_string";
        builder.field(ID.getPreferredName(), jobId);
        if (params.paramAsBoolean(EXCLUDE_GENERATED, false) == false) {
            builder.field(JOB_TYPE.getPreferredName(), jobType);
            if (jobVersion != null) {
                builder.field(JOB_VERSION.getPreferredName(), jobVersion);
            }
            builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + humanReadableSuffix, createTime.getTime());
            if (finishedTime != null) {
                builder.timeField(FINISHED_TIME.getPreferredName(), FINISHED_TIME.getPreferredName() + humanReadableSuffix,
                    finishedTime.getTime());
            }
            if (modelSnapshotId != null) {
                builder.field(MODEL_SNAPSHOT_ID.getPreferredName(), modelSnapshotId);
            }
            if (deleting) {
                builder.field(DELETING.getPreferredName(), deleting);
            }
            if (modelSnapshotMinVersion != null) {
                builder.field(MODEL_SNAPSHOT_MIN_VERSION.getPreferredName(), modelSnapshotMinVersion);
            }
            if (customSettings != null) {
                builder.field(CUSTOM_SETTINGS.getPreferredName(), customSettings);
            }
            //TODO in v8.0.0 move this out so that it will be included when `exclude_generated` is `true`
            if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false) == false) {
                if (datafeedConfig != null) {
                    builder.field(DATAFEED_CONFIG.getPreferredName(), datafeedConfig, params);
                }
            }
        } else {
            if (customSettings != null) {
                HashMap<String, Object> newCustomSettings = new HashMap<>(customSettings);
                newCustomSettings.remove("created_by");
                builder.field(CUSTOM_SETTINGS.getPreferredName(), newCustomSettings);
            }
        }
        if (groups.isEmpty() == false) {
            builder.field(GROUPS.getPreferredName(), groups);
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.field(ANALYSIS_CONFIG.getPreferredName(), analysisConfig, params);
        if (analysisLimits != null) {
            builder.field(ANALYSIS_LIMITS.getPreferredName(), analysisLimits, params);
        }
        if (dataDescription != null) {
            builder.field(DATA_DESCRIPTION.getPreferredName(), dataDescription, params);
        }
        if (modelPlotConfig != null) {
            builder.field(MODEL_PLOT_CONFIG.getPreferredName(), modelPlotConfig, params);
        }
        if (renormalizationWindowDays != null) {
            builder.field(RENORMALIZATION_WINDOW_DAYS.getPreferredName(), renormalizationWindowDays);
        }
        if (backgroundPersistInterval != null) {
            builder.field(BACKGROUND_PERSIST_INTERVAL.getPreferredName(), backgroundPersistInterval.getStringRep());
        }
        if (modelSnapshotRetentionDays != null) {
            builder.field(MODEL_SNAPSHOT_RETENTION_DAYS.getPreferredName(), modelSnapshotRetentionDays);
        }
        if (dailyModelSnapshotRetentionAfterDays != null) {
            builder.field(DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS.getPreferredName(), dailyModelSnapshotRetentionAfterDays);
        }
        if (resultsRetentionDays != null) {
            builder.field(RESULTS_RETENTION_DAYS.getPreferredName(), resultsRetentionDays);
        }
        builder.field(RESULTS_INDEX_NAME.getPreferredName(), resultsIndexName);
        builder.field(ALLOW_LAZY_OPEN.getPreferredName(), allowLazyOpen);
        if (blocked.getReason() != Blocked.Reason.NONE) {
            builder.field(BLOCKED.getPreferredName(), blocked);
        }
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof Job == false) {
            return false;
        }

        Job that = (Job) other;
        return Objects.equals(this.jobId, that.jobId)
                && Objects.equals(this.jobType, that.jobType)
                && Objects.equals(this.jobVersion, that.jobVersion)
                && Objects.equals(this.groups, that.groups)
                && Objects.equals(this.description, that.description)
                && Objects.equals(this.createTime, that.createTime)
                && Objects.equals(this.finishedTime, that.finishedTime)
                && Objects.equals(this.analysisConfig, that.analysisConfig)
                && Objects.equals(this.analysisLimits, that.analysisLimits)
                && Objects.equals(this.dataDescription, that.dataDescription)
                && Objects.equals(this.modelPlotConfig, that.modelPlotConfig)
                && Objects.equals(this.renormalizationWindowDays, that.renormalizationWindowDays)
                && Objects.equals(this.backgroundPersistInterval, that.backgroundPersistInterval)
                && Objects.equals(this.modelSnapshotRetentionDays, that.modelSnapshotRetentionDays)
                && Objects.equals(this.dailyModelSnapshotRetentionAfterDays, that.dailyModelSnapshotRetentionAfterDays)
                && Objects.equals(this.resultsRetentionDays, that.resultsRetentionDays)
                && Objects.equals(this.customSettings, that.customSettings)
                && Objects.equals(this.modelSnapshotId, that.modelSnapshotId)
                && Objects.equals(this.modelSnapshotMinVersion, that.modelSnapshotMinVersion)
                && Objects.equals(this.resultsIndexName, that.resultsIndexName)
                && Objects.equals(this.deleting, that.deleting)
                && Objects.equals(this.allowLazyOpen, that.allowLazyOpen)
                && Objects.equals(this.blocked, that.blocked)
                && Objects.equals(this.datafeedConfig, that.datafeedConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobType, jobVersion, groups, description, createTime, finishedTime,
            analysisConfig, analysisLimits, dataDescription, modelPlotConfig, renormalizationWindowDays,
            backgroundPersistInterval, modelSnapshotRetentionDays, dailyModelSnapshotRetentionAfterDays, resultsRetentionDays,
            customSettings, modelSnapshotId, modelSnapshotMinVersion, resultsIndexName, deleting, allowLazyOpen, blocked,
            datafeedConfig);
    }

    // Class already extends from AbstractDiffable, so copied from ToXContentToBytes#toString()
    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    private static void checkValueNotLessThan(long minVal, String name, Long value) {
        if (value != null && value < minVal) {
            throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW, name, minVal, value));
        }
    }

    /**
     * Returns the job types that are compatible with a node running on {@code nodeVersion}
     * @param nodeVersion the version of the node
     * @return the compatible job types
     */
    public static Set<String> getCompatibleJobTypes(Version nodeVersion) {
        Set<String> compatibleTypes = new HashSet<>();
        compatibleTypes.add(ANOMALY_DETECTOR_JOB_TYPE);
        return compatibleTypes;
    }

    public static class Builder implements Writeable {

        private String id;
        private String jobType = ANOMALY_DETECTOR_JOB_TYPE;
        private Version jobVersion;
        private List<String> groups = Collections.emptyList();
        private String description;
        private AnalysisConfig analysisConfig;
        private AnalysisLimits analysisLimits;
        private DataDescription dataDescription;
        private Date createTime;
        private Date finishedTime;
        private ModelPlotConfig modelPlotConfig;
        private Long renormalizationWindowDays;
        private TimeValue backgroundPersistInterval;
        private Long modelSnapshotRetentionDays = DEFAULT_MODEL_SNAPSHOT_RETENTION_DAYS;
        private Long dailyModelSnapshotRetentionAfterDays;
        private Long resultsRetentionDays;
        private Map<String, Object> customSettings;
        private String modelSnapshotId;
        private Version modelSnapshotMinVersion;
        private String resultsIndexName;
        private boolean deleting;
        private boolean allowLazyOpen;
        private Blocked blocked = Blocked.none();
        private DatafeedConfig.Builder datafeedConfig;

        public Builder() {
        }

        public Builder(String id) {
            this.id = id;
        }

        public Builder(Job job) {
            this.id = job.getId();
            this.jobType = job.getJobType();
            this.jobVersion = job.getJobVersion();
            this.groups = new ArrayList<>(job.getGroups());
            this.description = job.getDescription();
            this.analysisConfig = job.getAnalysisConfig();
            this.analysisLimits = job.getAnalysisLimits();
            this.dataDescription = job.getDataDescription();
            this.createTime = job.getCreateTime();
            this.finishedTime = job.getFinishedTime();
            this.modelPlotConfig = job.getModelPlotConfig();
            this.renormalizationWindowDays = job.getRenormalizationWindowDays();
            this.backgroundPersistInterval = job.getBackgroundPersistInterval();
            this.modelSnapshotRetentionDays = job.getModelSnapshotRetentionDays();
            this.dailyModelSnapshotRetentionAfterDays = job.getDailyModelSnapshotRetentionAfterDays();
            this.resultsRetentionDays = job.getResultsRetentionDays();
            this.customSettings = job.getCustomSettings() == null ? null : new LinkedHashMap<>(job.getCustomSettings());
            this.modelSnapshotId = job.getModelSnapshotId();
            this.modelSnapshotMinVersion = job.getModelSnapshotMinVersion();
            this.resultsIndexName = job.getResultsIndexNameNoPrefix();
            this.deleting = job.isDeleting();
            this.allowLazyOpen = job.allowLazyOpen();
            this.blocked = job.getBlocked();
            this.datafeedConfig = job.getDatafeedConfig().isPresent() ? new DatafeedConfig.Builder(job.datafeedConfig) : null;
        }

        public Builder(StreamInput in) throws IOException {
            id = in.readOptionalString();
            jobType = in.readString();
            jobVersion = in.readBoolean() ? Version.readVersion(in) : null;
            groups = in.readStringList();
            description = in.readOptionalString();
            createTime = in.readBoolean() ? new Date(in.readVLong()) : null;
            finishedTime = in.readBoolean() ? new Date(in.readVLong()) : null;
            analysisConfig = in.readOptionalWriteable(AnalysisConfig::new);
            analysisLimits = in.readOptionalWriteable(AnalysisLimits::new);
            dataDescription = in.readOptionalWriteable(DataDescription::new);
            modelPlotConfig = in.readOptionalWriteable(ModelPlotConfig::new);
            renormalizationWindowDays = in.readOptionalLong();
            backgroundPersistInterval = in.readOptionalTimeValue();
            modelSnapshotRetentionDays = in.readOptionalLong();
            dailyModelSnapshotRetentionAfterDays = in.readOptionalLong();
            resultsRetentionDays = in.readOptionalLong();
            customSettings = in.readMap();
            modelSnapshotId = in.readOptionalString();
            if (in.readBoolean()) {
                modelSnapshotMinVersion = Version.readVersion(in);
            } else {
                modelSnapshotMinVersion = null;
            }
            resultsIndexName = in.readOptionalString();
            deleting = in.readBoolean();
            allowLazyOpen = in.readBoolean();
            blocked = new Blocked(in);
            datafeedConfig = in.readOptionalWriteable(DatafeedConfig.Builder::new);
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public String getId() {
            return id;
        }

        public void setJobVersion(Version jobVersion) {
            this.jobVersion = jobVersion;
        }

        private void setJobVersion(String jobVersion) {
            this.jobVersion = Version.fromString(jobVersion);
        }

        private void setJobType(String jobType) {
            this.jobType = jobType;
        }

        public void setGroups(List<String> groups) {
            this.groups = groups == null ? Collections.emptyList() : groups;
        }

        public List<String> getGroups() {
            return groups;
        }

        public Builder setCustomSettings(Map<String, Object> customSettings) {
            this.customSettings = customSettings;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setAnalysisConfig(AnalysisConfig.Builder configBuilder) {
            analysisConfig = ExceptionsHelper.requireNonNull(configBuilder, ANALYSIS_CONFIG.getPreferredName()).build();
            return this;
        }

        public AnalysisConfig getAnalysisConfig() {
             return analysisConfig;
        }

        public Builder setAnalysisLimits(AnalysisLimits analysisLimits) {
            this.analysisLimits = ExceptionsHelper.requireNonNull(analysisLimits, ANALYSIS_LIMITS.getPreferredName());
            return this;
        }

        public Builder setCreateTime(Date createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setFinishedTime(Date finishedTime) {
            this.finishedTime = finishedTime;
            return this;
        }

        public Builder setDataDescription(DataDescription.Builder description) {
            dataDescription = ExceptionsHelper.requireNonNull(description, DATA_DESCRIPTION.getPreferredName()).build();
            return this;
        }

        public Builder setModelPlotConfig(ModelPlotConfig modelPlotConfig) {
            this.modelPlotConfig = modelPlotConfig;
            return this;
        }

        public Builder setBackgroundPersistInterval(TimeValue backgroundPersistInterval) {
            this.backgroundPersistInterval = backgroundPersistInterval;
            return this;
        }

        public Builder setRenormalizationWindowDays(Long renormalizationWindowDays) {
            this.renormalizationWindowDays = renormalizationWindowDays;
            return this;
        }

        public Builder setModelSnapshotRetentionDays(Long modelSnapshotRetentionDays) {
            this.modelSnapshotRetentionDays = modelSnapshotRetentionDays;
            return this;
        }

        public Long getModelSnapshotRetentionDays() {
            return modelSnapshotRetentionDays;
        }

        public Builder setDailyModelSnapshotRetentionAfterDays(Long dailyModelSnapshotRetentionAfterDays) {
            this.dailyModelSnapshotRetentionAfterDays = dailyModelSnapshotRetentionAfterDays;
            return this;
        }

        public Builder setResultsRetentionDays(Long resultsRetentionDays) {
            this.resultsRetentionDays = resultsRetentionDays;
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

        Builder setModelSnapshotMinVersion(String modelSnapshotMinVersion) {
            this.modelSnapshotMinVersion = Version.fromString(modelSnapshotMinVersion);
            return this;
        }

        public Builder setResultsIndexName(String resultsIndexName) {
            this.resultsIndexName = resultsIndexName;
            return this;
        }

        public Builder setDeleting(boolean deleting) {
            this.deleting = deleting;
            return this;
        }

        public Builder setAllowLazyOpen(boolean allowLazyOpen) {
            this.allowLazyOpen = allowLazyOpen;
            return this;
        }

        public Builder setBlocked(Blocked blocked) {
            this.blocked = ExceptionsHelper.requireNonNull(blocked, BLOCKED);
            return this;
        }

        public Builder setDatafeed(DatafeedConfig.Builder datafeed) {
            this.datafeedConfig = datafeed;
            return this;
        }

        public DatafeedConfig.Builder getDatafeedConfig() {
            return datafeedConfig;
        }

        /**
         * This is used for parsing. If the datafeed_config exists AND its indices options are `null`, we set them to these options
         *
         * @param indicesOptions To set if the datafeed indices options are null
         * @return The job builder.
         */
        public Builder setDatafeedIndicesOptionsIfRequired(IndicesOptions indicesOptions) {
            if (this.datafeedConfig != null && this.datafeedConfig.getIndicesOptions() == null) {
                this.datafeedConfig.setIndicesOptions(indicesOptions);
            }
            return this;
        }

        /**
         * Return the list of fields that have been set and are invalid to
         * be set when the job is created e.g. model snapshot Id should not
         * be set at job creation.
         * @return List of fields set fields that should not be.
         */
        public List<String> invalidCreateTimeSettings() {
            List<String> invalidCreateValues = new ArrayList<>();
            if (modelSnapshotId != null) {
                invalidCreateValues.add(MODEL_SNAPSHOT_ID.getPreferredName());
            }
            if (finishedTime != null) {
                invalidCreateValues.add(FINISHED_TIME.getPreferredName());
            }
            if (createTime != null) {
                invalidCreateValues.add(CREATE_TIME.getPreferredName());
            }
            return invalidCreateValues;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(id);
            out.writeString(jobType);
            if (jobVersion != null) {
                out.writeBoolean(true);
                Version.writeVersion(jobVersion, out);
            } else {
                out.writeBoolean(false);
            }
            out.writeStringCollection(groups);
            out.writeOptionalString(description);
            if (createTime != null) {
                out.writeBoolean(true);
                out.writeVLong(createTime.getTime());
            } else {
                out.writeBoolean(false);
            }
            if (finishedTime != null) {
                out.writeBoolean(true);
                out.writeVLong(finishedTime.getTime());
            } else {
                out.writeBoolean(false);
            }

            out.writeOptionalWriteable(analysisConfig);
            out.writeOptionalWriteable(analysisLimits);
            out.writeOptionalWriteable(dataDescription);
            out.writeOptionalWriteable(modelPlotConfig);
            out.writeOptionalLong(renormalizationWindowDays);
            out.writeOptionalTimeValue(backgroundPersistInterval);
            out.writeOptionalLong(modelSnapshotRetentionDays);
            out.writeOptionalLong(dailyModelSnapshotRetentionAfterDays);
            out.writeOptionalLong(resultsRetentionDays);
            out.writeMap(customSettings);
            out.writeOptionalString(modelSnapshotId);
            if (modelSnapshotMinVersion != null) {
                out.writeBoolean(true);
                Version.writeVersion(modelSnapshotMinVersion, out);
            } else {
                out.writeBoolean(false);
            }
            out.writeOptionalString(resultsIndexName);
            out.writeBoolean(deleting);
            out.writeBoolean(allowLazyOpen);
            blocked.writeTo(out);
            out.writeOptionalWriteable(datafeedConfig);
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Job.Builder that = (Job.Builder) o;
            return Objects.equals(this.id, that.id)
                    && Objects.equals(this.jobType, that.jobType)
                    && Objects.equals(this.jobVersion, that.jobVersion)
                    && Objects.equals(this.groups, that.groups)
                    && Objects.equals(this.description, that.description)
                    && Objects.equals(this.analysisConfig, that.analysisConfig)
                    && Objects.equals(this.analysisLimits, that.analysisLimits)
                    && Objects.equals(this.dataDescription, that.dataDescription)
                    && Objects.equals(this.createTime, that.createTime)
                    && Objects.equals(this.finishedTime, that.finishedTime)
                    && Objects.equals(this.modelPlotConfig, that.modelPlotConfig)
                    && Objects.equals(this.renormalizationWindowDays, that.renormalizationWindowDays)
                    && Objects.equals(this.backgroundPersistInterval, that.backgroundPersistInterval)
                    && Objects.equals(this.modelSnapshotRetentionDays, that.modelSnapshotRetentionDays)
                    && Objects.equals(this.dailyModelSnapshotRetentionAfterDays, that.dailyModelSnapshotRetentionAfterDays)
                    && Objects.equals(this.resultsRetentionDays, that.resultsRetentionDays)
                    && Objects.equals(this.customSettings, that.customSettings)
                    && Objects.equals(this.modelSnapshotId, that.modelSnapshotId)
                    && Objects.equals(this.modelSnapshotMinVersion, that.modelSnapshotMinVersion)
                    && Objects.equals(this.resultsIndexName, that.resultsIndexName)
                    && Objects.equals(this.deleting, that.deleting)
                    && Objects.equals(this.allowLazyOpen, that.allowLazyOpen)
                    && Objects.equals(this.blocked, that.blocked)
                    && Objects.equals(this.datafeedConfig, that.datafeedConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, jobType, jobVersion, groups, description, analysisConfig, analysisLimits, dataDescription,
                    createTime, finishedTime, modelPlotConfig, renormalizationWindowDays,
                    backgroundPersistInterval, modelSnapshotRetentionDays, dailyModelSnapshotRetentionAfterDays, resultsRetentionDays,
                    customSettings, modelSnapshotId, modelSnapshotMinVersion, resultsIndexName, deleting, allowLazyOpen, blocked,
                datafeedConfig);
        }

        /**
         * Call this method to validate that the job JSON provided by a user is valid.
         * Throws an exception if there are any problems; normal return implies valid.
         */
        public void validateInputFields() {

            if (analysisConfig == null) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_MISSING_ANALYSISCONFIG));
            }

            if (dataDescription == null) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_MISSING_DATA_DESCRIPTION));
            }

            checkTimeFieldNotInAnalysisConfig(dataDescription, analysisConfig);

            checkValidBackgroundPersistInterval();
            checkValueNotLessThan(0, RENORMALIZATION_WINDOW_DAYS.getPreferredName(), renormalizationWindowDays);
            checkValueNotLessThan(0, RESULTS_RETENTION_DAYS.getPreferredName(), resultsRetentionDays);

            if (MlStrings.isValidId(id) == false) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_ID, ID.getPreferredName(), id));
            }
            if (MlStrings.hasValidLengthForId(id) == false) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_ID_TOO_LONG, MlStrings.ID_LENGTH_LIMIT));
            }

            validateModelSnapshotRetentionSettings();

            validateGroups();

            // Results index name not specified in user input means use the default, so is acceptable in this validation
            if (Strings.isNullOrEmpty(resultsIndexName) == false && MlStrings.isValidId(resultsIndexName) == false) {
                throw new IllegalArgumentException(
                        Messages.getMessage(Messages.INVALID_ID, RESULTS_INDEX_NAME.getPreferredName(), resultsIndexName));
            }

            // Creation time is NOT required in user input, hence validated only on build
        }

        /**
         * This is meant to be called when a new job is created.
         * It will optionally validate the model memory limit against the max limit
         * and it will set the current version defaults to missing values.
         */
        public void validateAnalysisLimitsAndSetDefaults(@Nullable ByteSizeValue maxModelMemoryLimit) {
            analysisLimits = AnalysisLimits.validateAndSetDefaults(analysisLimits, maxModelMemoryLimit,
                    AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB);
        }

        /**
         * This is meant to be called when a new job is created.
         * It sets {@link #dailyModelSnapshotRetentionAfterDays} to the default value if it is not set and the default makes sense.
         */
        public void validateModelSnapshotRetentionSettingsAndSetDefaults() {
            validateModelSnapshotRetentionSettings();
            if (dailyModelSnapshotRetentionAfterDays == null &&
                modelSnapshotRetentionDays != null &&
                modelSnapshotRetentionDays > DEFAULT_DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS) {
                dailyModelSnapshotRetentionAfterDays = DEFAULT_DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS;
            }
        }

        /**
         * Validates that {@link #modelSnapshotRetentionDays} and {@link #dailyModelSnapshotRetentionAfterDays} make sense,
         * both individually and in combination.
         */
        public void validateModelSnapshotRetentionSettings() {

            checkValueNotLessThan(0, MODEL_SNAPSHOT_RETENTION_DAYS.getPreferredName(), modelSnapshotRetentionDays);
            checkValueNotLessThan(0, DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS.getPreferredName(),
                dailyModelSnapshotRetentionAfterDays);

            if (modelSnapshotRetentionDays != null &&
                dailyModelSnapshotRetentionAfterDays != null &&
                dailyModelSnapshotRetentionAfterDays > modelSnapshotRetentionDays) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_MODEL_SNAPSHOT_RETENTION_SETTINGS_INCONSISTENT,
                    dailyModelSnapshotRetentionAfterDays, modelSnapshotRetentionDays));
            }
        }

        private void validateGroups() {
            for (String group : this.groups) {
                if (MlStrings.isValidId(group) == false) {
                    throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_GROUP, group));
                }
                if (this.id.equals(group)) {
                    // cannot have a group name the same as the job id
                    throw new ResourceAlreadyExistsException(Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, group));
                }
            }
        }

        /**
         * Validates that the Detector configs are unique up to detectorIndex field (which is ignored).
         */
        public void validateDetectorsAreUnique() {
            Set<Detector> canonicalDetectors = new HashSet<>();
            for (Detector detector : this.analysisConfig.getDetectors()) {
                // While testing for equality, ignore detectorIndex field as this field is auto-generated.
                Detector canonicalDetector = new Detector.Builder(detector).setDetectorIndex(0).build();
                if (canonicalDetectors.add(canonicalDetector) == false) {
                    throw new IllegalArgumentException(
                        Messages.getMessage(Messages.JOB_CONFIG_DUPLICATE_DETECTORS_DISALLOWED, detector.getDetectorDescription()));
                }
            }
        }

        /**
         * Builds a job with the given {@code createTime} and the current version.
         * This should be used when a new job is created as opposed to {@link #build()}.
         *
         * @param createTime The time this job was created
         * @return The job
         */
        public Job build(Date createTime) {
            setCreateTime(createTime);
            setJobVersion(Version.CURRENT);
            return build();
        }

        /**
         * Builds a job.
         * This should be used when an existing job is being built
         * as opposed to {@link #build(Date)}.
         *
         * @return The job
         */
        public Job build() {

            // If at the build stage there are missing values from analysis limits,
            // it means we are reading a pre 6.3 job. Since 6.1, the model_memory_limit
            // is always populated. So, if the value is missing, we fill with the pre 6.1
            // default. We do not need to check against the max limit here so we pass null.
            analysisLimits = AnalysisLimits.validateAndSetDefaults(analysisLimits, null,
                    AnalysisLimits.PRE_6_1_DEFAULT_MODEL_MEMORY_LIMIT_MB);

            validateInputFields();

            // Creation time is NOT required in user input, hence validated only on build
            ExceptionsHelper.requireNonNull(createTime, CREATE_TIME.getPreferredName());

            if (Strings.isNullOrEmpty(resultsIndexName)) {
                resultsIndexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
            } else if (resultsIndexName.equals(AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT) == false) {
                // User-defined names are prepended with "custom"
                // Conditional guards against multiple prepending due to updates instead of first creation
                resultsIndexName = resultsIndexName.startsWith("custom-")
                        ? resultsIndexName
                        : "custom-" + resultsIndexName;
            }
            if (datafeedConfig != null) {
                if (datafeedConfig.getId() == null) {
                    datafeedConfig.setId(id);
                }
                if (datafeedConfig.getJobId() != null && datafeedConfig.getJobId().equals(id) == false) {
                    throw new IllegalArgumentException(
                        Messages.getMessage(JOB_CONFIG_DATAFEED_CONFIG_JOB_ID_MISMATCH, datafeedConfig.getJobId(), id)
                    );
                }
                datafeedConfig.setJobId(id);
            }

            return new Job(
                    id, jobType, jobVersion, groups, description, createTime, finishedTime,
                    analysisConfig, analysisLimits, dataDescription, modelPlotConfig, renormalizationWindowDays,
                    backgroundPersistInterval, modelSnapshotRetentionDays, dailyModelSnapshotRetentionAfterDays, resultsRetentionDays,
                    customSettings, modelSnapshotId, modelSnapshotMinVersion, resultsIndexName, deleting, allowLazyOpen, blocked,
                Optional.ofNullable(datafeedConfig).map(DatafeedConfig.Builder::build).orElse(null));
        }

        private void checkValidBackgroundPersistInterval() {
            if (backgroundPersistInterval != null) {
                TimeUtils.checkMultiple(backgroundPersistInterval, TimeUnit.SECONDS, BACKGROUND_PERSIST_INTERVAL);
                checkValueNotLessThan(MIN_BACKGROUND_PERSIST_INTERVAL.getSeconds(), BACKGROUND_PERSIST_INTERVAL.getPreferredName(),
                        backgroundPersistInterval.getSeconds());
            }
        }

        static void checkTimeFieldNotInAnalysisConfig(DataDescription dataDescription, AnalysisConfig analysisConfig) {
            if (analysisConfig.analysisFields().contains(dataDescription.getTimeField())) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_TIME_FIELD_NOT_ALLOWED_IN_ANALYSIS_CONFIG));
            }
        }
    }
}
