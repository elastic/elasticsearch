/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * POJO for updating an existing Machine Learning {@link Job}
 */
public class JobUpdate implements ToXContentObject {
    public static final ParseField DETECTORS = new ParseField("detectors");

    public static final ConstructingObjectParser<Builder, Void> PARSER = new ConstructingObjectParser<>(
        "job_update", true, args -> new Builder((String) args[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), Job.ID);
        PARSER.declareStringArray(Builder::setGroups, Job.GROUPS);
        PARSER.declareStringOrNull(Builder::setDescription, Job.DESCRIPTION);
        PARSER.declareObjectArray(Builder::setDetectorUpdates, DetectorUpdate.PARSER, DETECTORS);
        PARSER.declareObject(Builder::setModelPlotConfig, ModelPlotConfig.PARSER, Job.MODEL_PLOT_CONFIG);
        PARSER.declareObject(Builder::setAnalysisLimits, AnalysisLimits.PARSER, Job.ANALYSIS_LIMITS);
        PARSER.declareString((builder, val) -> builder.setBackgroundPersistInterval(
                TimeValue.parseTimeValue(val, Job.BACKGROUND_PERSIST_INTERVAL.getPreferredName())), Job.BACKGROUND_PERSIST_INTERVAL);
        PARSER.declareLong(Builder::setRenormalizationWindowDays, Job.RENORMALIZATION_WINDOW_DAYS);
        PARSER.declareLong(Builder::setResultsRetentionDays, Job.RESULTS_RETENTION_DAYS);
        PARSER.declareLong(Builder::setModelSnapshotRetentionDays, Job.MODEL_SNAPSHOT_RETENTION_DAYS);
        PARSER.declareLong(Builder::setDailyModelSnapshotRetentionAfterDays, Job.DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS);
        PARSER.declareStringArray(Builder::setCategorizationFilters, AnalysisConfig.CATEGORIZATION_FILTERS);
        PARSER.declareField(Builder::setCustomSettings, (p, c) -> p.map(), Job.CUSTOM_SETTINGS, ObjectParser.ValueType.OBJECT);
        PARSER.declareBoolean(Builder::setAllowLazyOpen, Job.ALLOW_LAZY_OPEN);
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
    private final List<String> categorizationFilters;
    private final Map<String, Object> customSettings;
    private final Boolean allowLazyOpen;

    private JobUpdate(String jobId, @Nullable List<String> groups, @Nullable String description,
                      @Nullable List<DetectorUpdate> detectorUpdates, @Nullable ModelPlotConfig modelPlotConfig,
                      @Nullable AnalysisLimits analysisLimits, @Nullable TimeValue backgroundPersistInterval,
                      @Nullable Long renormalizationWindowDays, @Nullable Long resultsRetentionDays,
                      @Nullable Long modelSnapshotRetentionDays, @Nullable Long dailyModelSnapshotRetentionAfterDays,
                      @Nullable List<String> categorizationFilters,
                      @Nullable Map<String, Object> customSettings, @Nullable Boolean allowLazyOpen) {
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
        this.categorizationFilters = categorizationFilters;
        this.customSettings = customSettings;
        this.allowLazyOpen = allowLazyOpen;
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

    public Long getResultsRetentionDays() {
        return resultsRetentionDays;
    }

    public List<String> getCategorizationFilters() {
        return categorizationFilters;
    }

    public Map<String, Object> getCustomSettings() {
        return customSettings;
    }

    public Boolean getAllowLazyOpen() {
        return allowLazyOpen;
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
        if (categorizationFilters != null) {
            builder.field(AnalysisConfig.CATEGORIZATION_FILTERS.getPreferredName(), categorizationFilters);
        }
        if (customSettings != null) {
            builder.field(Job.CUSTOM_SETTINGS.getPreferredName(), customSettings);
        }
        if (allowLazyOpen != null) {
            builder.field(Job.ALLOW_LAZY_OPEN.getPreferredName(), allowLazyOpen);
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
            && Objects.equals(this.categorizationFilters, that.categorizationFilters)
            && Objects.equals(this.customSettings, that.customSettings)
            && Objects.equals(this.allowLazyOpen, that.allowLazyOpen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, groups, description, detectorUpdates, modelPlotConfig, analysisLimits, renormalizationWindowDays,
            backgroundPersistInterval, modelSnapshotRetentionDays, dailyModelSnapshotRetentionAfterDays, resultsRetentionDays,
            categorizationFilters, customSettings, allowLazyOpen);
    }

    public static class DetectorUpdate implements ToXContentObject {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DetectorUpdate, Void> PARSER =
            new ConstructingObjectParser<>("detector_update", true, a -> new DetectorUpdate((int) a[0], (String) a[1],
                (List<DetectionRule>) a[2]));

        static {
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), Detector.DETECTOR_INDEX);
            PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), Job.DESCRIPTION);
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (parser, parseFieldMatcher) ->
                DetectionRule.PARSER.apply(parser, parseFieldMatcher).build(), Detector.CUSTOM_RULES_FIELD);
        }

        private final int detectorIndex;
        private final String description;
        private final List<DetectionRule> rules;

        /**
         * A detector update to apply to the Machine Learning Job
         *
         * @param detectorIndex The identifier of the detector to update.
         * @param description The new description for the detector.
         * @param rules The new list of rules for the detector.
         */
        public DetectorUpdate(int detectorIndex, String description, List<DetectionRule> rules) {
            this.detectorIndex = detectorIndex;
            this.description = description;
            this.rules = rules;
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

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            DetectorUpdate that = (DetectorUpdate) other;
            return this.detectorIndex == that.detectorIndex && Objects.equals(this.description, that.description)
                && Objects.equals(this.rules, that.rules);
        }
    }

    public static class Builder {

        private final String jobId;
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
        private List<String> categorizationFilters;
        private Map<String, Object> customSettings;
        private Boolean allowLazyOpen;

        /**
         * New {@link JobUpdate.Builder} object for the existing job
         *
         * @param jobId non-null `jobId` for referencing an exising {@link Job}
         */
        public Builder(String jobId) {
            this.jobId = jobId;
        }

        /**
         * Set the job groups
         *
         * Updates the {@link Job#groups} setting
         *
         * @param groups A list of group names
         */
        public Builder setGroups(List<String> groups) {
            this.groups = groups;
            return this;
        }

        /**
         * Set the job description
         *
         * Updates the {@link Job#description} setting
         *
         * @param description the desired Machine Learning job description
         */
        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        /**
         * The detector updates to apply to the job
         *
         * Updates the {@link AnalysisConfig#detectors} setting
         *
         * @param detectorUpdates list of {@link JobUpdate.DetectorUpdate} objects
         */
        public Builder setDetectorUpdates(List<DetectorUpdate> detectorUpdates) {
            this.detectorUpdates = detectorUpdates;
            return this;
        }

        /**
         * Enables/disables the model plot config setting through {@link ModelPlotConfig#enabled}
         *
         * Updates the {@link Job#modelPlotConfig} setting
         *
         * @param modelPlotConfig {@link ModelPlotConfig} object with updated fields
         */
        public Builder setModelPlotConfig(ModelPlotConfig modelPlotConfig) {
            this.modelPlotConfig = modelPlotConfig;
            return this;
        }

        /**
         * Sets new {@link AnalysisLimits} for the {@link Job}
         *
         * Updates the {@link Job#analysisLimits} setting
         *
         * @param analysisLimits Updates to {@link AnalysisLimits}
         */
        public Builder setAnalysisLimits(AnalysisLimits analysisLimits) {
            this.analysisLimits = analysisLimits;
            return this;
        }

        /**
         * Advanced configuration option. The period over which adjustments to the score are applied, as new data is seen
         *
         * Updates the {@link Job#renormalizationWindowDays} setting
         *
         * @param renormalizationWindowDays number of renormalization window days
         */
        public Builder setRenormalizationWindowDays(Long renormalizationWindowDays) {
            this.renormalizationWindowDays = renormalizationWindowDays;
            return this;
        }

        /**
         * Advanced configuration option. The time between each periodic persistence of the model
         *
         * Updates the {@link Job#backgroundPersistInterval} setting
         *
         * @param backgroundPersistInterval the time between background persistence
         */
        public Builder setBackgroundPersistInterval(TimeValue backgroundPersistInterval) {
            this.backgroundPersistInterval = backgroundPersistInterval;
            return this;
        }

        /**
         * The time in days that model snapshots are retained for the job.
         *
         * Updates the {@link Job#modelSnapshotRetentionDays} setting
         *
         * @param modelSnapshotRetentionDays number of days to keep a model snapshot
         */
        public Builder setModelSnapshotRetentionDays(Long modelSnapshotRetentionDays) {
            this.modelSnapshotRetentionDays = modelSnapshotRetentionDays;
            return this;
        }

        /**
         * The time in days after which only one model snapshot per day is retained for the job.
         *
         * Updates the {@link Job#dailyModelSnapshotRetentionAfterDays} setting
         *
         * @param dailyModelSnapshotRetentionAfterDays number of days to keep a model snapshot
         */
        public Builder setDailyModelSnapshotRetentionAfterDays(Long dailyModelSnapshotRetentionAfterDays) {
            this.dailyModelSnapshotRetentionAfterDays = dailyModelSnapshotRetentionAfterDays;
            return this;
        }

        /**
         * Advanced configuration option. The number of days for which job results are retained
         *
         * Updates the {@link Job#resultsRetentionDays} setting
         *
         * @param resultsRetentionDays number of days to keep results.
         */
        public Builder setResultsRetentionDays(Long resultsRetentionDays) {
            this.resultsRetentionDays = resultsRetentionDays;
            return this;
        }

        /**
         * Sets the categorization filters on the {@link Job}
         *
         * Updates the {@link AnalysisConfig#categorizationFilters} setting.
         * Requires {@link AnalysisConfig#categorizationFieldName} to have been set on the existing Job.
         *
         * @param categorizationFilters list of categorization filters for the Job's {@link AnalysisConfig}
         */
        public Builder setCategorizationFilters(List<String> categorizationFilters) {
            this.categorizationFilters = categorizationFilters;
            return this;
        }

        /**
         * Contains custom meta data about the job.
         *
         * Updates the {@link Job#customSettings} setting
         *
         * @param customSettings custom settings map for the job
         */
        public Builder setCustomSettings(Map<String, Object> customSettings) {
            this.customSettings = customSettings;
            return this;
        }

        public Builder setAllowLazyOpen(boolean allowLazyOpen) {
            this.allowLazyOpen = allowLazyOpen;
            return this;
        }

        public JobUpdate build() {
            return new JobUpdate(jobId, groups, description, detectorUpdates, modelPlotConfig, analysisLimits, backgroundPersistInterval,
                renormalizationWindowDays, resultsRetentionDays, modelSnapshotRetentionDays, dailyModelSnapshotRetentionAfterDays,
                categorizationFilters, customSettings, allowLazyOpen);
        }
    }
}
