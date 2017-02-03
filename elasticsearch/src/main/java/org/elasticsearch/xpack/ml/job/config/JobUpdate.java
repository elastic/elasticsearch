/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JobUpdate implements Writeable, ToXContent {
    public static final ParseField DETECTORS = new ParseField("detectors");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<JobUpdate, Void> PARSER =
            new ConstructingObjectParser<>("job_update", a -> new JobUpdate((String) a[0], (List<DetectorUpdate>) a[1],
                    (ModelDebugConfig) a[2], (AnalysisLimits) a[3], (Long) a[4], (Long) a[5], (Long) a[6], (Long) a[7],
                    (List<String>) a[8], (Map<String, Object>) a[9]));


    static {
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), Job.DESCRIPTION);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), DetectorUpdate.PARSER, DETECTORS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelDebugConfig.PARSER, Job.MODEL_DEBUG_CONFIG);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), AnalysisLimits.PARSER, Job.ANALYSIS_LIMITS);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), Job.BACKGROUND_PERSIST_INTERVAL);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), Job.RENORMALIZATION_WINDOW_DAYS);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), Job.RESULTS_RETENTION_DAYS);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), Job.MODEL_SNAPSHOT_RETENTION_DAYS);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), AnalysisConfig.CATEGORIZATION_FILTERS);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), Job.CUSTOM_SETTINGS,
                ObjectParser.ValueType.OBJECT);
    }

    private final String description;
    private final List<DetectorUpdate> detectorUpdates;
    private final ModelDebugConfig modelDebugConfig;
    private final AnalysisLimits analysisLimits;
    private final Long renormalizationWindowDays;
    private final Long backgroundPersistInterval;
    private final Long modelSnapshotRetentionDays;
    private final Long resultsRetentionDays;
    private final List<String> categorizationFilters;
    private final Map<String, Object> customSettings;

    public JobUpdate(@Nullable String description, @Nullable List<DetectorUpdate> detectorUpdates,
                     @Nullable ModelDebugConfig modelDebugConfig, @Nullable AnalysisLimits analysisLimits,
                     @Nullable Long backgroundPersistInterval, @Nullable Long renormalizationWindowDays,
                     @Nullable Long resultsRetentionDays, @Nullable Long modelSnapshotRetentionDays,
                     @Nullable List<String> categorisationFilters, @Nullable  Map<String, Object> customSettings) {
        this.description = description;
        this.detectorUpdates = detectorUpdates;
        this.modelDebugConfig = modelDebugConfig;
        this.analysisLimits = analysisLimits;
        this.renormalizationWindowDays = renormalizationWindowDays;
        this.backgroundPersistInterval = backgroundPersistInterval;
        this.modelSnapshotRetentionDays = modelSnapshotRetentionDays;
        this.resultsRetentionDays = resultsRetentionDays;
        this.categorizationFilters = categorisationFilters;
        this.customSettings = customSettings;
    }

    public JobUpdate(StreamInput in) throws IOException {
        description = in.readOptionalString();
        if (in.readBoolean()) {
            detectorUpdates = in.readList(DetectorUpdate::new);
        } else {
            detectorUpdates = null;
        }
        modelDebugConfig = in.readOptionalWriteable(ModelDebugConfig::new);
        analysisLimits = in.readOptionalWriteable(AnalysisLimits::new);
        renormalizationWindowDays = in.readOptionalLong();
        backgroundPersistInterval = in.readOptionalLong();
        modelSnapshotRetentionDays = in.readOptionalLong();
        resultsRetentionDays = in.readOptionalLong();
        if (in.readBoolean()) {
            categorizationFilters = in.readList(StreamInput::readString);
        } else {
            categorizationFilters = null;
        }
        customSettings = in.readMap();
    }
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(description);
        out.writeBoolean(detectorUpdates != null);
        if (detectorUpdates != null) {
            out.writeList(detectorUpdates);
        }
        out.writeOptionalWriteable(modelDebugConfig);
        out.writeOptionalWriteable(analysisLimits);
        out.writeOptionalLong(renormalizationWindowDays);
        out.writeOptionalLong(backgroundPersistInterval);
        out.writeOptionalLong(modelSnapshotRetentionDays);
        out.writeOptionalLong(resultsRetentionDays);
        out.writeBoolean(categorizationFilters != null);
        if (categorizationFilters != null) {
            out.writeStringList(categorizationFilters);
        }
        out.writeMap(customSettings);
    }

    public String getDescription() {
        return description;
    }

    public List<DetectorUpdate> getDetectorUpdates() {
        return detectorUpdates;
    }

    public ModelDebugConfig getModelDebugConfig() {
        return modelDebugConfig;
    }

    public AnalysisLimits getAnalysisLimits() {
        return analysisLimits;
    }

    public Long getRenormalizationWindowDays() {
        return renormalizationWindowDays;
    }

    public Long getBackgroundPersistInterval() {
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (description != null) {
            builder.field(Job.DESCRIPTION.getPreferredName(), description);
        }
        if (detectorUpdates != null) {
            builder.field(DETECTORS.getPreferredName(), detectorUpdates);
        }
        if (modelDebugConfig != null) {
            builder.field(Job.MODEL_DEBUG_CONFIG.getPreferredName(), modelDebugConfig);
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
        if (resultsRetentionDays != null) {
            builder.field(Job.RESULTS_RETENTION_DAYS.getPreferredName(), resultsRetentionDays);
        }
        if (categorizationFilters != null) {
            builder.field(AnalysisConfig.CATEGORIZATION_FILTERS.getPreferredName(), categorizationFilters);
        }
        if (customSettings != null) {
            builder.field(Job.CUSTOM_SETTINGS.getPreferredName(), customSettings);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Updates {@code source} with the new values in this object returning a new {@link Job}.
     *
     * @param source Source job to be updated
     * @return A new job equivalent to {@code source} updated.
     */
    public Job mergeWithJob(Job source) {
        Job.Builder builder = new Job.Builder(source);
        if (description != null) {
            builder.setDescription(description);
        }
        if (detectorUpdates != null && detectorUpdates.isEmpty() == false) {
            AnalysisConfig ac = source.getAnalysisConfig();
            int numDetectors = ac.getDetectors().size();
            for (DetectorUpdate dd : detectorUpdates) {
                if (dd.getIndex() >= numDetectors) {
                    throw new IllegalArgumentException("Detector index is >= the number of detectors");
                }

                Detector.Builder detectorbuilder = new Detector.Builder(ac.getDetectors().get(dd.getIndex()));
                if (dd.getDescription() != null) {
                    detectorbuilder.setDetectorDescription(dd.getDescription());
                }
                if (dd.getRules() != null) {
                    detectorbuilder.setDetectorRules(dd.getRules());
                }
                ac.getDetectors().set(dd.getIndex(), detectorbuilder.build());
            }

            AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(ac);
            builder.setAnalysisConfig(acBuilder);
        }
        if (modelDebugConfig != null) {
            builder.setModelDebugConfig(modelDebugConfig);
        }
        if (analysisLimits != null) {
            builder.setAnalysisLimits(analysisLimits);
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
        if (resultsRetentionDays != null) {
            builder.setResultsRetentionDays(resultsRetentionDays);
        }
        if (categorizationFilters != null) {
            AnalysisConfig.Builder analysisConfigBuilder = new AnalysisConfig.Builder(source.getAnalysisConfig());
            analysisConfigBuilder.setCategorizationFilters(categorizationFilters);
            builder.setAnalysisConfig(analysisConfigBuilder);
        }
        if (customSettings != null) {
            builder.setCustomSettings(customSettings);
        }

        return builder.build();
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

        return Objects.equals(this.description, that.description)
                && Objects.equals(this.detectorUpdates, that.detectorUpdates)
                && Objects.equals(this.modelDebugConfig, that.modelDebugConfig)
                && Objects.equals(this.analysisLimits, that.analysisLimits)
                && Objects.equals(this.renormalizationWindowDays, that.renormalizationWindowDays)
                && Objects.equals(this.backgroundPersistInterval, that.backgroundPersistInterval)
                && Objects.equals(this.modelSnapshotRetentionDays, that.modelSnapshotRetentionDays)
                && Objects.equals(this.resultsRetentionDays, that.resultsRetentionDays)
                && Objects.equals(this.categorizationFilters, that.categorizationFilters)
                && Objects.equals(this.customSettings, that.customSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, detectorUpdates, modelDebugConfig, analysisLimits, renormalizationWindowDays,
                backgroundPersistInterval, modelSnapshotRetentionDays, resultsRetentionDays, categorizationFilters, customSettings);
    }

    public static class DetectorUpdate implements Writeable, ToXContent {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DetectorUpdate, Void> PARSER =
                new ConstructingObjectParser<>("detector_update", a -> new DetectorUpdate((int) a[0], (String) a[1],
                        (List<DetectionRule>) a[2]));

        public static final ParseField INDEX = new ParseField("index");
        public static final ParseField RULES = new ParseField("rules");

        static {
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), INDEX);
            PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), Job.DESCRIPTION);
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), DetectionRule.PARSER, RULES);
        }

        private int index;
        private String description;
        private List<DetectionRule> rules;

        public DetectorUpdate(int index, String description, List<DetectionRule> rules) {
            this.index = index;
            this.description = description;
            this.rules = rules;
        }

        public DetectorUpdate(StreamInput in) throws IOException {
            index = in.readInt();
            description = in.readOptionalString();
            if (in.readBoolean()) {
                rules = in.readList(DetectionRule::new);
            } else {
                rules = null;
            }
        }

        public int getIndex() {
            return index;
        }

        public String getDescription() {
            return description;
        }

        public List<DetectionRule> getRules() {
            return rules;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(index);
            out.writeOptionalString(description);
            out.writeBoolean(rules != null);
            if (rules != null) {
                out.writeList(rules);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(INDEX.getPreferredName(), index);
            if (description != null) {
                builder.field(Job.DESCRIPTION.getPreferredName(), description);
            }
            if (rules != null) {
                builder.field(RULES.getPreferredName(), rules);
            }
            builder.endObject();

            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, description, rules);
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
            return this.index == that.index && Objects.equals(this.description, that.description)
                    && Objects.equals(this.rules, that.rules);
        }
    }
}
