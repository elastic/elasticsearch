/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStats;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.FOR_INTERNAL_STORAGE;

public class AnalyticsResult implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("analytics_result");

    private static final ParseField PHASE_PROGRESS = new ParseField("phase_progress");
    private static final ParseField PROGRESS_PERCENT = new ParseField("progress_percent");
    private static final ParseField INFERENCE_MODEL = new ParseField("inference_model");
    private static final ParseField ANALYTICS_MEMORY_USAGE = new ParseField("analytics_memory_usage");
    private static final ParseField OUTLIER_DETECTION_STATS = new ParseField("outlier_detection_stats");
    private static final ParseField CLASSIFICATION_STATS = new ParseField("classification_stats");
    private static final ParseField REGRESSION_STATS = new ParseField("regression_stats");

    public static final ConstructingObjectParser<AnalyticsResult, Void> PARSER = new ConstructingObjectParser<>(TYPE.getPreferredName(),
            a -> new AnalyticsResult(
                (RowResults) a[0],
                (PhaseProgress) a[1],
                (Integer) a[2],
                (TrainedModelDefinition.Builder) a[3],
                (MemoryUsage) a[4],
                (OutlierDetectionStats) a[5],
                (ClassificationStats) a[6],
                (RegressionStats) a[7]
            ));

    static {
        PARSER.declareObject(optionalConstructorArg(), RowResults.PARSER, RowResults.TYPE);
        PARSER.declareObject(optionalConstructorArg(), PhaseProgress.PARSER, PHASE_PROGRESS);
        PARSER.declareInt(optionalConstructorArg(), PROGRESS_PERCENT);
        // TODO change back to STRICT_PARSER once native side is aligned
        PARSER.declareObject(optionalConstructorArg(), TrainedModelDefinition.LENIENT_PARSER, INFERENCE_MODEL);
        PARSER.declareObject(optionalConstructorArg(), MemoryUsage.STRICT_PARSER, ANALYTICS_MEMORY_USAGE);
        PARSER.declareObject(optionalConstructorArg(), OutlierDetectionStats.STRICT_PARSER, OUTLIER_DETECTION_STATS);
        PARSER.declareObject(optionalConstructorArg(), ClassificationStats.STRICT_PARSER, CLASSIFICATION_STATS);
        PARSER.declareObject(optionalConstructorArg(), RegressionStats.STRICT_PARSER, REGRESSION_STATS);
    }

    private final RowResults rowResults;
    private final PhaseProgress phaseProgress;

    // TODO remove after process is writing out phase_progress
    private final Integer progressPercent;

    private final TrainedModelDefinition.Builder inferenceModelBuilder;
    private final TrainedModelDefinition inferenceModel;
    private final MemoryUsage memoryUsage;
    private final OutlierDetectionStats outlierDetectionStats;
    private final ClassificationStats classificationStats;
    private final RegressionStats regressionStats;

    public AnalyticsResult(@Nullable RowResults rowResults,
                           @Nullable PhaseProgress phaseProgress,
                           @Nullable Integer progressPercent,
                           @Nullable TrainedModelDefinition.Builder inferenceModelBuilder,
                           @Nullable MemoryUsage memoryUsage,
                           @Nullable OutlierDetectionStats outlierDetectionStats,
                           @Nullable ClassificationStats classificationStats,
                           @Nullable RegressionStats regressionStats) {
        this.rowResults = rowResults;
        this.phaseProgress = phaseProgress;
        this.progressPercent = progressPercent;
        this.inferenceModelBuilder = inferenceModelBuilder;
        this.inferenceModel = inferenceModelBuilder == null ? null : inferenceModelBuilder.build();
        this.memoryUsage = memoryUsage;
        this.outlierDetectionStats = outlierDetectionStats;
        this.classificationStats = classificationStats;
        this.regressionStats = regressionStats;
    }

    public RowResults getRowResults() {
        return rowResults;
    }

    public PhaseProgress getPhaseProgress() {
        return phaseProgress;
    }

    public Integer getProgressPercent() {
        return progressPercent;
    }

    public TrainedModelDefinition.Builder getInferenceModelBuilder() {
        return inferenceModelBuilder;
    }

    public MemoryUsage getMemoryUsage() {
        return memoryUsage;
    }

    public OutlierDetectionStats getOutlierDetectionStats() {
        return outlierDetectionStats;
    }

    public ClassificationStats getClassificationStats() {
        return classificationStats;
    }

    public RegressionStats getRegressionStats() {
        return regressionStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (rowResults != null) {
            builder.field(RowResults.TYPE.getPreferredName(), rowResults);
        }
        if (phaseProgress != null) {
            builder.field(PHASE_PROGRESS.getPreferredName(), phaseProgress);
        }
        if (progressPercent != null) {
            builder.field(PROGRESS_PERCENT.getPreferredName(), progressPercent);
        }
        if (inferenceModel != null) {
            builder.field(INFERENCE_MODEL.getPreferredName(),
                inferenceModel,
                new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true")));
        }
        if (memoryUsage != null) {
            builder.field(ANALYTICS_MEMORY_USAGE.getPreferredName(), memoryUsage, params);
        }
        if (outlierDetectionStats != null) {
            builder.field(OUTLIER_DETECTION_STATS.getPreferredName(), outlierDetectionStats, params);
        }
        if (classificationStats != null) {
            builder.field(CLASSIFICATION_STATS.getPreferredName(), classificationStats, params);
        }
        if (regressionStats != null) {
            builder.field(REGRESSION_STATS.getPreferredName(), regressionStats, params);
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

        AnalyticsResult that = (AnalyticsResult) other;
        return Objects.equals(rowResults, that.rowResults)
            && Objects.equals(phaseProgress, that.phaseProgress)
            && Objects.equals(progressPercent, that.progressPercent)
            && Objects.equals(inferenceModel, that.inferenceModel)
            && Objects.equals(memoryUsage, that.memoryUsage)
            && Objects.equals(outlierDetectionStats, that.outlierDetectionStats)
            && Objects.equals(classificationStats, that.classificationStats)
            && Objects.equals(regressionStats, that.regressionStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowResults, phaseProgress, progressPercent, inferenceModel, memoryUsage, outlierDetectionStats,
            classificationStats, regressionStats);
    }
}
