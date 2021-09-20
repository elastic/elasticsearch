/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStats;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfo;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class AnalyticsResult implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("analytics_result");

    private static final ParseField PHASE_PROGRESS = new ParseField("phase_progress");
    private static final ParseField MODEL_SIZE_INFO = new ParseField("model_size_info");
    private static final ParseField COMPRESSED_INFERENCE_MODEL = new ParseField("compressed_inference_model");
    private static final ParseField ANALYTICS_MEMORY_USAGE = new ParseField("analytics_memory_usage");
    private static final ParseField OUTLIER_DETECTION_STATS = new ParseField("outlier_detection_stats");
    private static final ParseField CLASSIFICATION_STATS = new ParseField("classification_stats");
    private static final ParseField REGRESSION_STATS = new ParseField("regression_stats");
    private static final ParseField MODEL_METADATA = new ParseField("model_metadata");

    public static final ConstructingObjectParser<AnalyticsResult, Void> PARSER = new ConstructingObjectParser<>(TYPE.getPreferredName(),
            a -> new AnalyticsResult(
                (RowResults) a[0],
                (PhaseProgress) a[1],
                (MemoryUsage) a[2],
                (OutlierDetectionStats) a[3],
                (ClassificationStats) a[4],
                (RegressionStats) a[5],
                (ModelSizeInfo) a[6],
                (TrainedModelDefinitionChunk) a[7],
                (ModelMetadata) a[8]
            ));

    static {
        PARSER.declareObject(optionalConstructorArg(), RowResults.PARSER, RowResults.TYPE);
        PARSER.declareObject(optionalConstructorArg(), PhaseProgress.PARSER, PHASE_PROGRESS);
        PARSER.declareObject(optionalConstructorArg(), MemoryUsage.STRICT_PARSER, ANALYTICS_MEMORY_USAGE);
        PARSER.declareObject(optionalConstructorArg(), OutlierDetectionStats.STRICT_PARSER, OUTLIER_DETECTION_STATS);
        PARSER.declareObject(optionalConstructorArg(), ClassificationStats.STRICT_PARSER, CLASSIFICATION_STATS);
        PARSER.declareObject(optionalConstructorArg(), RegressionStats.STRICT_PARSER, REGRESSION_STATS);
        PARSER.declareObject(optionalConstructorArg(), ModelSizeInfo.PARSER, MODEL_SIZE_INFO);
        PARSER.declareObject(optionalConstructorArg(), TrainedModelDefinitionChunk.PARSER, COMPRESSED_INFERENCE_MODEL);
        PARSER.declareObject(optionalConstructorArg(), ModelMetadata.PARSER, MODEL_METADATA);
    }

    private final RowResults rowResults;
    private final PhaseProgress phaseProgress;
    private final MemoryUsage memoryUsage;
    private final OutlierDetectionStats outlierDetectionStats;
    private final ClassificationStats classificationStats;
    private final RegressionStats regressionStats;
    private final ModelSizeInfo modelSizeInfo;
    private final TrainedModelDefinitionChunk trainedModelDefinitionChunk;
    private final ModelMetadata modelMetadata;

    private AnalyticsResult(@Nullable RowResults rowResults,
                            @Nullable PhaseProgress phaseProgress,
                            @Nullable MemoryUsage memoryUsage,
                            @Nullable OutlierDetectionStats outlierDetectionStats,
                            @Nullable ClassificationStats classificationStats,
                            @Nullable RegressionStats regressionStats,
                            @Nullable ModelSizeInfo modelSizeInfo,
                            @Nullable TrainedModelDefinitionChunk trainedModelDefinitionChunk,
                            @Nullable ModelMetadata modelMetadata) {
        this.rowResults = rowResults;
        this.phaseProgress = phaseProgress;
        this.memoryUsage = memoryUsage;
        this.outlierDetectionStats = outlierDetectionStats;
        this.classificationStats = classificationStats;
        this.regressionStats = regressionStats;
        this.modelSizeInfo = modelSizeInfo;
        this.trainedModelDefinitionChunk = trainedModelDefinitionChunk;
        this.modelMetadata = modelMetadata;
    }

    public RowResults getRowResults() {
        return rowResults;
    }

    public PhaseProgress getPhaseProgress() {
        return phaseProgress;
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

    public ModelSizeInfo getModelSizeInfo() {
        return modelSizeInfo;
    }

    public TrainedModelDefinitionChunk getTrainedModelDefinitionChunk() {
        return trainedModelDefinitionChunk;
    }

    public ModelMetadata getModelMetadata() {
        return modelMetadata;
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
        if (modelSizeInfo != null) {
            builder.field(MODEL_SIZE_INFO.getPreferredName(), modelSizeInfo);
        }
        if (trainedModelDefinitionChunk != null) {
            builder.field(COMPRESSED_INFERENCE_MODEL.getPreferredName(), trainedModelDefinitionChunk);
        }
        if (modelMetadata != null) {
            builder.field(MODEL_METADATA.getPreferredName(), modelMetadata);
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
            && Objects.equals(memoryUsage, that.memoryUsage)
            && Objects.equals(outlierDetectionStats, that.outlierDetectionStats)
            && Objects.equals(classificationStats, that.classificationStats)
            && Objects.equals(modelSizeInfo, that.modelSizeInfo)
            && Objects.equals(trainedModelDefinitionChunk, that.trainedModelDefinitionChunk)
            && Objects.equals(modelMetadata, that.modelMetadata)
            && Objects.equals(regressionStats, that.regressionStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowResults, phaseProgress, memoryUsage, outlierDetectionStats, classificationStats,
            regressionStats, modelSizeInfo, trainedModelDefinitionChunk, modelMetadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private RowResults rowResults;
        private PhaseProgress phaseProgress;
        private MemoryUsage memoryUsage;
        private OutlierDetectionStats outlierDetectionStats;
        private ClassificationStats classificationStats;
        private RegressionStats regressionStats;
        private ModelSizeInfo modelSizeInfo;
        private TrainedModelDefinitionChunk trainedModelDefinitionChunk;
        private ModelMetadata modelMetadata;

        private Builder() {}

        public Builder setRowResults(RowResults rowResults) {
            this.rowResults = rowResults;
            return this;
        }

        public Builder setPhaseProgress(PhaseProgress phaseProgress) {
            this.phaseProgress = phaseProgress;
            return this;
        }

        public Builder setMemoryUsage(MemoryUsage memoryUsage) {
            this.memoryUsage = memoryUsage;
            return this;
        }

        public Builder setOutlierDetectionStats(OutlierDetectionStats outlierDetectionStats) {
            this.outlierDetectionStats = outlierDetectionStats;
            return this;
        }

        public Builder setClassificationStats(ClassificationStats classificationStats) {
            this.classificationStats = classificationStats;
            return this;
        }

        public Builder setRegressionStats(RegressionStats regressionStats) {
            this.regressionStats = regressionStats;
            return this;
        }

        public Builder setModelSizeInfo(ModelSizeInfo modelSizeInfo) {
            this.modelSizeInfo = modelSizeInfo;
            return this;
        }

        public Builder setTrainedModelDefinitionChunk(TrainedModelDefinitionChunk trainedModelDefinitionChunk) {
            this.trainedModelDefinitionChunk = trainedModelDefinitionChunk;
            return this;
        }

        public Builder setModelMetadata(ModelMetadata modelMetadata) {
            this.modelMetadata = modelMetadata;
            return this;
        }

        public AnalyticsResult build() {
            return new AnalyticsResult(
                rowResults,
                phaseProgress,
                memoryUsage,
                outlierDetectionStats,
                classificationStats,
                regressionStats,
                modelSizeInfo,
                trainedModelDefinitionChunk,
                modelMetadata
            );
        }
    }
}
