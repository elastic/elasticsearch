/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStats;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsHolder;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsPersister;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.MultiField;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class AnalyticsResultProcessor {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsResultProcessor.class);

    /**
     * While we report progress as we read row results there are other things we need to account for
     * to report completion. There are other types of results we can't predict the number of like
     * progress objects and the inference model. Thus, we report a max progress until we know we have
     * completed processing results.
     *
     * It is critical to ensure we do not report complete progress too soon as restarting a job
     * uses the progress to determine which state to restart from. If we report full progress too soon
     * we cannot restart a job as we will think the job was finished.
     */
    private static final int MAX_PROGRESS_BEFORE_COMPLETION = 98;

    private final DataFrameAnalyticsConfig analytics;
    private final DataFrameRowsJoiner dataFrameRowsJoiner;
    private final StatsHolder statsHolder;
    private final TrainedModelProvider trainedModelProvider;
    private final DataFrameAnalyticsAuditor auditor;
    private final StatsPersister statsPersister;
    private final List<ExtractedField> fieldNames;
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private volatile String failure;
    private volatile boolean isCancelled;

    public AnalyticsResultProcessor(DataFrameAnalyticsConfig analytics, DataFrameRowsJoiner dataFrameRowsJoiner,
                                    StatsHolder statsHolder, TrainedModelProvider trainedModelProvider,
                                    DataFrameAnalyticsAuditor auditor, StatsPersister statsPersister, List<ExtractedField> fieldNames) {
        this.analytics = Objects.requireNonNull(analytics);
        this.dataFrameRowsJoiner = Objects.requireNonNull(dataFrameRowsJoiner);
        this.statsHolder = Objects.requireNonNull(statsHolder);
        this.trainedModelProvider = Objects.requireNonNull(trainedModelProvider);
        this.auditor = Objects.requireNonNull(auditor);
        this.statsPersister = Objects.requireNonNull(statsPersister);
        this.fieldNames = Collections.unmodifiableList(Objects.requireNonNull(fieldNames));
    }

    @Nullable
    public String getFailure() {
        return failure == null ? dataFrameRowsJoiner.getFailure() : failure;
    }

    public void awaitForCompletion() {
        try {
            completionLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            setAndReportFailure(ExceptionsHelper.serverError("interrupted waiting for results processor to complete", e));
        }
    }

    public void cancel() {
        dataFrameRowsJoiner.cancel();
        statsPersister.cancel();
        isCancelled = true;
    }

    public void process(AnalyticsProcess<AnalyticsResult> process) {
        long totalRows = process.getConfig().rows();
        long processedRows = 0;

        // TODO When java 9 features can be used, we will not need the local variable here
        try (DataFrameRowsJoiner resultsJoiner = dataFrameRowsJoiner) {
            Iterator<AnalyticsResult> iterator = process.readAnalyticsResults();
            while (iterator.hasNext()) {
                if (isCancelled) {
                    break;
                }
                AnalyticsResult result = iterator.next();
                processResult(result, resultsJoiner);
                if (result.getRowResults() != null) {
                    if (processedRows == 0) {
                        LOGGER.info("[{}] Started writing results", analytics.getId());
                        auditor.info(analytics.getId(), Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_STARTED_WRITING_RESULTS));
                    }
                    processedRows++;
                    updateResultsProgress(processedRows >= totalRows ? 100 : (int) (processedRows * 100.0 / totalRows));
                }
            }
        } catch (Exception e) {
            if (isCancelled) {
                // No need to log error as it's due to stopping
            } else {
                setAndReportFailure(e);
            }
        } finally {
            if (isCancelled == false && failure == null) {
                completeResultsProgress();
            }
            completionLatch.countDown();
            process.consumeAndCloseOutputStream();
        }
    }

    private void updateResultsProgress(int progress) {
        statsHolder.getProgressTracker().updateWritingResultsProgress(Math.min(progress, MAX_PROGRESS_BEFORE_COMPLETION));
    }

    private void completeResultsProgress() {
        statsHolder.getProgressTracker().updateWritingResultsProgress(100);
    }

    private void processResult(AnalyticsResult result, DataFrameRowsJoiner resultsJoiner) {
        RowResults rowResults = result.getRowResults();
        if (rowResults != null) {
            resultsJoiner.processRowResults(rowResults);
        }
        PhaseProgress phaseProgress = result.getPhaseProgress();
        if (phaseProgress != null) {
            LOGGER.debug("[{}] progress for phase [{}] updated to [{}]", analytics.getId(), phaseProgress.getPhase(),
                phaseProgress.getProgressPercent());
            statsHolder.getProgressTracker().updatePhase(phaseProgress);
        }
        TrainedModelDefinition.Builder inferenceModelBuilder = result.getInferenceModelBuilder();
        if (inferenceModelBuilder != null) {
            createAndIndexInferenceModel(inferenceModelBuilder);
        }
        MemoryUsage memoryUsage = result.getMemoryUsage();
        if (memoryUsage != null) {
            statsHolder.setMemoryUsage(memoryUsage);
            statsPersister.persistWithRetry(memoryUsage, memoryUsage::documentId);
        }
        OutlierDetectionStats outlierDetectionStats = result.getOutlierDetectionStats();
        if (outlierDetectionStats != null) {
            statsHolder.setAnalysisStats(outlierDetectionStats);
            statsPersister.persistWithRetry(outlierDetectionStats, outlierDetectionStats::documentId);
        }
        ClassificationStats classificationStats = result.getClassificationStats();
        if (classificationStats != null) {
            statsHolder.setAnalysisStats(classificationStats);
            statsPersister.persistWithRetry(classificationStats, classificationStats::documentId);
        }
        RegressionStats regressionStats = result.getRegressionStats();
        if (regressionStats != null) {
            statsHolder.setAnalysisStats(regressionStats);
            statsPersister.persistWithRetry(regressionStats, regressionStats::documentId);
        }
    }

    private void createAndIndexInferenceModel(TrainedModelDefinition.Builder inferenceModel) {
        TrainedModelConfig trainedModelConfig = createTrainedModelConfig(inferenceModel);
        CountDownLatch latch = storeTrainedModel(trainedModelConfig);

        try {
            if (latch.await(30, TimeUnit.SECONDS) == false) {
                LOGGER.error("[{}] Timed out (30s) waiting for inference model to be stored", analytics.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            setAndReportFailure(ExceptionsHelper.serverError("interrupted waiting for inference model to be stored"));
        }
    }

    private TrainedModelConfig createTrainedModelConfig(TrainedModelDefinition.Builder inferenceModel) {
        Instant createTime = Instant.now();
        String modelId = analytics.getId() + "-" + createTime.toEpochMilli();
        TrainedModelDefinition definition = inferenceModel.build();
        String dependentVariable = getDependentVariable();
        List<String> fieldNamesWithoutDependentVariable = fieldNames.stream()
            .map(ExtractedField::getName)
            .filter(f -> f.equals(dependentVariable) == false)
            .collect(toList());
        Map<String, String> defaultFieldMapping = fieldNames.stream()
            .filter(ef -> ef instanceof MultiField && (ef.getName().equals(dependentVariable) == false))
            .collect(Collectors.toMap(ExtractedField::getParentField, ExtractedField::getName));
        return TrainedModelConfig.builder()
            .setModelId(modelId)
            .setCreatedBy(XPackUser.NAME)
            .setVersion(Version.CURRENT)
            .setCreateTime(createTime)
            // NOTE: GET _cat/ml/trained_models relies on the creating analytics ID being in the tags
            .setTags(Collections.singletonList(analytics.getId()))
            .setDescription(analytics.getDescription())
            .setMetadata(Collections.singletonMap("analytics_config",
                XContentHelper.convertToMap(JsonXContent.jsonXContent, analytics.toString(), true)))
            .setEstimatedHeapMemory(definition.ramBytesUsed())
            .setEstimatedOperations(definition.getTrainedModel().estimatedNumOperations())
            .setParsedDefinition(inferenceModel)
            .setInput(new TrainedModelInput(fieldNamesWithoutDependentVariable))
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setDefaultFieldMap(defaultFieldMapping)
            .setInferenceConfig(buildInferenceConfig(definition.getTrainedModel().targetType()))
            .build();
    }

    private InferenceConfig buildInferenceConfig(TargetType targetType) {
        switch (targetType) {
            case CLASSIFICATION:
                assert analytics.getAnalysis() instanceof Classification;
                Classification classification = ((Classification)analytics.getAnalysis());
                PredictionFieldType predictionFieldType = getPredictionFieldType(classification);
                return ClassificationConfig.builder()
                    .setNumTopClasses(classification.getNumTopClasses())
                    .setNumTopFeatureImportanceValues(classification.getBoostedTreeParams().getNumTopFeatureImportanceValues())
                    .setPredictionFieldType(predictionFieldType)
                    .build();
            case REGRESSION:
                assert analytics.getAnalysis() instanceof Regression;
                Regression regression = ((Regression)analytics.getAnalysis());
                return RegressionConfig.builder()
                    .setNumTopFeatureImportanceValues(regression.getBoostedTreeParams().getNumTopFeatureImportanceValues())
                    .build();
            default:
                throw ExceptionsHelper.serverError(
                    "process created a model with an unsupported target type [{}]",
                    null,
                    targetType);
        }
    }

    PredictionFieldType getPredictionFieldType(Classification classification) {
        String dependentVariable = classification.getDependentVariable();
        Optional<ExtractedField> extractedField = fieldNames.stream()
            .filter(f -> f.getName().equals(dependentVariable))
            .findAny();
        PredictionFieldType predictionFieldType = Classification.getPredictionFieldType(
            extractedField.isPresent() ? extractedField.get().getTypes() : null
        );
        return predictionFieldType == null ? PredictionFieldType.STRING : predictionFieldType;
    }

    private String getDependentVariable() {
        if (analytics.getAnalysis() instanceof Classification) {
            return ((Classification)analytics.getAnalysis()).getDependentVariable();
        }
        if (analytics.getAnalysis() instanceof Regression) {
            return ((Regression)analytics.getAnalysis()).getDependentVariable();
        }
        return null;
    }

    private CountDownLatch storeTrainedModel(TrainedModelConfig trainedModelConfig) {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Boolean> storeListener = ActionListener.wrap(
            aBoolean -> {
                if (aBoolean == false) {
                    LOGGER.error("[{}] Storing trained model responded false", analytics.getId());
                    setAndReportFailure(ExceptionsHelper.serverError("storing trained model responded false"));
                } else {
                    LOGGER.info("[{}] Stored trained model with id [{}]", analytics.getId(), trainedModelConfig.getModelId());
                    auditor.info(analytics.getId(), "Stored trained model with id [" + trainedModelConfig.getModelId() + "]");
                }
            },
            e -> setAndReportFailure(ExceptionsHelper.serverError("error storing trained model with id [{}]", e,
                trainedModelConfig.getModelId()))
        );
        trainedModelProvider.storeTrainedModel(trainedModelConfig, new LatchedActionListener<>(storeListener, latch));
        return latch;
    }

    private void setAndReportFailure(Exception e) {
        LOGGER.error(new ParameterizedMessage("[{}] Error processing results; ", analytics.getId()), e);
        failure = "error processing results; " + e.getMessage();
        auditor.error(analytics.getId(), "Error processing results; " + e.getMessage());
    }
}
