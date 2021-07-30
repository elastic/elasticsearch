/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStats;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.process.results.ModelMetadata;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.elasticsearch.xpack.ml.dataframe.process.results.TrainedModelDefinitionChunk;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsHolder;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsPersister;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfo;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

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
    private final DataFrameAnalyticsAuditor auditor;
    private final StatsPersister statsPersister;
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private final ChunkedTrainedModelPersister chunkedTrainedModelPersister;
    private volatile String failure;
    private volatile boolean isCancelled;
    private long processedRows;

    private volatile String latestModelId;

    public AnalyticsResultProcessor(DataFrameAnalyticsConfig analytics, DataFrameRowsJoiner dataFrameRowsJoiner,
                                    StatsHolder statsHolder, TrainedModelProvider trainedModelProvider,
                                    DataFrameAnalyticsAuditor auditor, StatsPersister statsPersister, ExtractedFields extractedFields) {
        this.analytics = Objects.requireNonNull(analytics);
        this.dataFrameRowsJoiner = Objects.requireNonNull(dataFrameRowsJoiner);
        this.statsHolder = Objects.requireNonNull(statsHolder);
        this.auditor = Objects.requireNonNull(auditor);
        this.statsPersister = Objects.requireNonNull(statsPersister);
        this.chunkedTrainedModelPersister = new ChunkedTrainedModelPersister(
            trainedModelProvider,
            analytics,
            auditor,
            this::setAndReportFailure,
            extractedFields
        );
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
        isCancelled = true;
    }

    public void process(AnalyticsProcess<AnalyticsResult> process) {
        long totalRows = process.getConfig().rows();

        // TODO When java 9 features can be used, we will not need the local variable here
        try (DataFrameRowsJoiner resultsJoiner = dataFrameRowsJoiner) {
            Iterator<AnalyticsResult> iterator = process.readAnalyticsResults();
            while (iterator.hasNext()) {
                processResult(iterator.next(), resultsJoiner, totalRows);
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
        }
    }

    private void updateResultsProgress(int progress) {
        statsHolder.getProgressTracker().updateWritingResultsProgress(Math.min(progress, MAX_PROGRESS_BEFORE_COMPLETION));
    }

    private void completeResultsProgress() {
        statsHolder.getProgressTracker().updateWritingResultsProgress(100);
    }

    private void processResult(AnalyticsResult result, DataFrameRowsJoiner resultsJoiner, long totalRows) {
        RowResults rowResults = result.getRowResults();
        if (rowResults != null && isCancelled == false) {
            processRowResult(resultsJoiner, totalRows, rowResults);
        }
        PhaseProgress phaseProgress = result.getPhaseProgress();
        if (phaseProgress != null) {
            LOGGER.debug("[{}] progress for phase [{}] updated to [{}]", analytics.getId(), phaseProgress.getPhase(),
                phaseProgress.getProgressPercent());
            statsHolder.getProgressTracker().updatePhase(phaseProgress);
        }
        ModelSizeInfo modelSize = result.getModelSizeInfo();
        if (modelSize != null) {
            latestModelId = chunkedTrainedModelPersister.createAndIndexInferenceModelConfig(modelSize, TrainedModelType.TREE_ENSEMBLE);
        }
        TrainedModelDefinitionChunk trainedModelDefinitionChunk = result.getTrainedModelDefinitionChunk();
        if (trainedModelDefinitionChunk != null && isCancelled == false) {
            chunkedTrainedModelPersister.createAndIndexInferenceModelDoc(trainedModelDefinitionChunk);
        }
        ModelMetadata modelMetadata = result.getModelMetadata();
        if (modelMetadata != null) {
            chunkedTrainedModelPersister.createAndIndexInferenceModelMetadata(modelMetadata);
        }
        MemoryUsage memoryUsage = result.getMemoryUsage();
        if (memoryUsage != null) {
            processMemoryUsage(memoryUsage);
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

    private void processRowResult(DataFrameRowsJoiner rowsJoiner, long totalRows, RowResults rowResults) {
        rowsJoiner.processRowResults(rowResults);
        if (processedRows == 0) {
            LOGGER.info("[{}] Started writing results", analytics.getId());
            auditor.info(analytics.getId(), Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_STARTED_WRITING_RESULTS));
        }
        processedRows++;
        updateResultsProgress(processedRows >= totalRows ? 100 : (int) (processedRows * 100.0 / totalRows));
    }

    private void setAndReportFailure(Exception e) {
        LOGGER.error(new ParameterizedMessage("[{}] Error processing results; ", analytics.getId()), e);
        failure = "error processing results; " + e.getMessage();
        auditor.error(analytics.getId(), "Error processing results; " + e.getMessage());
    }

    private void processMemoryUsage(MemoryUsage memoryUsage) {
        statsHolder.setMemoryUsage(memoryUsage);
        statsPersister.persistWithRetry(memoryUsage, memoryUsage::documentId);
    }

    @Nullable
    public String getLatestModelId() {
        return latestModelId;
    }
}
