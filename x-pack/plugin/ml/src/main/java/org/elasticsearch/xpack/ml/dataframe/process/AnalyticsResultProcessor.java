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
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask.ProgressTracker;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    private final ProgressTracker progressTracker;
    private final TrainedModelProvider trainedModelProvider;
    private final DataFrameAnalyticsAuditor auditor;
    private final List<String> fieldNames;
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private volatile String failure;
    private volatile boolean isCancelled;

    public AnalyticsResultProcessor(DataFrameAnalyticsConfig analytics, DataFrameRowsJoiner dataFrameRowsJoiner,
                                    ProgressTracker progressTracker, TrainedModelProvider trainedModelProvider,
                                    DataFrameAnalyticsAuditor auditor, List<String> fieldNames) {
        this.analytics = Objects.requireNonNull(analytics);
        this.dataFrameRowsJoiner = Objects.requireNonNull(dataFrameRowsJoiner);
        this.progressTracker = Objects.requireNonNull(progressTracker);
        this.trainedModelProvider = Objects.requireNonNull(trainedModelProvider);
        this.auditor = Objects.requireNonNull(auditor);
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
        progressTracker.writingResultsPercent.set(Math.min(progress, MAX_PROGRESS_BEFORE_COMPLETION));
    }

    private void completeResultsProgress() {
        progressTracker.writingResultsPercent.set(100);
    }

    private void processResult(AnalyticsResult result, DataFrameRowsJoiner resultsJoiner) {
        RowResults rowResults = result.getRowResults();
        if (rowResults != null) {
            resultsJoiner.processRowResults(rowResults);
        }
        Integer progressPercent = result.getProgressPercent();
        if (progressPercent != null) {
            progressTracker.analyzingPercent.set(progressPercent);
        }
        TrainedModelDefinition.Builder inferenceModelBuilder = result.getInferenceModelBuilder();
        if (inferenceModelBuilder != null) {
            createAndIndexInferenceModel(inferenceModelBuilder);
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
            .filter(f -> f.equals(dependentVariable) == false)
            .collect(toList());
        return TrainedModelConfig.builder()
            .setModelId(modelId)
            .setCreatedBy(XPackUser.NAME)
            .setVersion(Version.CURRENT)
            .setCreateTime(createTime)
            .setTags(Collections.singletonList(analytics.getId()))
            .setDescription(analytics.getDescription())
            .setMetadata(Collections.singletonMap("analytics_config",
                XContentHelper.convertToMap(JsonXContent.jsonXContent, analytics.toString(), true)))
            .setEstimatedHeapMemory(definition.ramBytesUsed())
            .setEstimatedOperations(definition.getTrainedModel().estimatedNumOperations())
            .setParsedDefinition(inferenceModel)
            .setInput(new TrainedModelInput(fieldNamesWithoutDependentVariable))
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .build();
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
