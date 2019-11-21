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
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
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

public class AnalyticsResultProcessor {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsResultProcessor.class);

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
            LOGGER.error(new ParameterizedMessage("[{}] Interrupted waiting for results processor to complete", analytics.getId()), e);
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
                    progressTracker.writingResultsPercent.set(processedRows >= totalRows ? 100 : (int) (processedRows * 100.0 / totalRows));
                }
            }
            if (isCancelled == false) {
                // This means we completed successfully so we need to set the progress to 100.
                // This is because due to skipped rows, it is possible the processed rows will not reach the total rows.
                progressTracker.writingResultsPercent.set(100);
            }
        } catch (Exception e) {
            if (isCancelled) {
                // No need to log error as it's due to stopping
            } else {
                LOGGER.error(new ParameterizedMessage("[{}] Error parsing data frame analytics output", analytics.getId()), e);
                failure = "error parsing data frame analytics output: [" + e.getMessage() + "]";
            }
        } finally {
            completionLatch.countDown();
            process.consumeAndCloseOutputStream();
        }
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
            LOGGER.error(new ParameterizedMessage("[{}] Interrupted waiting for inference model to be stored", analytics.getId()), e);
        }
    }

    private TrainedModelConfig createTrainedModelConfig(TrainedModelDefinition.Builder inferenceModel) {
        Instant createTime = Instant.now();
        String modelId = analytics.getId() + "-" + createTime.toEpochMilli();
        TrainedModelDefinition definition = inferenceModel.setModelId(modelId).build();
        return TrainedModelConfig.builder()
            .setModelId(modelId)
            .setCreatedBy("data-frame-analytics")
            .setVersion(Version.CURRENT)
            .setCreateTime(createTime)
            .setTags(Collections.singletonList(analytics.getId()))
            .setDescription(analytics.getDescription())
            .setMetadata(Collections.singletonMap("analytics_config",
                XContentHelper.convertToMap(JsonXContent.jsonXContent, analytics.toString(), true)))
            .setDefinition(definition)
            .setEstimatedHeapMemory(definition.ramBytesUsed())
            .setEstimatedOperations(definition.getTrainedModel().estimatedNumOperations())
            .setInput(new TrainedModelInput(fieldNames))
            .build();
    }

    private CountDownLatch storeTrainedModel(TrainedModelConfig trainedModelConfig) {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Boolean> storeListener = ActionListener.wrap(
            aBoolean -> {
                if (aBoolean == false) {
                    LOGGER.error("[{}] Storing trained model responded false", analytics.getId());
                } else {
                    LOGGER.info("[{}] Stored trained model with id [{}]", analytics.getId(), trainedModelConfig.getModelId());
                    auditor.info(analytics.getId(), "Stored trained model with id [" + trainedModelConfig.getModelId() + "]");
                }
            },
            e -> {
                LOGGER.error(new ParameterizedMessage("[{}] Error storing trained model [{}]", analytics.getId(),
                    trainedModelConfig.getModelId()), e);
                auditor.error(analytics.getId(), "Error storing trained model with id [" + trainedModelConfig.getModelId()
                    + "]; error message [" + e.getMessage() + "]");
            }
        );
        trainedModelProvider.storeTrainedModel(trainedModelConfig, new LatchedActionListener<>(storeListener, latch));
        return latch;
    }
}
