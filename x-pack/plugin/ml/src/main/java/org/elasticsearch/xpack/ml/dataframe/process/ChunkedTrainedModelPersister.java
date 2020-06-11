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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.dataframe.process.results.TrainedModelDefinitionChunk;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.MultiField;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfo;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class ChunkedTrainedModelPersister {

    private static final Logger LOGGER = LogManager.getLogger(ChunkedTrainedModelPersister.class);
    private final TrainedModelProvider provider;
    private final AtomicReference<String> currentModelId;
    private final AtomicInteger currentChunkedDoc;
    private final AtomicLong persistedChunkLengths;
    private final DataFrameAnalyticsConfig analytics;
    private final DataFrameAnalyticsAuditor auditor;
    private final Consumer<Exception> failureHandler;
    private final List<ExtractedField> fieldNames;
    private volatile boolean readyToStoreNewModel = true;

    public ChunkedTrainedModelPersister(TrainedModelProvider provider,
                                        DataFrameAnalyticsConfig analytics,
                                        DataFrameAnalyticsAuditor auditor,
                                        Consumer<Exception> failureHandler,
                                        List<ExtractedField> fieldNames) {
        this.provider = provider;
        this.currentModelId = new AtomicReference<>("");
        this.currentChunkedDoc = new AtomicInteger(0);
        this.persistedChunkLengths = new AtomicLong(0L);
        this.analytics = analytics;
        this.auditor = auditor;
        this.failureHandler = failureHandler;
        this.fieldNames = fieldNames;
    }

    public void createAndIndexInferenceModelDoc(TrainedModelDefinitionChunk trainedModelDefinitionChunk) {
        if (Strings.isNullOrEmpty(this.currentModelId.get())) {
            failureHandler.accept(ExceptionsHelper.serverError(
                "chunked inference model definition is attempting to be stored before trained model configuration"
            ));
            return;
        }
        TrainedModelDefinitionDoc trainedModelDefinitionDoc = trainedModelDefinitionChunk.createTrainedModelDoc(
            this.currentModelId.get(),
            this.currentChunkedDoc.getAndIncrement());

        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Void> storeListener = ActionListener.wrap(
            r -> {
                LOGGER.debug(() -> new ParameterizedMessage(
                    "[{}] stored trained model definition chunk [{}] [{}]",
                    analytics.getId(),
                    trainedModelDefinitionDoc.getModelId(),
                    trainedModelDefinitionDoc.getDocNum()));

                long persistedChunkLengths = this.persistedChunkLengths.addAndGet(trainedModelDefinitionDoc.getDefinitionLength());
                if (persistedChunkLengths >= trainedModelDefinitionDoc.getTotalDefinitionLength()) {
                    readyToStoreNewModel = true;
                    LOGGER.info(
                        "[{}] finished stored trained model definition chunks with id [{}]",
                        analytics.getId(),
                        this.currentModelId.get());
                    auditor.info(analytics.getId(), "Stored trained model with id [" + this.currentModelId.get() + "]");
                    CountDownLatch refreshLatch = new CountDownLatch(1);
                    provider.refreshInferenceIndex(
                        new LatchedActionListener<>(ActionListener.wrap(
                            refreshResponse -> LOGGER.debug(() -> new ParameterizedMessage(
                                "[{}] refreshed inference index after model store",
                                analytics.getId()
                            )),
                            e -> LOGGER.warn("[{}] failed to refresh inference index after model store", analytics.getId())),
                            refreshLatch));
                    try {
                        if (refreshLatch.await(30, TimeUnit.SECONDS) == false) {
                            LOGGER.error("[{}] Timed out (30s) waiting for index refresh", analytics.getId());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            },
            e -> failureHandler.accept(ExceptionsHelper.serverError("error storing trained model definition chunk [{}] with id [{}]", e,
                trainedModelDefinitionDoc.getModelId(), trainedModelDefinitionDoc.getDocNum()))
        );
        provider.storeTrainedModelDefinitionDoc(trainedModelDefinitionDoc, new LatchedActionListener<>(storeListener, latch));
        try {
            if (latch.await(30, TimeUnit.SECONDS) == false) {
                LOGGER.error("[{}] Timed out (30s) waiting for chunked inference definition to be stored", analytics.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failureHandler.accept(ExceptionsHelper.serverError("interrupted waiting for chunked inference definition to be stored"));
        }
    }

    public void createAndIndexInferenceModelMetadata(ModelSizeInfo inferenceModelSize) {
        if (readyToStoreNewModel == false) {
            failureHandler.accept(ExceptionsHelper.serverError(
                "new inference model is attempting to be stored before completion previous model storage"
            ));
            return;
        }
        TrainedModelConfig trainedModelConfig = createTrainedModelConfig(inferenceModelSize);
        CountDownLatch latch = storeTrainedModelMetadata(trainedModelConfig);
        try {
            readyToStoreNewModel = false;
            if (latch.await(30, TimeUnit.SECONDS) == false) {
                LOGGER.error("[{}] Timed out (30s) waiting for inference model metadata to be stored", analytics.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failureHandler.accept(ExceptionsHelper.serverError("interrupted waiting for inference model metadata to be stored"));
        }
    }

    private CountDownLatch storeTrainedModelMetadata(TrainedModelConfig trainedModelConfig) {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Boolean> storeListener = ActionListener.wrap(
            aBoolean -> {
                if (aBoolean == false) {
                    LOGGER.error("[{}] Storing trained model metadata responded false", analytics.getId());
                    failureHandler.accept(ExceptionsHelper.serverError("storing trained model responded false"));
                } else {
                    LOGGER.info("[{}] Stored trained model metadata with id [{}]", analytics.getId(), trainedModelConfig.getModelId());
                }
            },
            e -> failureHandler.accept(ExceptionsHelper.serverError("error storing trained model metadata with id [{}]", e,
                trainedModelConfig.getModelId()))
        );
        provider.storeTrainedModelMetadata(trainedModelConfig, new LatchedActionListener<>(storeListener, latch));
        return latch;
    }

    private TrainedModelConfig createTrainedModelConfig(ModelSizeInfo modelSize) {
        Instant createTime = Instant.now();
        String modelId = analytics.getId() + "-" + createTime.toEpochMilli();
        currentModelId.set(modelId);
        currentChunkedDoc.set(0);
        persistedChunkLengths.set(0);
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
            .setEstimatedHeapMemory(modelSize.ramBytesUsed())
            .setEstimatedOperations(modelSize.numOperations())
            .setInput(new TrainedModelInput(fieldNamesWithoutDependentVariable))
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setDefaultFieldMap(defaultFieldMapping)
            .setInferenceConfig(buildInferenceConfigByAnalyticsType())
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

    InferenceConfig buildInferenceConfigByAnalyticsType() {
        if (analytics.getAnalysis() instanceof Classification) {
            Classification classification = ((Classification)analytics.getAnalysis());
            PredictionFieldType predictionFieldType = getPredictionFieldType(classification);
            return ClassificationConfig.builder()
                .setNumTopClasses(classification.getNumTopClasses())
                .setNumTopFeatureImportanceValues(classification.getBoostedTreeParams().getNumTopFeatureImportanceValues())
                .setPredictionFieldType(predictionFieldType)
                .build();
        } else if (analytics.getAnalysis() instanceof Regression) {
            Regression regression = ((Regression)analytics.getAnalysis());
            return RegressionConfig.builder()
                .setNumTopFeatureImportanceValues(regression.getBoostedTreeParams().getNumTopFeatureImportanceValues())
                .build();
        }
        throw ExceptionsHelper.serverError(
            "analytics type [{}] does not support model creation",
            null,
            analytics.getAnalysis().getWriteableName());
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
}
