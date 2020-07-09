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
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.dataframe.process.results.TrainedModelDefinitionChunk;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.MultiField;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfo;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class ChunkedTrainedModelPersister {

    private static final Logger LOGGER = LogManager.getLogger(ChunkedTrainedModelPersister.class);
    private static final int STORE_TIMEOUT_SEC = 30;
    private final TrainedModelProvider provider;
    private final AtomicReference<String> currentModelId;
    private final DataFrameAnalyticsConfig analytics;
    private final DataFrameAnalyticsAuditor auditor;
    private final Consumer<Exception> failureHandler;
    private final ExtractedFields extractedFields;
    private final AtomicBoolean readyToStoreNewModel = new AtomicBoolean(true);

    public ChunkedTrainedModelPersister(TrainedModelProvider provider,
                                        DataFrameAnalyticsConfig analytics,
                                        DataFrameAnalyticsAuditor auditor,
                                        Consumer<Exception> failureHandler,
                                        ExtractedFields extractedFields) {
        this.provider = provider;
        this.currentModelId = new AtomicReference<>("");
        this.analytics = analytics;
        this.auditor = auditor;
        this.failureHandler = failureHandler;
        this.extractedFields = extractedFields;
    }

    public void createAndIndexInferenceModelDoc(TrainedModelDefinitionChunk trainedModelDefinitionChunk) {
        if (Strings.isNullOrEmpty(this.currentModelId.get())) {
            failureHandler.accept(ExceptionsHelper.serverError(
                "chunked inference model definition is attempting to be stored before trained model configuration"
            ));
            return;
        }
        TrainedModelDefinitionDoc trainedModelDefinitionDoc = trainedModelDefinitionChunk.createTrainedModelDoc(this.currentModelId.get());

        CountDownLatch latch = storeTrainedModelDoc(trainedModelDefinitionDoc);
        try {
            if (latch.await(STORE_TIMEOUT_SEC, TimeUnit.SECONDS) == false) {
                LOGGER.error("[{}] Timed out (30s) waiting for chunked inference definition to be stored", analytics.getId());
                if (trainedModelDefinitionChunk.isEos()) {
                    this.readyToStoreNewModel.set(true);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.readyToStoreNewModel.set(true);
            failureHandler.accept(ExceptionsHelper.serverError("interrupted waiting for chunked inference definition to be stored"));
        }
    }

    public String createAndIndexInferenceModelMetadata(ModelSizeInfo inferenceModelSize) {
        if (readyToStoreNewModel.compareAndSet(true, false) == false) {
            failureHandler.accept(ExceptionsHelper.serverError(
                "new inference model is attempting to be stored before completion previous model storage"
            ));
            return null;
        }
        TrainedModelConfig trainedModelConfig = createTrainedModelConfig(inferenceModelSize);
        CountDownLatch latch = storeTrainedModelMetadata(trainedModelConfig);
        try {
            if (latch.await(STORE_TIMEOUT_SEC, TimeUnit.SECONDS) == false) {
                LOGGER.error("[{}] Timed out (30s) waiting for inference model metadata to be stored", analytics.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.readyToStoreNewModel.set(true);
            failureHandler.accept(ExceptionsHelper.serverError("interrupted waiting for inference model metadata to be stored"));
        }
        return trainedModelConfig.getModelId();
    }

    private CountDownLatch storeTrainedModelDoc(TrainedModelDefinitionDoc trainedModelDefinitionDoc) {
        CountDownLatch latch = new CountDownLatch(1);

        // Latch is attached to this action as it is the last one to execute.
        ActionListener<RefreshResponse> refreshListener = new LatchedActionListener<>(ActionListener.wrap(
            refreshed -> {
                if (refreshed != null) {
                    LOGGER.debug(() -> new ParameterizedMessage(
                        "[{}] refreshed inference index after model store",
                        analytics.getId()
                    ));
                }
            },
            e -> LOGGER.warn(
                new ParameterizedMessage("[{}] failed to refresh inference index after model store", analytics.getId()),
                e)
        ), latch);

        // First, store the model and refresh is necessary
        ActionListener<Void> storeListener = ActionListener.wrap(
            r -> {
                LOGGER.debug(() -> new ParameterizedMessage(
                    "[{}] stored trained model definition chunk [{}] [{}]",
                    analytics.getId(),
                    trainedModelDefinitionDoc.getModelId(),
                    trainedModelDefinitionDoc.getDocNum()));
                if (trainedModelDefinitionDoc.isEos() == false) {
                    refreshListener.onResponse(null);
                    return;
                }
                LOGGER.info(
                    "[{}] finished storing trained model with id [{}]",
                    analytics.getId(),
                    this.currentModelId.get());
                auditor.info(analytics.getId(), "Stored trained model with id [" + this.currentModelId.get() + "]");
                this.currentModelId.set("");
                readyToStoreNewModel.set(true);
                provider.refreshInferenceIndex(refreshListener);
            },
            e -> {
                this.readyToStoreNewModel.set(true);
                failureHandler.accept(ExceptionsHelper.serverError(
                    "error storing trained model definition chunk [{}] with id [{}]",
                    e,
                    trainedModelDefinitionDoc.getModelId(),
                    trainedModelDefinitionDoc.getDocNum()));
                refreshListener.onResponse(null);
            }
        );
        provider.storeTrainedModelDefinitionDoc(trainedModelDefinitionDoc, storeListener);
        return latch;
    }
    private CountDownLatch storeTrainedModelMetadata(TrainedModelConfig trainedModelConfig) {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Boolean> storeListener = ActionListener.wrap(
            aBoolean -> {
                if (aBoolean == false) {
                    LOGGER.error("[{}] Storing trained model metadata responded false", analytics.getId());
                    readyToStoreNewModel.set(true);
                    failureHandler.accept(ExceptionsHelper.serverError("storing trained model responded false"));
                } else {
                    LOGGER.debug("[{}] Stored trained model metadata with id [{}]", analytics.getId(), trainedModelConfig.getModelId());
                }
            },
            e -> {
                readyToStoreNewModel.set(true);
                failureHandler.accept(ExceptionsHelper.serverError("error storing trained model metadata with id [{}]",
                    e,
                    trainedModelConfig.getModelId()));
            }
        );
        provider.storeTrainedModelMetadata(trainedModelConfig, new LatchedActionListener<>(storeListener, latch));
        return latch;
    }

    private TrainedModelConfig createTrainedModelConfig(ModelSizeInfo modelSize) {
        Instant createTime = Instant.now();
        String modelId = analytics.getId() + "-" + createTime.toEpochMilli();
        currentModelId.set(modelId);
        List<ExtractedField> fieldNames = extractedFields.getAllFields();
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
            .setInferenceConfig(analytics.getAnalysis().inferenceConfig(new AnalysisFieldInfo(extractedFields)))
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

}
