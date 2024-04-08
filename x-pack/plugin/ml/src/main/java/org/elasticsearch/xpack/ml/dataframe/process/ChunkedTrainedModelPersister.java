/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.license.License;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TrainedModelMetadata;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.ml.dataframe.process.results.ModelMetadata;
import org.elasticsearch.xpack.ml.dataframe.process.results.TrainedModelDefinitionChunk;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.MultiField;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfo;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Instant;
import java.util.ArrayList;
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
import static org.elasticsearch.core.Strings.format;

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

    public ChunkedTrainedModelPersister(
        TrainedModelProvider provider,
        DataFrameAnalyticsConfig analytics,
        DataFrameAnalyticsAuditor auditor,
        Consumer<Exception> failureHandler,
        ExtractedFields extractedFields
    ) {
        this.provider = provider;
        this.currentModelId = new AtomicReference<>("");
        this.analytics = analytics;
        this.auditor = auditor;
        this.failureHandler = failureHandler;
        this.extractedFields = extractedFields;
    }

    public void createAndIndexInferenceModelDoc(TrainedModelDefinitionChunk trainedModelDefinitionChunk) {
        if (readyToStoreNewModel.get()) {
            failureHandler.accept(
                ExceptionsHelper.serverError(
                    "chunked inference model definition is attempting to be stored before trained model configuration"
                )
            );
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

    public String createAndIndexInferenceModelConfig(ModelSizeInfo inferenceModelSize, TrainedModelType trainedModelType) {
        if (readyToStoreNewModel.compareAndSet(true, false) == false) {
            failureHandler.accept(
                ExceptionsHelper.serverError("new inference model is attempting to be stored before completion previous model storage")
            );
            return null;
        }
        TrainedModelConfig trainedModelConfig = createTrainedModelConfig(trainedModelType, inferenceModelSize);
        CountDownLatch latch = storeTrainedModelConfig(trainedModelConfig);
        try {
            if (latch.await(STORE_TIMEOUT_SEC, TimeUnit.SECONDS) == false) {
                LOGGER.error("[{}] Timed out (30s) waiting for inference model config to be stored", analytics.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.readyToStoreNewModel.set(true);
            failureHandler.accept(ExceptionsHelper.serverError("interrupted waiting for inference model config to be stored"));
        }
        return trainedModelConfig.getModelId();
    }

    public void createAndIndexInferenceModelMetadata(ModelMetadata modelMetadata) {
        if (Strings.isNullOrEmpty(this.currentModelId.get())) {
            failureHandler.accept(
                ExceptionsHelper.serverError("inference model metadata is attempting to be stored before trained model configuration")
            );
            return;
        }
        TrainedModelMetadata trainedModelMetadata = new TrainedModelMetadata(
            this.currentModelId.get(),
            modelMetadata.getFeatureImportances(),
            modelMetadata.getFeatureImportanceBaseline(),
            modelMetadata.getHyperparameters()
        );

        CountDownLatch latch = storeTrainedModelMetadata(trainedModelMetadata);
        try {
            if (latch.await(STORE_TIMEOUT_SEC, TimeUnit.SECONDS) == false) {
                LOGGER.error("[{}] Timed out (30s) waiting for inference model metadata to be stored", analytics.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failureHandler.accept(ExceptionsHelper.serverError("interrupted waiting for inference model metadata to be stored"));
        }
    }

    private CountDownLatch storeTrainedModelDoc(TrainedModelDefinitionDoc trainedModelDefinitionDoc) {
        CountDownLatch latch = new CountDownLatch(1);

        // Latch is attached to this action as it is the last one to execute.
        ActionListener<BroadcastResponse> refreshListener = new LatchedActionListener<>(ActionListener.wrap(refreshed -> {
            if (refreshed != null) {
                LOGGER.debug(() -> "[" + analytics.getId() + "] refreshed inference index after model store");
            }
        }, e -> LOGGER.warn(() -> "[" + analytics.getId() + "] failed to refresh inference index after model store", e)), latch);

        // First, store the model and refresh is necessary
        ActionListener<Void> storeListener = ActionListener.wrap(r -> {
            LOGGER.debug(
                () -> format(
                    "[%s] stored trained model definition chunk [%s] [%s]",
                    analytics.getId(),
                    trainedModelDefinitionDoc.getModelId(),
                    trainedModelDefinitionDoc.getDocNum()
                )
            );
            if (trainedModelDefinitionDoc.isEos() == false) {
                refreshListener.onResponse(null);
                return;
            }
            LOGGER.info("[{}] finished storing trained model with id [{}]", analytics.getId(), this.currentModelId.get());
            auditor.info(analytics.getId(), "Stored trained model with id [" + this.currentModelId.get() + "]");
            readyToStoreNewModel.set(true);
            provider.refreshInferenceIndex(refreshListener);
        }, e -> {
            LOGGER.error(
                () -> format(
                    "[%s] error storing trained model definition chunk [%s] with id [%s]",
                    analytics.getId(),
                    trainedModelDefinitionDoc.getDocNum(),
                    trainedModelDefinitionDoc.getModelId()
                ),
                e
            );
            this.readyToStoreNewModel.set(true);
            failureHandler.accept(
                ExceptionsHelper.serverError(
                    "error storing trained model definition chunk [{}] with id [{}]",
                    e,
                    trainedModelDefinitionDoc.getDocNum(),
                    trainedModelDefinitionDoc.getModelId()
                )
            );
            refreshListener.onResponse(null);
        });
        provider.storeTrainedModelDefinitionDoc(trainedModelDefinitionDoc, storeListener);
        return latch;
    }

    private CountDownLatch storeTrainedModelMetadata(TrainedModelMetadata trainedModelMetadata) {
        CountDownLatch latch = new CountDownLatch(1);

        // Latch is attached to this action as it is the last one to execute.
        ActionListener<BroadcastResponse> refreshListener = new LatchedActionListener<>(ActionListener.wrap(refreshed -> {
            if (refreshed != null) {
                LOGGER.debug(() -> "[" + analytics.getId() + "] refreshed inference index after model metadata store");
            }
        }, e -> LOGGER.warn(() -> "[" + analytics.getId() + "] failed to refresh inference index after model metadata store", e)), latch);

        // First, store the model and refresh is necessary
        ActionListener<Void> storeListener = ActionListener.wrap(r -> {
            LOGGER.debug("[{}] stored trained model metadata with id [{}]", analytics.getId(), this.currentModelId.get());
            readyToStoreNewModel.set(true);
            provider.refreshInferenceIndex(refreshListener);
        }, e -> {
            LOGGER.error(
                () -> format(
                    "[%s] error storing trained model metadata with id [%s]",
                    analytics.getId(),
                    trainedModelMetadata.getModelId()
                ),
                e
            );
            this.readyToStoreNewModel.set(true);
            failureHandler.accept(
                ExceptionsHelper.serverError("error storing trained model metadata with id [{}]", e, trainedModelMetadata.getModelId())
            );
            refreshListener.onResponse(null);
        });
        provider.storeTrainedModelMetadata(trainedModelMetadata, storeListener);
        return latch;
    }

    private CountDownLatch storeTrainedModelConfig(TrainedModelConfig trainedModelConfig) {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Boolean> storeListener = ActionListener.wrap(aBoolean -> {
            if (aBoolean == false) {
                LOGGER.error("[{}] Storing trained model config responded false", analytics.getId());
                readyToStoreNewModel.set(true);
                failureHandler.accept(ExceptionsHelper.serverError("storing trained model config false"));
            } else {
                LOGGER.debug("[{}] Stored trained model config with id [{}]", analytics.getId(), trainedModelConfig.getModelId());
            }
        }, e -> {
            LOGGER.error(
                () -> format("[%s] error storing trained model config with id [%s]", analytics.getId(), trainedModelConfig.getModelId()),
                e
            );
            readyToStoreNewModel.set(true);
            failureHandler.accept(
                ExceptionsHelper.serverError("error storing trained model config with id [{}]", e, trainedModelConfig.getModelId())
            );
        });
        provider.storeTrainedModelConfig(trainedModelConfig, new LatchedActionListener<>(storeListener, latch));
        return latch;
    }

    private long customProcessorSize() {
        List<PreProcessor> preProcessors = new ArrayList<>();
        final DataFrameAnalysis analysis = analytics.getAnalysis();
        if (analysis instanceof Classification classification) {
            preProcessors = classification.getFeatureProcessors();
        } else if (analysis instanceof Regression regression) {
            preProcessors = regression.getFeatureProcessors();
        }
        return preProcessors.stream().mapToLong(PreProcessor::ramBytesUsed).sum() + RamUsageEstimator.NUM_BYTES_OBJECT_REF
            * (long) preProcessors.size();
    }

    private TrainedModelConfig createTrainedModelConfig(TrainedModelType trainedModelType, ModelSizeInfo modelSize) {
        Instant createTime = Instant.now();
        // The native process does not provide estimates for the custom feature_processor objects
        long customProcessorSize = customProcessorSize();
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
            .setModelType(trainedModelType)
            .setCreatedBy(InternalUsers.XPACK_USER.principal())
            .setVersion(MlConfigVersion.CURRENT)
            .setCreateTime(createTime)
            // NOTE: GET _cat/ml/trained_models relies on the creating analytics ID being in the tags
            .setTags(Collections.singletonList(analytics.getId()))
            .setDescription(analytics.getDescription())
            .setMetadata(
                Collections.singletonMap(
                    "analytics_config",
                    XContentHelper.convertToMap(JsonXContent.jsonXContent, analytics.toString(), true)
                )
            )
            .setModelSize(modelSize.ramBytesUsed() + customProcessorSize)
            .setEstimatedOperations(modelSize.numOperations())
            .setInput(new TrainedModelInput(fieldNamesWithoutDependentVariable))
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setDefaultFieldMap(defaultFieldMapping)
            .setInferenceConfig(analytics.getAnalysis().inferenceConfig(new AnalysisFieldInfo(extractedFields)))
            .build();
    }

    private String getDependentVariable() {
        final DataFrameAnalysis analysis = analytics.getAnalysis();
        if (analysis instanceof Classification classification) {
            return classification.getDependentVariable();
        }
        if (analysis instanceof Regression regression) {
            return regression.getDependentVariable();
        }
        return null;
    }

}
