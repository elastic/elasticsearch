/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING;

public class LocalModel implements Model {

    private final InferenceDefinition trainedModelDefinition;
    private final String modelId;
    private final Set<String> fieldNames;
    private final Map<String, String> defaultFieldMap;
    private final InferenceStats.Accumulator statsAccumulator;
    private final TrainedModelStatsService trainedModelStatsService;
    private volatile long persistenceQuotient = 100;
    private final LongAdder currentInferenceCount;
    private final InferenceConfig inferenceConfig;

    public LocalModel(String modelId,
                      String nodeId,
                      InferenceDefinition trainedModelDefinition,
                      TrainedModelInput input,
                      Map<String, String> defaultFieldMap,
                      InferenceConfig modelInferenceConfig,
                      TrainedModelStatsService trainedModelStatsService) {
        this.trainedModelDefinition = trainedModelDefinition;
        this.modelId = modelId;
        this.fieldNames = new HashSet<>(input.getFieldNames());
        // the ctor being called means a new instance was created.
        // Consequently, it was not loaded from cache and on stats persist we should increment accordingly.
        this.statsAccumulator = new InferenceStats.Accumulator(modelId, nodeId, 1L);
        this.trainedModelStatsService = trainedModelStatsService;
        this.defaultFieldMap = defaultFieldMap == null ? null : new HashMap<>(defaultFieldMap);
        this.currentInferenceCount = new LongAdder();
        this.inferenceConfig = modelInferenceConfig;
    }

    long ramBytesUsed() {
        return trainedModelDefinition.ramBytesUsed();
    }

    @Override
    public String getModelId() {
        return modelId;
    }

    @Override
    public InferenceStats getLatestStatsAndReset() {
        return statsAccumulator.currentStatsAndReset();
    }

    @Override
    public String getResultsType() {
        switch (trainedModelDefinition.getTargetType()) {
            case CLASSIFICATION:
                return ClassificationInferenceResults.NAME;
            case REGRESSION:
                return RegressionInferenceResults.NAME;
            default:
                throw ExceptionsHelper.badRequestException("Model [{}] has unsupported target type [{}]",
                    modelId,
                    trainedModelDefinition.getTargetType());
        }
    }

    void persistStats(boolean flush) {
        trainedModelStatsService.queueStats(getLatestStatsAndReset(), flush);
        if (persistenceQuotient < 1000 && currentInferenceCount.sum() > 1000) {
            persistenceQuotient = 1000;
        }
        if (persistenceQuotient < 10_000 && currentInferenceCount.sum() > 10_000) {
            persistenceQuotient = 10_000;
        }
    }

    @Override
    public void infer(Map<String, Object> fields, InferenceConfigUpdate update, ActionListener<InferenceResults> listener) {
        if (update.isSupported(this.inferenceConfig) == false) {
            listener.onFailure(ExceptionsHelper.badRequestException(
                "Model [{}] has inference config of type [{}] which is not supported by inference request of type [{}]",
                this.modelId,
                this.inferenceConfig.getName(),
                update.getName()));
            return;
        }
        try {
            statsAccumulator.incInference();
            currentInferenceCount.increment();

            // Needs to happen before collapse as defaultFieldMap might resolve fields to their appropriate name
            Model.mapFieldsIfNecessary(fields, defaultFieldMap);

            Map<String, Object> flattenedFields = MapHelper.dotCollapse(fields, fieldNames);
            boolean shouldPersistStats = ((currentInferenceCount.sum() + 1) % persistenceQuotient == 0);
            if (flattenedFields.isEmpty()) {
                statsAccumulator.incMissingFields();
                if (shouldPersistStats) {
                    persistStats(false);
                }
                listener.onResponse(new WarningInferenceResults(Messages.getMessage(INFERENCE_WARNING_ALL_FIELDS_MISSING, modelId)));
                return;
            }
            InferenceResults inferenceResults = trainedModelDefinition.infer(flattenedFields, update.apply(inferenceConfig));
            if (shouldPersistStats) {
                persistStats(false);
            }
            listener.onResponse(inferenceResults);
        } catch (Exception e) {
            statsAccumulator.incFailure();
            listener.onFailure(e);
        }
    }

}
