/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING;

/**
 * LocalModels implement reference counting for proper accounting in
 * the {@link CircuitBreaker}. When the model is not longer used {@link #release()}
 * must be called and if the reference count == 0 then the model's bytes
 * will be removed from the circuit breaker.
 *
 * The class is constructed with an initial reference count of 1 and its
 * bytes <em>must</em> have been added to the circuit breaker before construction.
 * New references must call {@link #acquire()} and {@link #release()} as the model
 * is used.
 */
public class LocalModel implements Closeable {

    private final InferenceDefinition trainedModelDefinition;
    private final String modelId;
    private final Set<String> fieldNames;
    private final Map<String, String> defaultFieldMap;
    private final InferenceStats.Accumulator statsAccumulator;
    private final TrainedModelStatsService trainedModelStatsService;
    private volatile long persistenceQuotient = 100;
    private final LongAdder currentInferenceCount;
    private final InferenceConfig inferenceConfig;
    private final License.OperationMode licenseLevel;
    private final CircuitBreaker trainedModelCircuitBreaker;
    private final AtomicLong referenceCount;
    private final long cachedRamBytesUsed;

    LocalModel(
        String modelId,
        String nodeId,
        InferenceDefinition trainedModelDefinition,
        TrainedModelInput input,
        Map<String, String> defaultFieldMap,
        InferenceConfig modelInferenceConfig,
        License.OperationMode licenseLevel,
        TrainedModelStatsService trainedModelStatsService,
        CircuitBreaker trainedModelCircuitBreaker
    ) {
        this.trainedModelDefinition = trainedModelDefinition;
        this.cachedRamBytesUsed = trainedModelDefinition.ramBytesUsed();
        this.modelId = modelId;
        this.fieldNames = new HashSet<>(input.getFieldNames());
        // the ctor being called means a new instance was created.
        // Consequently, it was not loaded from cache and on stats persist we should increment accordingly.
        this.statsAccumulator = new InferenceStats.Accumulator(modelId, nodeId, 1L);
        this.trainedModelStatsService = trainedModelStatsService;
        this.defaultFieldMap = defaultFieldMap == null ? null : new HashMap<>(defaultFieldMap);
        this.currentInferenceCount = new LongAdder();
        this.inferenceConfig = modelInferenceConfig;
        this.licenseLevel = licenseLevel;
        this.trainedModelCircuitBreaker = trainedModelCircuitBreaker;
        this.referenceCount = new AtomicLong(1);
    }

    long ramBytesUsed() {
        // This should always be cached and not calculated on call.
        // This is because the caching system calls this method on every promotion call that changes the LRU head
        // Consequently, recalculating can cause serious throughput issues due to LRU changes in the cache
        return cachedRamBytesUsed;
    }

    public String getModelId() {
        return modelId;
    }

    public License.OperationMode getLicenseLevel() {
        return licenseLevel;
    }

    public InferenceStats getLatestStatsAndReset() {
        return statsAccumulator.currentStatsAndReset();
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

    /**
     * Infers without updating the stats.
     * This is mainly for usage by data frame analytics jobs
     * when they do inference against test data.
     */
    public InferenceResults inferNoStats(Map<String, Object> fields) {
        LocalModel.mapFieldsIfNecessary(fields, defaultFieldMap);
        Map<String, Object> flattenedFields = MapHelper.dotCollapse(fields, fieldNames);
        if (flattenedFields.isEmpty()) {
            new WarningInferenceResults(Messages.getMessage(INFERENCE_WARNING_ALL_FIELDS_MISSING, modelId));
        }
        return trainedModelDefinition.infer(flattenedFields, inferenceConfig);
    }

    public Collection<String> inputFields() {
        return fieldNames;
    }

    public void infer(Map<String, Object> fields, InferenceConfigUpdate update, ActionListener<InferenceResults> listener) {
        if (update.isSupported(this.inferenceConfig) == false) {
            listener.onFailure(
                ExceptionsHelper.badRequestException(
                    "Model [{}] has inference config of type [{}] which is not supported by inference request of type [{}]",
                    this.modelId,
                    this.inferenceConfig.getName(),
                    update.getName()
                )
            );
            return;
        }
        try {
            statsAccumulator.incInference();
            currentInferenceCount.increment();

            // Needs to happen before collapse as defaultFieldMap might resolve fields to their appropriate name
            LocalModel.mapFieldsIfNecessary(fields, defaultFieldMap);

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

    public InferenceResults infer(Map<String, Object> fields, InferenceConfigUpdate update) throws Exception {
        AtomicReference<InferenceResults> result = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<InferenceResults> listener = ActionListener.wrap(result::set, exception::set);

        infer(fields, update, listener);
        if (exception.get() != null) {
            throw exception.get();
        }

        return result.get();
    }

    /**
     * Used for translating field names in according to the passed `fieldMappings` parameter.
     *
     * This mutates the `fields` parameter in-place.
     *
     * Fields are only appended. If the expected field name already exists, it is not created/overwritten.
     *
     * Original fields are not deleted.
     *
     * @param fields Fields to map against
     * @param fieldMapping Field originalName to expectedName string mapping
     */
    public static void mapFieldsIfNecessary(Map<String, Object> fields, Map<String, String> fieldMapping) {
        if (fieldMapping != null) {
            fieldMapping.forEach((src, dest) -> {
                Object srcValue = MapHelper.dig(src, fields);
                if (srcValue != null) {
                    fields.putIfAbsent(dest, srcValue);
                }
            });
        }
    }

    long acquire() {
        long count = referenceCount.incrementAndGet();
        // protect against a race where the model could be release to a
        // count of zero then the model is quickly re-acquired
        if (count == 1) {
            trainedModelCircuitBreaker.addEstimateBytesAndMaybeBreak(trainedModelDefinition.ramBytesUsed(), modelId);
        }
        return count;
    }

    public long getReferenceCount() {
        return referenceCount.get();
    }

    public long release() {
        long count = referenceCount.decrementAndGet();
        assert count >= 0;
        if (count == 0) {
            // no references to this model, it no longer needs to be accounted for
            trainedModelCircuitBreaker.addWithoutBreaking(-ramBytesUsed());
        }
        return referenceCount.get();
    }

    /**
     * Convenience method so the class can be used in try-with-resource
     * constructs to invoke {@link #release()}.
     */
    public void close() {
        release();
    }

    @Override
    public String toString() {
        return "LocalModel{"
            + "trainedModelDefinition="
            + trainedModelDefinition
            + ", modelId='"
            + modelId
            + '\''
            + ", fieldNames="
            + fieldNames
            + ", defaultFieldMap="
            + defaultFieldMap
            + ", statsAccumulator="
            + statsAccumulator
            + ", trainedModelStatsService="
            + trainedModelStatsService
            + ", persistenceQuotient="
            + persistenceQuotient
            + ", currentInferenceCount="
            + currentInferenceCount
            + ", inferenceConfig="
            + inferenceConfig
            + ", licenseLevel="
            + licenseLevel
            + ", trainedModelCircuitBreaker="
            + trainedModelCircuitBreaker
            + ", referenceCount="
            + referenceCount
            + '}';
    }
}
