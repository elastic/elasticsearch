/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.dataframe.stats.DataCountsTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.utils.persistence.LimitAwareBulkIndexer;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class InferenceRunner {

    private static final Logger LOGGER = LogManager.getLogger(InferenceRunner.class);

    private static final int MAX_PROGRESS_BEFORE_COMPLETION = 98;

    private final Settings settings;
    private final Client client;
    private final ModelLoadingService modelLoadingService;
    private final ResultsPersisterService resultsPersisterService;
    private final TaskId parentTaskId;
    private final DataFrameAnalyticsConfig config;
    private final ExtractedFields extractedFields;
    private final ProgressTracker progressTracker;
    private final DataCountsTracker dataCountsTracker;
    private volatile boolean isCancelled;

    public InferenceRunner(Settings settings, Client client, ModelLoadingService modelLoadingService,
                           ResultsPersisterService resultsPersisterService, TaskId parentTaskId, DataFrameAnalyticsConfig config,
                           ExtractedFields extractedFields, ProgressTracker progressTracker, DataCountsTracker dataCountsTracker) {
        this.settings = Objects.requireNonNull(settings);
        this.client = Objects.requireNonNull(client);
        this.modelLoadingService = Objects.requireNonNull(modelLoadingService);
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.parentTaskId = Objects.requireNonNull(parentTaskId);
        this.config = Objects.requireNonNull(config);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.progressTracker = Objects.requireNonNull(progressTracker);
        this.dataCountsTracker = Objects.requireNonNull(dataCountsTracker);
    }

    public void cancel() {
        isCancelled = true;
    }

    public void run(String modelId) {
        if (isCancelled) {
            return;
        }

        LOGGER.info("[{}] Started inference on test data against model [{}]", config.getId(), modelId);
        try {
            PlainActionFuture<LocalModel> localModelPlainActionFuture = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(modelId, localModelPlainActionFuture);
            TestDocsIterator testDocsIterator = new TestDocsIterator(new OriginSettingClient(client, ClientHelper.ML_ORIGIN), config,
                extractedFields);
            try (LocalModel localModel = localModelPlainActionFuture.actionGet()) {
                LOGGER.debug("Loaded inference model [{}]", localModel);
                inferTestDocs(localModel, testDocsIterator);
            }
        } catch (Exception e) {
            LOGGER.error(new ParameterizedMessage("[{}] Error during inference against model [{}]", config.getId(), modelId), e);
            throw ExceptionsHelper.serverError("[{}] failed running inference on model [{}]; cause was [{}]", e, config.getId(), modelId,
                e.getMessage());
        }
    }

    // Visible for testing
    void inferTestDocs(LocalModel model, TestDocsIterator testDocsIterator) {
        long totalDocCount = 0;
        long processedDocCount = 0;

        try (LimitAwareBulkIndexer bulkIndexer = new LimitAwareBulkIndexer(settings, this::executeBulkRequest)) {
            while (testDocsIterator.hasNext()) {
                if (isCancelled) {
                    break;
                }

                Deque<SearchHit> batch = testDocsIterator.next();

                if (totalDocCount == 0) {
                    totalDocCount = testDocsIterator.getTotalHits();
                }

                for (SearchHit doc : batch) {
                    dataCountsTracker.incrementTestDocsCount();
                    InferenceResults inferenceResults = model.inferNoStats(featuresFromDoc(doc));
                    bulkIndexer.addAndExecuteIfNeeded(createIndexRequest(doc, inferenceResults, config.getDest().getResultsField()));

                    processedDocCount++;
                    int progressPercent = Math.min((int) (processedDocCount * 100.0 / totalDocCount), MAX_PROGRESS_BEFORE_COMPLETION);
                    progressTracker.updateInferenceProgress(progressPercent);
                }
            }
        }

        if (isCancelled == false) {
            progressTracker.updateInferenceProgress(100);
        }
    }

    private Map<String, Object> featuresFromDoc(SearchHit doc) {
        Map<String, Object> features = new HashMap<>();
        for (ExtractedField extractedField : extractedFields.getAllFields()) {
            Object[] values = extractedField.value(doc);
            if (values.length == 1) {
                features.put(extractedField.getName(), values[0]);
            }
        }
        return features;
    }

    private IndexRequest createIndexRequest(SearchHit hit, InferenceResults results, String resultField) {
        Map<String, Object> resultsMap = new LinkedHashMap<>(results.asMap());
        resultsMap.put(DestinationIndex.IS_TRAINING, false);

        Map<String, Object> source = new LinkedHashMap<>(hit.getSourceAsMap());
        source.put(resultField, resultsMap);
        IndexRequest indexRequest = new IndexRequest(hit.getIndex());
        indexRequest.id(hit.getId());
        indexRequest.source(source);
        indexRequest.opType(DocWriteRequest.OpType.INDEX);
        indexRequest.setParentTask(parentTaskId);
        return indexRequest;
    }

    private void executeBulkRequest(BulkRequest bulkRequest) {
        resultsPersisterService.bulkIndexWithHeadersWithRetry(
            config.getHeaders(),
            bulkRequest,
            config.getId(),
            () -> isCancelled == false,
            errorMsg -> {});
    }
}
