/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.steps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.dataframe.inference.InferenceRunner;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceStep extends AbstractDataFrameAnalyticsStep {

    private static final Logger LOGGER = LogManager.getLogger(InferenceStep.class);

    private final ThreadPool threadPool;
    private final InferenceRunner inferenceRunner;

    public InferenceStep(
        NodeClient client,
        DataFrameAnalyticsTask task,
        DataFrameAnalyticsAuditor auditor,
        DataFrameAnalyticsConfig config,
        ThreadPool threadPool,
        InferenceRunner inferenceRunner
    ) {
        super(client, task, auditor, config);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.inferenceRunner = Objects.requireNonNull(inferenceRunner);
    }

    @Override
    public Name name() {
        return Name.INFERENCE;
    }

    @Override
    protected void doExecute(ActionListener<StepResponse> listener) {
        if (config.getAnalysis().supportsInference() == false) {
            LOGGER.debug(() -> format("[%s] Inference step completed immediately as analysis does not support inference", config.getId()));
            listener.onResponse(new StepResponse(false));
            return;
        }

        refreshDestAsync(
            listener.delegateFailureAndWrap(
                (delegate, refreshResponse) -> searchIfTestDocsExist(delegate.delegateFailureAndWrap((delegate2, testDocsExist) -> {
                    if (testDocsExist) {
                        getModelId(delegate2.delegateFailureAndWrap((l, modelId) -> runInference(modelId, l)));
                    } else {
                        // no need to run inference at all so let us skip
                        // loading the model in memory.
                        LOGGER.debug(() -> "[" + config.getId() + "] Inference step completed immediately as there are no test docs");
                        task.getStatsHolder().getProgressTracker().updateInferenceProgress(100);
                        delegate2.onResponse(new StepResponse(isTaskStopping()));
                    }
                }))
            )
        );
    }

    private void runInference(String modelId, ActionListener<StepResponse> listener) {
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(ActionRunnable.wrap(listener, delegate -> {
            inferenceRunner.run(modelId, ActionListener.wrap(aVoid -> delegate.onResponse(new StepResponse(isTaskStopping())), e -> {
                if (task.isStopping()) {
                    delegate.onResponse(new StepResponse(false));
                } else {
                    delegate.onFailure(e);
                }
            }));
        }));
    }

    private void searchIfTestDocsExist(ActionListener<Boolean> listener) {
        SearchRequest searchRequest = new SearchRequest(config.getDest().getIndex());
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
        searchRequest.source()
            .query(
                QueryBuilders.boolQuery()
                    .mustNot(QueryBuilders.termQuery(config.getDest().getResultsField() + "." + DestinationIndex.IS_TRAINING, true))
            );
        searchRequest.source().size(0);
        searchRequest.source().trackTotalHitsUpTo(1);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            TransportSearchAction.TYPE,
            searchRequest,
            listener.delegateFailureAndWrap((l, searchResponse) -> l.onResponse(searchResponse.getHits().getTotalHits().value() > 0))
        );
    }

    private void getModelId(ActionListener<String> listener) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(1);
        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.query(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(TrainedModelConfig.TAGS.getPreferredName(), config.getId()))
        );
        searchSourceBuilder.sort(TrainedModelConfig.CREATE_TIME.getPreferredName(), SortOrder.DESC);
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN);
        searchRequest.source(searchSourceBuilder);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            TransportSearchAction.TYPE,
            searchRequest,
            listener.delegateFailureAndWrap((l, searchResponse) -> {
                SearchHit[] hits = searchResponse.getHits().getHits();
                if (hits.length == 0) {
                    l.onFailure(new ResourceNotFoundException("No model could be found to perform inference"));
                } else {
                    l.onResponse(hits[0].getId());
                }
            })
        );
    }

    @Override
    public void cancel(String reason, TimeValue timeout) {
        inferenceRunner.cancel();
    }

    @Override
    public void updateProgress(ActionListener<Void> listener) {
        // Inference runner updates progress directly
        listener.onResponse(null);
    }
}
