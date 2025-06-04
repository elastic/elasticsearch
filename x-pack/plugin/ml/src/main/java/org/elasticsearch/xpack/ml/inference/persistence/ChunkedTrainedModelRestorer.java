/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ml.MachineLearning.NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;

/**
 * Searches for and emits {@link TrainedModelDefinitionDoc}s in
 * order based on the {@code doc_num}.
 *
 * This is a one-use class it has internal state to track progress
 * and cannot be used again to load another model.
 *
 * Defaults to searching in {@link InferenceIndexConstants#INDEX_PATTERN}
 * if a different index is not set.
 */
public class ChunkedTrainedModelRestorer {

    private static final Logger logger = LogManager.getLogger(ChunkedTrainedModelRestorer.class);

    private static final int MAX_NUM_DEFINITION_DOCS = 20;
    private static final int SEARCH_RETRY_LIMIT = 5;
    private static final TimeValue SEARCH_FAILURE_RETRY_WAIT_TIME = new TimeValue(5, TimeUnit.SECONDS);

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final ExecutorService executorService;
    private final String modelId;
    private String index = InferenceIndexConstants.INDEX_PATTERN;
    private int searchSize = 10;
    private int numDocsWritten = 0;

    public ChunkedTrainedModelRestorer(
        String modelId,
        Client client,
        ExecutorService executorService,
        NamedXContentRegistry xContentRegistry
    ) {
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.executorService = executorService;
        this.xContentRegistry = xContentRegistry;
        this.modelId = modelId;
    }

    public void setSearchSize(int searchSize) {
        if (searchSize > MAX_NUM_DEFINITION_DOCS) {
            throw new IllegalArgumentException("search size [" + searchSize + "] cannot be bigger than [" + MAX_NUM_DEFINITION_DOCS + "]");
        }
        if (searchSize <= 0) {
            throw new IllegalArgumentException("search size [" + searchSize + "] must be greater than 0");
        }
        this.searchSize = searchSize;
    }

    public void setSearchIndex(String indexNameOrPattern) {
        this.index = indexNameOrPattern;
    }

    public int getNumDocsWritten() {
        return numDocsWritten;
    }

    /**
     * Return the model definitions one at a time on the {@code modelConsumer}.
     * Either {@code errorConsumer} or {@code successConsumer} will be called
     * when the process is finished.
     *
     * The {@code modelConsumer} has the opportunity to cancel loading by
     * returning false in which case the {@code successConsumer} is called
     * with the parameter Boolean.FALSE.
     *
     * The docs are returned in order based on {@link TrainedModelDefinitionDoc#getDocNum()}
     * there is no error checking for duplicate or missing docs the consumer should handle
     * those errors.
     *
     * Depending on the search size multiple searches may be made.
     *
     * @param modelConsumer    Consumes model definition docs
     * @param successConsumer  Called when all docs have been returned or the loading is cancelled
     * @param errorConsumer    In the event of an error
     */
    public void restoreModelDefinition(
        CheckedFunction<TrainedModelDefinitionDoc, Boolean, IOException> modelConsumer,
        Consumer<Boolean> successConsumer,
        Consumer<Exception> errorConsumer
    ) {

        logger.debug("[{}] restoring model", modelId);
        SearchRequest searchRequest = buildSearch(client, modelId, index, searchSize, null);
        executorService.execute(() -> doSearch(searchRequest, modelConsumer, successConsumer, errorConsumer));
    }

    private void doSearch(
        SearchRequest searchRequest,
        CheckedFunction<TrainedModelDefinitionDoc, Boolean, IOException> modelConsumer,
        Consumer<Boolean> successConsumer,
        Consumer<Exception> errorConsumer
    ) {
        try {
            assert Thread.currentThread().getName().contains(NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME)
                || Thread.currentThread().getName().contains(UTILITY_THREAD_POOL_NAME)
                : format(
                    "Must execute from [%s] or [%s] but thread is [%s]",
                    NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME,
                    UTILITY_THREAD_POOL_NAME,
                    Thread.currentThread().getName()
                );

            SearchResponse searchResponse = retryingSearch(
                client,
                modelId,
                searchRequest,
                SEARCH_RETRY_LIMIT,
                SEARCH_FAILURE_RETRY_WAIT_TIME
            );
            try {
                if (searchResponse.getHits().getHits().length == 0) {
                    errorConsumer.accept(new ResourceNotFoundException(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                    return;
                }

                // Set lastNum to a non-zero to prevent an infinite loop of
                // search after requests in the absolute worse case where
                // it has all gone wrong.
                // Docs are numbered 0..N. we must have seen at least
                // this many docs so far.
                int lastNum = numDocsWritten - 1;
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    logger.debug(() -> format("[%s] Restoring model definition doc with id [%s]", modelId, hit.getId()));
                    try {
                        TrainedModelDefinitionDoc doc = parseModelDefinitionDocLenientlyFromSource(
                            hit.getSourceRef(),
                            modelId,
                            xContentRegistry
                        );
                        lastNum = doc.getDocNum();

                        boolean continueSearching = modelConsumer.apply(doc);
                        if (continueSearching == false) {
                            // signal the search has finished early
                            successConsumer.accept(Boolean.FALSE);
                            return;
                        }

                    } catch (IOException e) {
                        logger.error(() -> "[" + modelId + "] error writing model definition", e);
                        errorConsumer.accept(e);
                        return;
                    }
                }

                numDocsWritten += searchResponse.getHits().getHits().length;

                boolean endOfSearch = searchResponse.getHits().getHits().length < searchSize
                    || searchResponse.getHits().getTotalHits().value() == numDocsWritten;

                if (endOfSearch) {
                    successConsumer.accept(Boolean.TRUE);
                } else {
                    // search again with after
                    SearchHit lastHit = searchResponse.getHits().getAt(searchResponse.getHits().getHits().length - 1);
                    SearchRequestBuilder searchRequestBuilder = buildSearchBuilder(client, modelId, index, searchSize);
                    searchRequestBuilder.searchAfter(new Object[] { lastHit.getIndex(), lastNum });
                    executorService.execute(() -> doSearch(searchRequestBuilder.request(), modelConsumer, successConsumer, errorConsumer));
                }
            } finally {
                searchResponse.decRef();
            }
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                errorConsumer.accept(new ResourceNotFoundException(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
            } else {
                errorConsumer.accept(e);
            }
        }
    }

    static SearchResponse retryingSearch(Client client, String modelId, SearchRequest searchRequest, int retries, TimeValue sleep)
        throws InterruptedException {
        int failureCount = 0;

        while (true) {
            try {
                return client.search(searchRequest).actionGet();
            } catch (Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof SearchPhaseExecutionException == false
                    && ExceptionsHelper.unwrapCause(e) instanceof CircuitBreakingException == false) {
                    throw e;
                }

                if (failureCount >= retries) {
                    logger.warn(format("[%s] searching for model part failed %s times, returning failure", modelId, retries));
                    /*
                     * ElasticsearchException does not implement the ElasticsearchWrapperException interface so this exception cannot
                     * be unwrapped. This is important because the TrainedModelAssignmentNodeService has retry logic when a
                     * SearchPhaseExecutionException occurs:
                     * https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/ml/src/main/java/org/elasticsearch/xpack/ml/inference/assignment/TrainedModelAssignmentNodeService.java#L219
                     * This intentionally prevents that code from attempting to retry loading the entire model. If the retry logic here
                     * fails after the set retries we should not retry loading the entire model to avoid additional strain on the cluster.
                     */
                    throw new ElasticsearchStatusException(
                        format(
                            "loading model [%s] failed after [%s] retries. The deployment is now in a failed state, "
                                + "the error may be transient please stop the deployment and restart",
                            modelId,
                            retries
                        ),
                        RestStatus.TOO_MANY_REQUESTS,
                        e
                    );
                }

                failureCount++;
                logger.debug(format("[%s] searching for model part failed %s times, retrying", modelId, failureCount));
                TimeUnit.SECONDS.sleep(sleep.getSeconds());
            }
        }
    }

    private static SearchRequestBuilder buildSearchBuilder(Client client, String modelId, String index, int searchSize) {
        return client.prepareSearch(index)
            .setQuery(
                QueryBuilders.constantScoreQuery(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId))
                        .filter(
                            QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(), TrainedModelDefinitionDoc.NAME)
                        )
                )
            )
            .setSize(searchSize)
            .setTrackTotalHits(true)
            // First find the latest index
            .addSort("_index", SortOrder.DESC)
            // Then, sort by doc_num
            .addSort(
                SortBuilders.fieldSort(TrainedModelDefinitionDoc.DOC_NUM.getPreferredName()).order(SortOrder.ASC).unmappedType("long")
            );
    }

    public static SearchRequest buildSearch(Client client, String modelId, String index, int searchSize, @Nullable TaskId parentTaskId) {
        SearchRequest searchRequest = buildSearchBuilder(client, modelId, index, searchSize).request();
        if (parentTaskId != null) {
            searchRequest.setParentTask(parentTaskId);
        }
        return searchRequest;
    }

    public static TrainedModelDefinitionDoc parseModelDefinitionDocLenientlyFromSource(
        BytesReference source,
        String modelId,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG.withRegistry(xContentRegistry),
                source,
                XContentType.JSON
            )
        ) {
            return TrainedModelDefinitionDoc.fromXContent(parser, true).build();
        } catch (IOException e) {
            logger.error(() -> "[" + modelId + "] failed to parse model definition", e);
            throw e;
        }
    }
}
