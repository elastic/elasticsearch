/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.ml.job.retention.WritableIndexExpander;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class JobDataDeleter {

    private static final Logger logger = LogManager.getLogger(JobDataDeleter.class);

    private static final int MAX_SNAPSHOTS_TO_DELETE = 10000;

    private final Client client;
    private final String jobId;
    private final boolean deleteUserAnnotations;

    public JobDataDeleter(Client client, String jobId) {
        this(client, jobId, false);
    }

    public JobDataDeleter(Client client, String jobId, boolean deleteUserAnnotations) {
        this.client = Objects.requireNonNull(client);
        this.jobId = Objects.requireNonNull(jobId);
        this.deleteUserAnnotations = deleteUserAnnotations;
    }

    /**
     * Delete a list of model snapshots and their corresponding state documents.
     *
     * @param modelSnapshots the model snapshots to delete
     */
    public void deleteModelSnapshots(List<ModelSnapshot> modelSnapshots, ActionListener<BulkByScrollResponse> listener) {
        if (modelSnapshots.isEmpty()) {
            listener.onResponse(
                new BulkByScrollResponse(
                    TimeValue.ZERO,
                    new BulkByScrollTask.Status(Collections.emptyList(), null),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    false
                )
            );
            return;
        }

        String stateIndexName = AnomalyDetectorsIndex.jobStateIndexPattern();

        List<String> idsToDelete = new ArrayList<>();
        Set<String> indices = new HashSet<>();
        indices.add(stateIndexName);
        indices.add(AnnotationIndex.READ_ALIAS_NAME);
        for (ModelSnapshot modelSnapshot : modelSnapshots) {
            idsToDelete.addAll(modelSnapshot.stateDocumentIds());
            idsToDelete.add(ModelSnapshot.documentId(modelSnapshot));
            idsToDelete.add(ModelSnapshot.annotationDocumentId(modelSnapshot));
            indices.add(AnomalyDetectorsIndex.jobResultsAliasedName(modelSnapshot.getJobId()));
        }

        String[] indicesToQuery = removeReadOnlyIndices(new ArrayList<>(indices), listener, "model snapshots", null);
        if (indicesToQuery.length == 0) return;

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(indicesToQuery).setRefresh(true)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(idsToDelete.toArray(new String[0])));

        // _doc is the most efficient sort order and will also disable scoring
        deleteByQueryRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, deleteByQueryRequest, listener);
    }

    /**
     * Asynchronously delete the annotations
     * If the deleteUserAnnotations field is set to true then all
     * annotations - both auto-generated and user-added - are removed, else
     * only the auto-generated ones, (i.e. created by the _xpack user) are
     * removed.
     * @param listener Response listener
     */
    public void deleteAllAnnotations(ActionListener<Boolean> listener) {
        deleteAnnotations(null, null, null, listener);
    }

    /**
     * Asynchronously delete all the auto-generated (i.e. created by the _xpack user) annotations starting from {@code cutOffTime}
     *
     * @param fromEpochMs Only annotations at and after this time will be deleted. If {@code null}, no cutoff is applied
     * @param toEpochMs Only annotations before this time will be deleted. If {@code null}, no cutoff is applied
     * @param eventsToDelete Only annotations with one of the provided event types will be deleted.
     *                       If {@code null} or empty, no event-related filtering is applied
     * @param listener Response listener
     */
    public void deleteAnnotations(
        @Nullable Long fromEpochMs,
        @Nullable Long toEpochMs,
        @Nullable Set<String> eventsToDelete,
        ActionListener<Boolean> listener
    ) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId));
        if (deleteUserAnnotations == false) {
            boolQuery.filter(QueryBuilders.termQuery(Annotation.CREATE_USERNAME.getPreferredName(), InternalUsers.XPACK_USER.principal()));
        }
        if (fromEpochMs != null || toEpochMs != null) {
            boolQuery.filter(QueryBuilders.rangeQuery(Annotation.TIMESTAMP.getPreferredName()).gte(fromEpochMs).lt(toEpochMs));
        }
        if (eventsToDelete != null && eventsToDelete.isEmpty() == false) {
            boolQuery.filter(QueryBuilders.termsQuery(Annotation.EVENT.getPreferredName(), eventsToDelete));
        }
        QueryBuilder query = QueryBuilders.constantScoreQuery(boolQuery);

        String[] indicesToQuery = removeReadOnlyIndices(
            List.of(AnnotationIndex.READ_ALIAS_NAME),
            listener,
            "annotations",
            () -> listener.onResponse(true)
        );
        if (indicesToQuery.length == 0) return;

        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(indicesToQuery).setQuery(query)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setAbortOnVersionConflict(false)
            .setRefresh(true)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);

        // _doc is the most efficient sort order and will also disable scoring
        dbqRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            DeleteByQueryAction.INSTANCE,
            dbqRequest,
            ActionListener.wrap(r -> listener.onResponse(true), listener::onFailure)
        );
    }

    private <T> String[] removeReadOnlyIndices(
        List<String> indicesToQuery,
        ActionListener<T> listener,
        String entityType,
        Runnable onEmpty
    ) {
        try {
            indicesToQuery = WritableIndexExpander.getInstance().getWritableIndices(indicesToQuery);
        } catch (Exception e) {
            logger.error("Failed to get writable indices for [" + jobId + "]", e);
            listener.onFailure(e);
            return new String[0];
        }
        if (indicesToQuery.isEmpty()) {
            logger.info("No writable {} indices found for [{}] job. No {} to remove.", entityType, jobId, entityType);
            if (onEmpty != null) {
                onEmpty.run();
            }
        }
        return indicesToQuery.toArray(String[]::new);
    }

    /**
     * Asynchronously delete all result types (Buckets, Records, Influencers) from {@code cutOffTime}.
     * Forecasts are <em>not</em> deleted, as they will not be automatically regenerated after
     * restarting a datafeed following a model snapshot reversion.
     *
     * @param cutoffEpochMs Results at and after this time will be deleted
     * @param listener Response listener
     */
    public void deleteResultsFromTime(long cutoffEpochMs, ActionListener<Boolean> listener) {
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.termsQuery(
                    Result.RESULT_TYPE.getPreferredName(),
                    AnomalyRecord.RESULT_TYPE_VALUE,
                    Bucket.RESULT_TYPE_VALUE,
                    BucketInfluencer.RESULT_TYPE_VALUE,
                    Influencer.RESULT_TYPE_VALUE,
                    ModelPlot.RESULT_TYPE_VALUE
                )
            )
            .filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).gte(cutoffEpochMs));
        String[] indicesToQuery = removeReadOnlyIndices(
            List.of(AnomalyDetectorsIndex.jobResultsAliasedName(jobId)),
            listener,
            "results",
            () -> listener.onResponse(true)
        );
        if (indicesToQuery.length == 0) return;
        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(indicesToQuery).setQuery(query)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setAbortOnVersionConflict(false)
            .setRefresh(true)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);

        // _doc is the most efficient sort order and will also disable scoring
        dbqRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            DeleteByQueryAction.INSTANCE,
            dbqRequest,
            ActionListener.wrap(r -> listener.onResponse(true), listener::onFailure)
        );
    }

    /**
     * Delete all results marked as interim
     */
    public void deleteInterimResults() {
        QueryBuilder query = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery(Result.IS_INTERIM.getPreferredName(), true));
        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId)).setQuery(query)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setAbortOnVersionConflict(false)
            .setRefresh(false)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);

        // _doc is the most efficient sort order and will also disable scoring
        dbqRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            client.execute(DeleteByQueryAction.INSTANCE, dbqRequest).get();
        } catch (Exception e) {
            logger.error("[" + jobId + "] An error occurred while deleting interim results", e);
        }
    }

    /**
     * Delete the datafeed timing stats document from all the job results indices
     *
     * @param listener Response listener
     */
    public void deleteDatafeedTimingStats(ActionListener<BulkByScrollResponse> listener) {
        String[] indicesToQuery = removeReadOnlyIndices(
            List.of(AnomalyDetectorsIndex.jobResultsAliasedName(jobId)),
            listener,
            "datafeed timing stats",
            null
        );
        if (indicesToQuery.length == 0) return;
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(indicesToQuery).setRefresh(true)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(DatafeedTimingStats.documentId(jobId)));

        // _doc is the most efficient sort order and will also disable scoring
        deleteByQueryRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, deleteByQueryRequest, listener);
    }

    /**
     * Deletes all documents associated with a job except user annotations and notifications
     */
    public void deleteJobDocuments(
        JobConfigProvider jobConfigProvider,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        CheckedConsumer<Boolean, Exception> finishedHandler,
        Consumer<Exception> failureHandler
    ) {

        AtomicReference<String[]> indexNames = new AtomicReference<>();

        final ActionListener<IndicesAliasesResponse> completionHandler = ActionListener.wrap(
            response -> finishedHandler.accept(response.isAcknowledged()),
            failureHandler
        );

        // Step 9. If we did not drop the indices and after DBQ state done, we delete the aliases
        ActionListener<BulkByScrollResponse> dbqHandler = ActionListener.wrap(bulkByScrollResponse -> {
            if (bulkByScrollResponse == null) { // no action was taken by DBQ, assume indices were deleted
                completionHandler.onResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);
            } else {
                if (bulkByScrollResponse.isTimedOut()) {
                    logger.warn("[{}] DeleteByQuery for indices [{}] timed out.", jobId, String.join(", ", indexNames.get()));
                }
                if (bulkByScrollResponse.getBulkFailures().isEmpty() == false) {
                    logger.warn(
                        "[{}] {} failures and {} conflicts encountered while running DeleteByQuery on indices [{}].",
                        jobId,
                        bulkByScrollResponse.getBulkFailures().size(),
                        bulkByScrollResponse.getVersionConflicts(),
                        String.join(", ", indexNames.get())
                    );
                    for (BulkItemResponse.Failure failure : bulkByScrollResponse.getBulkFailures()) {
                        logger.warn("DBQ failure: " + failure);
                    }
                }
                deleteAliases(jobId, completionHandler);
            }
        }, failureHandler);

        // Step 8. If we did not delete the indices, we run a delete by query
        ActionListener<Boolean> deleteByQueryExecutor = ActionListener.wrap(response -> {
            if (response && indexNames.get().length > 0) {
                deleteResultsByQuery(jobId, indexNames.get(), dbqHandler);
            } else { // We did not execute DBQ, no need to delete aliases or check the response
                dbqHandler.onResponse(null);
            }
        }, failureHandler);

        // Step 7. Handle each multi-search response. There should be one response for each underlying index.
        // For each underlying index that contains results ONLY for the current job, we will delete that index.
        // If there exists at least 1 index that has another job's results, we will run DBQ.
        ActionListener<MultiSearchResponse> customIndexSearchHandler = ActionListener.wrap(multiSearchResponse -> {
            if (multiSearchResponse == null) {
                deleteByQueryExecutor.onResponse(true); // We need to run DBQ and alias deletion
                return;
            }
            String defaultSharedIndex = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX
                + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
            List<String> indicesToDelete = new ArrayList<>();
            boolean needToRunDBQTemp = false;
            assert multiSearchResponse.getResponses().length == indexNames.get().length;
            int i = 0;
            for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
                if (item.isFailure()) {
                    ++i;
                    if (ExceptionsHelper.unwrapCause(item.getFailure()) instanceof IndexNotFoundException) {
                        // index is already deleted, no need to take action against it
                        continue;
                    } else {
                        failureHandler.accept(item.getFailure());
                        return;
                    }
                }
                SearchResponse searchResponse = item.getResponse();
                if (searchResponse.getHits().getTotalHits().value > 0 || indexNames.get()[i].equals(defaultSharedIndex)) {
                    needToRunDBQTemp = true;
                } else {
                    indicesToDelete.add(indexNames.get()[i]);
                }
                ++i;
            }
            final boolean needToRunDBQ = needToRunDBQTemp;
            if (indicesToDelete.isEmpty()) {
                deleteByQueryExecutor.onResponse(needToRunDBQ);
                return;
            }
            logger.info("[{}] deleting the following indices directly {}", jobId, indicesToDelete);
            DeleteIndexRequest request = new DeleteIndexRequest(indicesToDelete.toArray(String[]::new));
            request.indicesOptions(IndicesOptions.lenientExpandOpenHidden());
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ML_ORIGIN,
                request,
                ActionListener.<AcknowledgedResponse>wrap(
                    response -> deleteByQueryExecutor.onResponse(needToRunDBQ), // only run DBQ if there is a shared index
                    failureHandler
                ),
                client.admin().indices()::delete
            );
        }, failure -> {
            if (ExceptionsHelper.unwrapCause(failure) instanceof IndexNotFoundException) { // assume the index is already deleted
                deleteByQueryExecutor.onResponse(false); // skip DBQ && Alias
            } else {
                failureHandler.accept(failure);
            }
        });

        // Step 6. If we successfully find a job, gather information about its result indices.
        // This will execute a multi-search action for every concrete index behind the job results alias.
        // If there are no concrete indices, take no action and go to the next step.
        ActionListener<Job.Builder> getJobHandler = ActionListener.wrap(builder -> {
            indexNames.set(
                indexNameExpressionResolver.concreteIndexNames(
                    clusterState,
                    IndicesOptions.lenientExpandOpen(),
                    AnomalyDetectorsIndex.jobResultsAliasedName(jobId)
                )
            );
            if (indexNames.get().length == 0) {
                // don't bother searching the index any further - it's already been closed or deleted
                customIndexSearchHandler.onResponse(null);
                return;
            }
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            // It is important that the requests are in the same order as the index names.
            // This is because responses are ordered according to their requests.
            for (String indexName : indexNames.get()) {
                SearchSourceBuilder source = new SearchSourceBuilder().size(0)
                    // if we have just one hit we cannot delete the index
                    .trackTotalHitsUpTo(1)
                    .query(
                        QueryBuilders.boolQuery()
                            .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId)))
                    );
                multiSearchRequest.add(new SearchRequest(indexName).source(source));
            }
            executeAsyncWithOrigin(client, ML_ORIGIN, TransportMultiSearchAction.TYPE, multiSearchRequest, customIndexSearchHandler);
        }, failureHandler);

        // Step 5. Get the job as the initial result index name is required
        ActionListener<Boolean> deleteAnnotationsHandler = ActionListener.wrap(
            response -> jobConfigProvider.getJob(jobId, null, getJobHandler),
            failureHandler
        );

        // Step 4. Delete annotations associated with the job
        ActionListener<Boolean> deleteCategorizerStateHandler = ActionListener.wrap(
            response -> deleteAllAnnotations(deleteAnnotationsHandler),
            failureHandler
        );

        // Step 3. Delete quantiles done, delete the categorizer state
        ActionListener<Boolean> deleteQuantilesHandler = ActionListener.wrap(
            response -> deleteCategorizerState(jobId, 1, deleteCategorizerStateHandler),
            failureHandler
        );

        // Step 2. Delete state done, delete the quantiles
        ActionListener<BulkByScrollResponse> deleteStateHandler = ActionListener.wrap(
            bulkResponse -> deleteQuantiles(jobId, deleteQuantilesHandler),
            failureHandler
        );

        // Step 1. Delete the model state
        deleteModelState(jobId, deleteStateHandler);
    }

    private void deleteResultsByQuery(
        @SuppressWarnings("HiddenField") String jobId,
        String[] indices,
        ActionListener<BulkByScrollResponse> listener
    ) {
        assert indices.length > 0;

        ActionListener<BroadcastResponse> refreshListener = ActionListener.wrap(refreshResponse -> {
            logger.info("[{}] running delete by query on [{}]", jobId, String.join(", ", indices));
            ConstantScoreQueryBuilder query = new ConstantScoreQueryBuilder(new TermQueryBuilder(Job.ID.getPreferredName(), jobId));
            String[] indicesToQuery = removeReadOnlyIndices(List.of(indices), listener, "results", null);
            if (indicesToQuery.length == 0) return;
            DeleteByQueryRequest request = new DeleteByQueryRequest(indicesToQuery).setQuery(query)
                .setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpenHidden()))
                .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
                .setAbortOnVersionConflict(false)
                .setRefresh(true);

            executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, listener);
        }, listener::onFailure);

        // First, we refresh the indices to ensure any in-flight docs become visible
        RefreshRequest refreshRequest = new RefreshRequest(indices);
        refreshRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpenHidden()));
        executeAsyncWithOrigin(client, ML_ORIGIN, RefreshAction.INSTANCE, refreshRequest, refreshListener);
    }

    private void deleteAliases(@SuppressWarnings("HiddenField") String jobId, ActionListener<IndicesAliasesResponse> finishedHandler) {
        final String readAliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        final String writeAliasName = AnomalyDetectorsIndex.resultsWriteAlias(jobId);

        // first find the concrete indices associated with the aliases
        GetAliasesRequest aliasesRequest = new GetAliasesRequest().aliases(readAliasName, writeAliasName)
            .indicesOptions(IndicesOptions.lenientExpandOpenHidden());
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            aliasesRequest,
            ActionListener.<GetAliasesResponse>wrap(getAliasesResponse -> {
                // remove the aliases from the concrete indices found in the first step
                IndicesAliasesRequest removeRequest = buildRemoveAliasesRequest(getAliasesResponse);
                if (removeRequest == null) {
                    // don't error if the job's aliases have already been deleted - carry on and delete the
                    // rest of the job's data
                    finishedHandler.onResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);
                    return;
                }
                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    ML_ORIGIN,
                    removeRequest,
                    finishedHandler,
                    client.admin().indices()::aliases
                );
            }, finishedHandler::onFailure),
            client.admin().indices()::getAliases
        );
    }

    private static IndicesAliasesRequest buildRemoveAliasesRequest(GetAliasesResponse getAliasesResponse) {
        Set<String> aliases = new HashSet<>();
        List<String> indices = new ArrayList<>();
        for (var entry : getAliasesResponse.getAliases().entrySet()) {
            // The response includes _all_ indices, but only those associated with
            // the aliases we asked about will have associated AliasMetadata
            if (entry.getValue().isEmpty() == false) {
                indices.add(entry.getKey());
                entry.getValue().forEach(metadata -> aliases.add(metadata.getAlias()));
            }
        }
        return aliases.isEmpty()
            ? null
            : new IndicesAliasesRequest().addAliasAction(
                IndicesAliasesRequest.AliasActions.remove().aliases(aliases.toArray(new String[0])).indices(indices.toArray(new String[0]))
            );
    }

    private void deleteQuantiles(@SuppressWarnings("HiddenField") String jobId, ActionListener<Boolean> finishedHandler) {
        // Just use ID here, not type, as trying to delete different types spams the logs with an exception stack trace
        IdsQueryBuilder query = new IdsQueryBuilder().addIds(Quantiles.documentId(jobId));

        String[] indicesToQuery = removeReadOnlyIndices(
            List.of(AnomalyDetectorsIndex.jobStateIndexPattern()),
            finishedHandler,
            "quantiles",
            () -> finishedHandler.onResponse(true)
        );
        if (indicesToQuery.length == 0) return;

        DeleteByQueryRequest request = new DeleteByQueryRequest(indicesToQuery).setQuery(query)
            .setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()))
            .setAbortOnVersionConflict(false)
            .setRefresh(true);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            DeleteByQueryAction.INSTANCE,
            request,
            ActionListener.wrap(response -> finishedHandler.onResponse(true), ignoreIndexNotFoundException(finishedHandler))
        );
    }

    private void deleteModelState(@SuppressWarnings("HiddenField") String jobId, ActionListener<BulkByScrollResponse> listener) {
        GetModelSnapshotsAction.Request request = new GetModelSnapshotsAction.Request(jobId, null);
        request.setPageParams(new PageParams(0, MAX_SNAPSHOTS_TO_DELETE));
        executeAsyncWithOrigin(client, ML_ORIGIN, GetModelSnapshotsAction.INSTANCE, request, ActionListener.wrap(response -> {
            List<ModelSnapshot> deleteCandidates = response.getPage().results();
            deleteModelSnapshots(deleteCandidates, listener);
        }, listener::onFailure));
    }

    private void deleteCategorizerState(
        @SuppressWarnings("HiddenField") String jobId,
        int docNum,
        ActionListener<Boolean> finishedHandler
    ) {
        // Just use ID here, not type, as trying to delete different types spams the logs with an exception stack trace
        IdsQueryBuilder query = new IdsQueryBuilder().addIds(CategorizerState.documentId(jobId, docNum));
        String[] indicesToQuery = removeReadOnlyIndices(
            List.of(AnomalyDetectorsIndex.jobStateIndexPattern()),
            finishedHandler,
            "categorizer state",
            () -> finishedHandler.onResponse(true)
        );
        if (indicesToQuery.length == 0) return;
        DeleteByQueryRequest request = new DeleteByQueryRequest(indicesToQuery).setQuery(query)
            .setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()))
            .setAbortOnVersionConflict(false)
            .setRefresh(true);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            // If we successfully deleted a document try the next one; if not we're done
            if (response.getDeleted() > 0) {
                // There's an assumption here that there won't be very many categorizer
                // state documents, so the recursion won't go more than, say, 5 levels deep
                deleteCategorizerState(jobId, docNum + 1, finishedHandler);
                return;
            }
            finishedHandler.onResponse(true);
        }, ignoreIndexNotFoundException(finishedHandler)));
    }

    private static Consumer<Exception> ignoreIndexNotFoundException(ActionListener<Boolean> finishedHandler) {
        return e -> {
            // It's not a problem for us if the index wasn't found - it's equivalent to document not found
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                finishedHandler.onResponse(true);
            } else {
                finishedHandler.onFailure(e);
            }
        };
    }
}
