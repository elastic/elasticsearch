/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.utils.MlIndicesUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/*
 Moving this class to plugin-core caused a *lot* of server side logic to be pulled in to plugin-core. This should be considered as needing
 refactoring to move it back to core. See DeleteJobAction for its use.
*/
public class JobStorageDeletionTask extends Task {

    private static final int MAX_SNAPSHOTS_TO_DELETE = 10000;

    private final Logger logger;

    public JobStorageDeletionTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
        this.logger = Loggers.getLogger(getClass());
    }

    public void delete(String jobId, Client client, ClusterState state,
                       CheckedConsumer<Boolean, Exception> finishedHandler,
                       Consumer<Exception> failureHandler) {

        final String indexName = AnomalyDetectorsIndex.getPhysicalIndexFromState(state, jobId);
        final String indexPattern = indexName + "-*";

        final ActionListener<AcknowledgedResponse> completionHandler = ActionListener.wrap(
            response -> finishedHandler.accept(response.isAcknowledged()),
            failureHandler);

        // Step 7. If we did not drop the index and after DBQ state done, we delete the aliases
        ActionListener<BulkByScrollResponse> dbqHandler = ActionListener.wrap(
                bulkByScrollResponse -> {
                    if (bulkByScrollResponse == null) { // no action was taken by DBQ, assume Index was deleted
                        completionHandler.onResponse(new AcknowledgedResponse(true));
                    } else {
                        if (bulkByScrollResponse.isTimedOut()) {
                            logger.warn("[{}] DeleteByQuery for indices [{}, {}] timed out.", jobId, indexName, indexPattern);
                        }
                        if (!bulkByScrollResponse.getBulkFailures().isEmpty()) {
                            logger.warn("[{}] {} failures and {} conflicts encountered while running DeleteByQuery on indices [{}, {}].",
                                jobId, bulkByScrollResponse.getBulkFailures().size(), bulkByScrollResponse.getVersionConflicts(),
                                indexName, indexPattern);
                            for (BulkItemResponse.Failure failure : bulkByScrollResponse.getBulkFailures()) {
                                logger.warn("DBQ failure: " + failure);
                            }
                        }
                        deleteAliases(jobId, client, completionHandler);
                    }
                },
                failureHandler);

        // Step 6. If we did not delete the index, we run a delete by query
        ActionListener<Boolean> deleteByQueryExecutor = ActionListener.wrap(
                response -> {
                    if (response) {
                        logger.info("Running DBQ on [" + indexName + "," + indexPattern + "] for job [" + jobId + "]");
                        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName, indexPattern);
                        ConstantScoreQueryBuilder query =
                            new ConstantScoreQueryBuilder(new TermQueryBuilder(Job.ID.getPreferredName(), jobId));
                        request.setQuery(query);
                        request.setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
                        request.setSlices(5);
                        request.setAbortOnVersionConflict(false);
                        request.setRefresh(true);

                        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, dbqHandler);
                    } else { // We did not execute DBQ, no need to delete aliases or check the response
                        dbqHandler.onResponse(null);
                    }
                },
                failureHandler);

        // Step 5. If we have any hits, that means we are NOT the only job on this index, and should not delete it
        // if we do not have any hits, we can drop the index and then skip the DBQ and alias deletion
        ActionListener<SearchResponse> customIndexSearchHandler = ActionListener.wrap(
            searchResponse -> {
                if (searchResponse == null || searchResponse.getHits().totalHits > 0) {
                    deleteByQueryExecutor.onResponse(true); // We need to run DBQ and alias deletion
                } else {
                    logger.info("Running DELETE Index on [" + indexName + "] for job [" + jobId + "]");
                    DeleteIndexRequest request = new DeleteIndexRequest(indexName);
                    request.indicesOptions(IndicesOptions.lenientExpandOpen());
                    // If we have deleted the index, then we don't need to delete the aliases or run the DBQ
                    executeAsyncWithOrigin(
                        client.threadPool().getThreadContext(),
                        ML_ORIGIN,
                        request,
                        ActionListener.<AcknowledgedResponse>wrap(
                            response -> deleteByQueryExecutor.onResponse(false), // skip DBQ && Alias
                            failureHandler),
                        client.admin().indices()::delete);
                }
            },
            failure -> {
                if (failure.getClass() == IndexNotFoundException.class) { // assume the index is already deleted
                    deleteByQueryExecutor.onResponse(false); // skip DBQ && Alias
                } else {
                    failureHandler.accept(failure);
                }
            }
        );

        // Step 4. Determine if we are on a shared index by looking at `.ml-anomalies-shared` or the custom index's aliases
        ActionListener<Boolean> deleteCategorizerStateHandler = ActionListener.wrap(
            response -> {
                if (indexName.equals(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX +
                    AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT)) {
                    customIndexSearchHandler.onResponse(null); //don't bother searching the index any further, we are on the default shared
                } else {
                    SearchSourceBuilder source = new SearchSourceBuilder()
                        .size(1)
                        .query(QueryBuilders.boolQuery().filter(
                            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))));

                    SearchRequest searchRequest = new SearchRequest(indexName);
                    searchRequest.source(source);
                    executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, customIndexSearchHandler);
                }
            },
            failureHandler
        );

        // Step 3. Delete quantiles done, delete the categorizer state
        ActionListener<Boolean> deleteQuantilesHandler = ActionListener.wrap(
                response -> deleteCategorizerState(jobId, client, 1, deleteCategorizerStateHandler),
                failureHandler);

        // Step 2. Delete state done, delete the quantiles
        ActionListener<BulkResponse> deleteStateHandler = ActionListener.wrap(
                bulkResponse -> deleteQuantiles(jobId, client, deleteQuantilesHandler),
                failureHandler);

        // Step 1. Delete the model state
        deleteModelState(jobId, client, deleteStateHandler);
    }

    private void deleteQuantiles(String jobId, Client client, ActionListener<Boolean> finishedHandler) {
        // The quantiles type and doc ID changed in v5.5 so delete both the old and new format
        DeleteByQueryRequest request = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobStateIndexName());
        // Just use ID here, not type, as trying to delete different types spams the logs with an exception stack trace
        IdsQueryBuilder query = new IdsQueryBuilder().addIds(Quantiles.documentId(jobId),
                // TODO: remove in 7.0
                Quantiles.v54DocumentId(jobId));
        request.setQuery(query);
        request.setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
        request.setAbortOnVersionConflict(false);
        request.setRefresh(true);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(
                response -> finishedHandler.onResponse(true),
                e -> {
                    // It's not a problem for us if the index wasn't found - it's equivalent to document not found
                    if (e instanceof IndexNotFoundException) {
                        finishedHandler.onResponse(true);
                    } else {
                        finishedHandler.onFailure(e);
                    }
                }));
    }

    private void deleteModelState(String jobId, Client client, ActionListener<BulkResponse> listener) {
        GetModelSnapshotsAction.Request request = new GetModelSnapshotsAction.Request(jobId, null);
        request.setPageParams(new PageParams(0, MAX_SNAPSHOTS_TO_DELETE));
        executeAsyncWithOrigin(client, ML_ORIGIN, GetModelSnapshotsAction.INSTANCE, request, ActionListener.wrap(
                response -> {
                    List<ModelSnapshot> deleteCandidates = response.getPage().results();
                    JobDataDeleter deleter = new JobDataDeleter(client, jobId);
                    deleter.deleteModelSnapshots(deleteCandidates, listener);
                },
                listener::onFailure));
    }

    private void deleteCategorizerState(String jobId, Client client, int docNum, ActionListener<Boolean> finishedHandler) {
        // The categorizer state type and doc ID changed in v5.5 so delete both the old and new format
        DeleteByQueryRequest request = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobStateIndexName());
        // Just use ID here, not type, as trying to delete different types spams the logs with an exception stack trace
        IdsQueryBuilder query = new IdsQueryBuilder().addIds(CategorizerState.documentId(jobId, docNum),
                // TODO: remove in 7.0
                CategorizerState.v54DocumentId(jobId, docNum));
        request.setQuery(query);
        request.setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
        request.setAbortOnVersionConflict(false);
        request.setRefresh(true);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(
                response -> {
                    // If we successfully deleted a document try the next one; if not we're done
                    if (response.getDeleted() > 0) {
                        // There's an assumption here that there won't be very many categorizer
                        // state documents, so the recursion won't go more than, say, 5 levels deep
                        deleteCategorizerState(jobId, client, docNum + 1, finishedHandler);
                        return;
                    }
                    finishedHandler.onResponse(true);
                },
                e -> {
                    // It's not a problem for us if the index wasn't found - it's equivalent to document not found
                    if (e instanceof IndexNotFoundException) {
                        finishedHandler.onResponse(true);
                    } else {
                        finishedHandler.onFailure(e);
                    }
                }));
    }

    private void deleteAliases(String jobId, Client client, ActionListener<AcknowledgedResponse> finishedHandler) {
        final String readAliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        final String writeAliasName = AnomalyDetectorsIndex.resultsWriteAlias(jobId);

        // first find the concrete indices associated with the aliases
        GetAliasesRequest aliasesRequest = new GetAliasesRequest().aliases(readAliasName, writeAliasName)
                .indicesOptions(IndicesOptions.lenientExpandOpen());
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, aliasesRequest,
                ActionListener.<GetAliasesResponse>wrap(
                        getAliasesResponse -> {
                            // remove the aliases from the concrete indices found in the first step
                            IndicesAliasesRequest removeRequest = buildRemoveAliasesRequest(getAliasesResponse);
                            if (removeRequest == null) {
                                // don't error if the job's aliases have already been deleted - carry on and delete the
                                // rest of the job's data
                                finishedHandler.onResponse(new AcknowledgedResponse(true));
                                return;
                            }
                            executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, removeRequest,
                                ActionListener.<AcknowledgedResponse>wrap(
                                    finishedHandler::onResponse,
                                    finishedHandler::onFailure),
                                    client.admin().indices()::aliases);
                        },
                        finishedHandler::onFailure), client.admin().indices()::getAliases);
    }

    private IndicesAliasesRequest buildRemoveAliasesRequest(GetAliasesResponse getAliasesResponse) {
        Set<String> aliases = new HashSet<>();
        List<String> indices = new ArrayList<>();
        for (ObjectObjectCursor<String, List<AliasMetaData>> entry : getAliasesResponse.getAliases()) {
            // The response includes _all_ indices, but only those associated with
            // the aliases we asked about will have associated AliasMetaData
            if (entry.value.isEmpty() == false) {
                indices.add(entry.key);
                entry.value.forEach(metadata -> aliases.add(metadata.getAlias()));
            }
        }
        return aliases.isEmpty() ? null : new IndicesAliasesRequest().addAliasAction(
                IndicesAliasesRequest.AliasActions.remove()
                        .aliases(aliases.toArray(new String[aliases.size()]))
                        .indices(indices.toArray(new String[indices.size()])));
    }
}
