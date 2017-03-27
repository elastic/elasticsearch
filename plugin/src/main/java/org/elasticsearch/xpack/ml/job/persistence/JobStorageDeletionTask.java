/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.DeleteByQueryRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.ml.action.MlDeleteByQueryAction;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;

import java.util.List;
import java.util.function.Consumer;

public class JobStorageDeletionTask extends Task {
    private final Logger logger;

    public JobStorageDeletionTask(long id, String type, String action, String description, TaskId parentTask) {
        super(id, type, action, description, parentTask);
        this.logger = Loggers.getLogger(getClass());
    }

    public void delete(String jobId, Client client, ClusterState state,
                       CheckedConsumer<Boolean, Exception> finishedHandler,
                       Consumer<Exception> failureHandler) {

        final String indexName = AnomalyDetectorsIndex.getPhysicalIndexFromState(state, jobId);
        final String indexPattern = indexName + "-*";
        final String aliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

        ActionListener<Boolean> deleteAliasHandler = ActionListener.wrap(finishedHandler, failureHandler);

        // Step 5. Delete categorizer state done, delete the alias
        ActionListener<Boolean> deleteCategorizerStateHandler = ActionListener.wrap(
                bulkItemResponses -> deleteAlias(jobId, aliasName, indexName, client, deleteAliasHandler),
                failureHandler);

        // Step 4. Delete model state done, delete the categorizer state
        ActionListener<BulkResponse> deleteStateHandler = ActionListener.wrap(
                response -> deleteCategorizerState(jobId, client, deleteCategorizerStateHandler),
                failureHandler);

        // Step 3. Delete quantiles done, delete the model state
        ActionListener<DeleteResponse> deleteQuantilesHandler = ActionListener.wrap(
                deleteResponse -> deleteModelState(jobId, client, deleteStateHandler),
                failureHandler);


        // Step 2. DBQ done, delete the state
        // -------
        ActionListener<BulkByScrollResponse> dbqHandler = ActionListener.wrap(bulkByScrollResponse -> {
                    if (bulkByScrollResponse.isTimedOut()) {
                        logger.warn("[{}] DeleteByQuery for indices [{}, {}] timed out.", jobId, indexName, indexPattern);
                    }
                    if (!bulkByScrollResponse.getBulkFailures().isEmpty()) {
                        logger.warn("[{}] {} failures encountered while running DeleteByQuery on indices [{}, {}].",
                                jobId, bulkByScrollResponse.getBulkFailures().size(), indexName, indexPattern);
                    }
                    deleteQuantiles(jobId, client, deleteQuantilesHandler);
                },
                failureHandler
        );


        // Step 1. DeleteByQuery on the index, matching all docs with the right job_id
        // -------
        logger.info("Running DBQ on [" + indexName + "," + indexPattern + "] for job [" + jobId + "]");
        SearchRequest searchRequest = new SearchRequest(indexName, indexPattern);
        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);
        ConstantScoreQueryBuilder query = new ConstantScoreQueryBuilder(new TermQueryBuilder(Job.ID.getPreferredName(), jobId));
        searchRequest.source(new SearchSourceBuilder().query(query));
        searchRequest.indicesOptions(JobProvider.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
        request.setSlices(5);

        client.execute(MlDeleteByQueryAction.INSTANCE, request, dbqHandler);
    }

    private void deleteQuantiles(String jobId, Client client, ActionListener<DeleteResponse> finishedHandler) {
        client.prepareDelete(AnomalyDetectorsIndex.jobStateIndexName(), Quantiles.TYPE.getPreferredName(), Quantiles.documentId(jobId))
                .execute(finishedHandler);
    }

    private void deleteModelState(String jobId, Client client, ActionListener<BulkResponse> listener) {

        JobProvider jobProvider = new JobProvider(client, Settings.EMPTY);
        jobProvider.modelSnapshots(jobId, 0, 10000,
                page -> {
                    List<ModelSnapshot> deleteCandidates = page.results();

                    // Delete the snapshot and any associated state files
                    JobDataDeleter deleter = new JobDataDeleter(client, jobId);
                    for (ModelSnapshot deleteCandidate : deleteCandidates) {
                        deleter.deleteModelSnapshot(deleteCandidate);
                    }

                    deleter.commit(listener);
                },
                listener::onFailure);
    }

    private void deleteCategorizerState(String jobId, Client client, ActionListener<Boolean> finishedHandler) {
        SearchRequest searchRequest = new SearchRequest(AnomalyDetectorsIndex.jobStateIndexName());
        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);
        request.setSlices(5);

        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        WildcardQueryBuilder query = new WildcardQueryBuilder(UidFieldMapper.NAME, Uid.createUid(CategorizerState.TYPE, jobId + "#*"));
        searchRequest.source(new SearchSourceBuilder().query(query));
        client.execute(MlDeleteByQueryAction.INSTANCE, request, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                finishedHandler.onResponse(true);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("[" + jobId + "] Failed to delete categorizer state for job.", e);
                finishedHandler.onFailure(e);
            }
        });
    }

    private void deleteAlias(String jobId, String aliasName, String indexName, Client client, ActionListener<Boolean> finishedHandler ) {
        IndicesAliasesRequest request = new IndicesAliasesRequest()
                .addAliasAction(IndicesAliasesRequest.AliasActions.remove().alias(aliasName).index(indexName));
        client.admin().indices().aliases(request, ActionListener.wrap(
                response -> finishedHandler.onResponse(true),
                e -> {
                    if (e instanceof AliasesNotFoundException || e instanceof IndexNotFoundException) {
                        logger.warn("[{}] Alias [{}] not found. Continuing to delete job.", jobId, aliasName);
                        finishedHandler.onResponse(true);
                    } else {
                        // all other exceptions should die
                        logger.error("[" + jobId + "] Failed to delete alias [" + aliasName + "].", e);
                        finishedHandler.onFailure(e);
                    }
                }));
    }
}
