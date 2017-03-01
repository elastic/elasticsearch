/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.action.bulk.byscroll.DeleteByQueryRequest;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.ml.action.MlDeleteByQueryAction;
import org.elasticsearch.xpack.ml.job.config.Job;


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

        CheckedConsumer<IndicesAliasesResponse, Exception> deleteAliasHandler = indicesAliasesResponse -> {
            if (!indicesAliasesResponse.isAcknowledged()) {
                logger.warn("Delete Alias request not acknowledged for alias [" + aliasName + "].");
            } else {
                logger.info("Done deleting alias [" + aliasName + "]");
            }

            finishedHandler.accept(true);
        };

        // Step 2. DBQ done, delete the alias
        // -------
        // TODO norelease more robust handling of failures?
        CheckedConsumer<BulkByScrollResponse, Exception> dbqHandler = bulkByScrollResponse -> {
            if (bulkByScrollResponse.isTimedOut()) {
                logger.warn("DeleteByQuery for indices [" + indexName + ", " + indexPattern + "] timed out.");
            }
            if (!bulkByScrollResponse.getBulkFailures().isEmpty()) {
                logger.warn("[" + bulkByScrollResponse.getBulkFailures().size()
                        + "] failures encountered while running DeleteByQuery on indices [" + indexName + ", "
                        + indexPattern + "]. ");
            }
            IndicesAliasesRequest request = new IndicesAliasesRequest()
                    .addAliasAction(IndicesAliasesRequest.AliasActions.remove().alias(aliasName).index(indexName));
            client.admin().indices().aliases(request, ActionListener.wrap(deleteAliasHandler,
                    e -> {
                        if (e instanceof IndexNotFoundException) {
                            logger.warn("Alias [" + aliasName + "] not found. Continuing to delete job.");
                            try {
                                finishedHandler.accept(false);
                            } catch (Exception e1) {
                                failureHandler.accept(e1);
                            }
                        } else {
                            // all other exceptions should die
                            failureHandler.accept(e);
                        }
                    }));
        };

        // Step 1. DeleteByQuery on the index, matching all docs with the right job_id
        // -------
        logger.info("Running DBQ on [" + indexName + "," + indexPattern + "] for job [" + jobId + "]");
        SearchRequest searchRequest = new SearchRequest(indexName, indexPattern);
        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);
        ConstantScoreQueryBuilder query = new ConstantScoreQueryBuilder(new TermQueryBuilder(Job.ID.getPreferredName(), jobId));
        searchRequest.source(new SearchSourceBuilder().query(query));
        searchRequest.indicesOptions(JobProvider.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
        request.setSlices(5);

        client.execute(MlDeleteByQueryAction.INSTANCE, request, ActionListener.wrap(dbqHandler, failureHandler));
    }
}
