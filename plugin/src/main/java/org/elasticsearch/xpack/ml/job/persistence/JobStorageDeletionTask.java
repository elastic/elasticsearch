/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
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

        String indexName = AnomalyDetectorsIndex.getCurrentResultsIndex(state, jobId);
        String indexPattern = indexName + "-*";

        // Step 2. Regardless of if the DBQ succeeds, we delete the physical index
        // -------
        // TODO this will be removed once shared indices are used
        CheckedConsumer<BulkByScrollResponse, Exception> dbqHandler = bulkByScrollResponse -> {
            if (bulkByScrollResponse.isTimedOut()) {
                logger.warn("DeleteByQuery for index [" + indexPattern + "] timed out. Continuing to delete index.");
            }
            if (!bulkByScrollResponse.getBulkFailures().isEmpty()) {
                logger.warn("[" + bulkByScrollResponse.getBulkFailures().size()
                        + "] failures encountered while running DeleteByQuery on index [" + indexPattern + "]. "
                        + "Continuing to delete index");
            }

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            client.admin().indices().delete(deleteIndexRequest, ActionListener.wrap(deleteIndexResponse -> {
                logger.info("Deleting index [" + indexName + "] successful");

                if (deleteIndexResponse.isAcknowledged()) {
                    logger.info("Index deletion acknowledged");
                } else {
                    logger.warn("Index deletion not acknowledged");
                }
                finishedHandler.accept(deleteIndexResponse.isAcknowledged());
            }, missingIndexHandler(indexName, finishedHandler, failureHandler)));
        };

        // Step 1. DeleteByQuery on the index, matching all docs with the right job_id
        // -------
        SearchRequest searchRequest = new SearchRequest(indexPattern);
        searchRequest.indicesOptions(JobProvider.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);
        searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder(Job.ID.getPreferredName(), jobId)));
        request.setSlices(5);

        client.execute(MlDeleteByQueryAction.INSTANCE, request,
                ActionListener.wrap(dbqHandler, missingIndexHandler(indexName, finishedHandler, failureHandler)));
    }

    // If the index doesn't exist, we need to catch the exception and carry onwards so that the cluster
    // state is properly updated
    private Consumer<Exception> missingIndexHandler(String indexName, CheckedConsumer<Boolean, Exception> finishedHandler,
                                                    Consumer<Exception> failureHandler) {
        return e -> {
            if (e instanceof IndexNotFoundException) {
                logger.warn("Physical index [" + indexName + "] not found. Continuing to delete job.");
                try {
                    finishedHandler.accept(false);
                } catch (Exception e1) {
                    failureHandler.accept(e1);
                }
            } else {
                // all other exceptions should die
                failureHandler.accept(e);
            }
        };
    }
}
