/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ClientHelper.stashWithOrigin;

public class JobDataDeleter {

    private static final Logger LOGGER = LogManager.getLogger(JobDataDeleter.class);

    private final Client client;
    private final String jobId;

    public JobDataDeleter(Client client, String jobId) {
        this.client = Objects.requireNonNull(client);
        this.jobId = Objects.requireNonNull(jobId);
    }

    /**
     * Delete a list of model snapshots and their corresponding state documents.
     *
     * @param modelSnapshots the model snapshots to delete
     */
    public void deleteModelSnapshots(List<ModelSnapshot> modelSnapshots, ActionListener<BulkResponse> listener) {
        if (modelSnapshots.isEmpty()) {
            listener.onResponse(new BulkResponse(new BulkItemResponse[0], 0L));
            return;
        }

        String stateIndexName = AnomalyDetectorsIndex.jobStateIndexName();

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (ModelSnapshot modelSnapshot : modelSnapshots) {
            for (String stateDocId : modelSnapshot.stateDocumentIds()) {
                bulkRequestBuilder.add(client.prepareDelete(stateIndexName, ElasticsearchMappings.DOC_TYPE, stateDocId));
            }

            bulkRequestBuilder.add(client.prepareDelete(AnomalyDetectorsIndex.jobResultsAliasedName(modelSnapshot.getJobId()),
                    ElasticsearchMappings.DOC_TYPE, ModelSnapshot.documentId(modelSnapshot)));
        }

        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            executeAsyncWithOrigin(client, ML_ORIGIN, BulkAction.INSTANCE, bulkRequestBuilder.request(), listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Asynchronously delete all result types (Buckets, Records, Influencers) from {@code cutOffTime}
     *
     * @param cutoffEpochMs Results at and after this time will be deleted
     * @param listener Response listener
     */
    public void deleteResultsFromTime(long cutoffEpochMs, ActionListener<Boolean> listener) {
        DeleteByQueryHolder deleteByQueryHolder = new DeleteByQueryHolder(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
        deleteByQueryHolder.dbqRequest.setRefresh(true);

        QueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.existsQuery(Result.RESULT_TYPE.getPreferredName()))
                .filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).gte(cutoffEpochMs));
        deleteByQueryHolder.dbqRequest.setIndicesOptions(IndicesOptions.lenientExpandOpen());
        deleteByQueryHolder.dbqRequest.setQuery(query);
        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, deleteByQueryHolder.dbqRequest,
                ActionListener.wrap(r -> listener.onResponse(true), listener::onFailure));
    }

    /**
     * Delete all results marked as interim
     */
    public void deleteInterimResults() {
        DeleteByQueryHolder deleteByQueryHolder = new DeleteByQueryHolder(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
        deleteByQueryHolder.dbqRequest.setRefresh(false);

        deleteByQueryHolder.dbqRequest.setIndicesOptions(IndicesOptions.lenientExpandOpen());
        QueryBuilder qb = QueryBuilders.termQuery(Result.IS_INTERIM.getPreferredName(), true);
        deleteByQueryHolder.dbqRequest.setQuery(new ConstantScoreQueryBuilder(qb));

        try (ThreadContext.StoredContext ignore = stashWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN)) {
            client.execute(DeleteByQueryAction.INSTANCE, deleteByQueryHolder.dbqRequest).get();
        } catch (Exception e) {
            LOGGER.error("[" + jobId + "] An error occurred while deleting interim results", e);
        }
    }

    // Wrapper to ensure safety
    private static class DeleteByQueryHolder {

        private final DeleteByQueryRequest dbqRequest;

        private DeleteByQueryHolder(String index) {
            dbqRequest = new DeleteByQueryRequest();
            dbqRequest.indices(index);
            dbqRequest.setSlices(5);
            dbqRequest.setAbortOnVersionConflict(false);
        }
    }
}
