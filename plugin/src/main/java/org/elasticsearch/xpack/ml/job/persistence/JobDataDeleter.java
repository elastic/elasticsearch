/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.common.action.XPackDeleteByQueryAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelState;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.Result;

import java.util.List;
import java.util.Objects;

public class JobDataDeleter {

    private static final Logger LOGGER = Loggers.getLogger(JobDataDeleter.class);

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
                bulkRequestBuilder.add(client.prepareDelete(stateIndexName, ModelState.TYPE.getPreferredName(), stateDocId));
            }

            bulkRequestBuilder.add(client.prepareDelete(AnomalyDetectorsIndex.jobResultsAliasedName(modelSnapshot.getJobId()),
                    ModelSnapshot.TYPE.getPreferredName(), ModelSnapshot.documentId(modelSnapshot)));
        }

        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            bulkRequestBuilder.execute(listener);
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

        RangeQueryBuilder timeRange = QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName());
        timeRange.gte(cutoffEpochMs);
        deleteByQueryHolder.searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        deleteByQueryHolder.searchRequest.types(Result.TYPE.getPreferredName());
        deleteByQueryHolder.searchRequest.source(new SearchSourceBuilder().query(timeRange));
        client.execute(XPackDeleteByQueryAction.INSTANCE, deleteByQueryHolder.dbqRequest, new ActionListener<BulkByScrollResponse>() {
                @Override
                public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                    listener.onResponse(true);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
        });
    }

    /**
     * Delete all results marked as interim
     */
    public void deleteInterimResults() {
        DeleteByQueryHolder deleteByQueryHolder = new DeleteByQueryHolder(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
        deleteByQueryHolder.dbqRequest.setRefresh(false);

        deleteByQueryHolder.searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        deleteByQueryHolder.searchRequest.types(Result.TYPE.getPreferredName());
        QueryBuilder qb = QueryBuilders.termQuery(Result.IS_INTERIM.getPreferredName(), true);
        deleteByQueryHolder.searchRequest.source(new SearchSourceBuilder().query(new ConstantScoreQueryBuilder(qb)));

        try {
            client.execute(XPackDeleteByQueryAction.INSTANCE, deleteByQueryHolder.dbqRequest).get();
        } catch (Exception e) {
            LOGGER.error("[" + jobId + "] An error occurred while deleting interim results", e);
        }
    }

    // Wrapper to ensure safety
    private static class DeleteByQueryHolder {

        private final SearchRequest searchRequest;
        private final DeleteByQueryRequest dbqRequest;

        private DeleteByQueryHolder(String index) {
            // The search request has to be constructed and passed to the DeleteByQueryRequest before more details are set to it
            searchRequest = new SearchRequest(index);
            dbqRequest = new DeleteByQueryRequest(searchRequest);
            dbqRequest.setSlices(5);
            dbqRequest.setAbortOnVersionConflict(false);
        }
    }
}
