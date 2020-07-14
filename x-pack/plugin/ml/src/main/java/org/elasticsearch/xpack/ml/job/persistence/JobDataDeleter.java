/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

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
    public void deleteModelSnapshots(List<ModelSnapshot> modelSnapshots, ActionListener<BulkByScrollResponse> listener) {
        if (modelSnapshots.isEmpty()) {
            listener.onResponse(new BulkByScrollResponse(TimeValue.ZERO,
                new BulkByScrollTask.Status(Collections.emptyList(), null),
                Collections.emptyList(),
                Collections.emptyList(),
                false));
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

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(indices.toArray(new String[0]))
            .setRefresh(true)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(idsToDelete.toArray(new String[0])));

        // _doc is the most efficient sort order and will also disable scoring
        deleteByQueryRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, deleteByQueryRequest, listener);
    }

    /**
     * Asynchronously delete all the auto-generated (i.e. created by the _xpack user) annotations
     *
     * @param listener Response listener
     */
    public void deleteAllAnnotations(ActionListener<Boolean> listener) {
        deleteAnnotationsFromTime(null, null, listener);
    }

    /**
     * Asynchronously delete all the auto-generated (i.e. created by the _xpack user) annotations starting from {@code cutOffTime}
     *
     * @param cutoffEpochMs Only annotations at and after this time will be deleted. If {@code null}, no cutoff is applied
     * @param eventsToDelete Only annotations with one of the provided event types will be deleted.
     *                       If {@code null} or empty, no event-related filtering is applied
     * @param listener Response listener
     */
    public void deleteAnnotationsFromTime(@Nullable Long cutoffEpochMs,
                                          @Nullable Set<String> eventsToDelete,
                                          ActionListener<Boolean> listener) {
        BoolQueryBuilder boolQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
                .filter(QueryBuilders.termQuery(Annotation.CREATE_USERNAME.getPreferredName(), XPackUser.NAME));
        if (cutoffEpochMs != null) {
            boolQuery.filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).gte(cutoffEpochMs));
        }
        if (eventsToDelete != null && eventsToDelete.isEmpty() == false) {
            boolQuery.filter(QueryBuilders.termsQuery(Annotation.EVENT.getPreferredName(), eventsToDelete));
        }
        QueryBuilder query = QueryBuilders.constantScoreQuery(boolQuery);
        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(AnnotationIndex.READ_ALIAS_NAME)
            .setQuery(query)
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
            ActionListener.wrap(r -> listener.onResponse(true), listener::onFailure));
    }

    /**
     * Asynchronously delete all result types (Buckets, Records, Influencers) from {@code cutOffTime}
     *
     * @param cutoffEpochMs Results at and after this time will be deleted
     * @param listener Response listener
     */
    public void deleteResultsFromTime(long cutoffEpochMs, ActionListener<Boolean> listener) {
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.existsQuery(Result.RESULT_TYPE.getPreferredName()))
            .filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).gte(cutoffEpochMs));
        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
            .setQuery(query)
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
            ActionListener.wrap(r -> listener.onResponse(true), listener::onFailure));
    }

    /**
     * Delete all results marked as interim
     */
    public void deleteInterimResults() {
        QueryBuilder query = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery(Result.IS_INTERIM.getPreferredName(), true));
        DeleteByQueryRequest dbqRequest = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
            .setQuery(query)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setAbortOnVersionConflict(false)
            .setRefresh(false)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);

        // _doc is the most efficient sort order and will also disable scoring
        dbqRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            client.execute(DeleteByQueryAction.INSTANCE, dbqRequest).get();
        } catch (Exception e) {
            LOGGER.error("[" + jobId + "] An error occurred while deleting interim results", e);
        }
    }

    /**
     * Delete the datafeed timing stats document from all the job results indices
     *
     * @param listener Response listener
     */
    public void deleteDatafeedTimingStats(ActionListener<BulkByScrollResponse> listener) {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
            .setRefresh(true)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(DatafeedTimingStats.documentId(jobId)));

        // _doc is the most efficient sort order and will also disable scoring
        deleteByQueryRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, deleteByQueryRequest, listener);
    }
}
