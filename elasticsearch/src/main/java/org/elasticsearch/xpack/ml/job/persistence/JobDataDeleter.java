/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.ml.job.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.ModelState;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.Result;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class JobDataDeleter {

    private static final Logger LOGGER = Loggers.getLogger(JobDataDeleter.class);

    private static final int SCROLL_SIZE = 1000;
    private static final String SCROLL_CONTEXT_DURATION = "5m";

    private final Client client;
    private final String jobId;
    private final BulkRequestBuilder bulkRequestBuilder;
    private long deletedResultCount;
    private long deletedModelSnapshotCount;
    private long deletedModelStateCount;
    private boolean quiet;

    public JobDataDeleter(Client client, String jobId) {
        this(client, jobId, false);
    }

    public JobDataDeleter(Client client, String jobId, boolean quiet) {
        this.client = Objects.requireNonNull(client);
        this.jobId = Objects.requireNonNull(jobId);
        bulkRequestBuilder = client.prepareBulk();
        deletedResultCount = 0;
        deletedModelSnapshotCount = 0;
        deletedModelStateCount = 0;
        this.quiet = quiet;
    }

    /**
     * Asynchronously delete all result types (Buckets, Records, Influencers) from {@code cutOffTime}
     *
     * @param cutoffEpochMs Results at and after this time will be deleted
     * @param listener Response listener
     */
    public void deleteResultsFromTime(long cutoffEpochMs, ActionListener<Boolean> listener) {
        String index = AnomalyDetectorsIndex.jobResultsIndexName(jobId);

        RangeQueryBuilder timeRange = QueryBuilders.rangeQuery(Bucket.TIMESTAMP.getPreferredName());
        timeRange.gte(cutoffEpochMs);
        timeRange.lt(new Date().getTime());

        RepeatingSearchScrollListener scrollSearchListener = new RepeatingSearchScrollListener(index, listener);

        client.prepareSearch(index)
                .setTypes(Result.TYPE.getPreferredName())
                .setFetchSource(false)
                .setQuery(timeRange)
                .setScroll(SCROLL_CONTEXT_DURATION)
                .setSize(SCROLL_SIZE)
                .execute(scrollSearchListener);
    }

    private void addDeleteRequestForSearchHits(SearchHits hits, String index) {
        for (SearchHit hit : hits.hits()) {
            LOGGER.trace("Search hit for result: {}", hit.getId());
            addDeleteRequest(hit, index);
        }
        deletedResultCount = hits.getTotalHits();
    }

    private void addDeleteRequest(SearchHit hit, String index) {
        DeleteRequestBuilder deleteRequest = DeleteAction.INSTANCE.newRequestBuilder(client)
                .setIndex(index)
                .setType(hit.getType())
                .setId(hit.getId());
        bulkRequestBuilder.add(deleteRequest);
    }

    /**
     * Delete a {@code ModelSnapshot}
     *
     * @param modelSnapshot the model snapshot to delete
     */
    public void deleteModelSnapshot(ModelSnapshot modelSnapshot) {
        String snapshotId = modelSnapshot.getSnapshotId();
        int docCount = modelSnapshot.getSnapshotDocCount();
        String stateIndexName = AnomalyDetectorsIndex.jobStateIndexName();
        // Deduce the document IDs of the state documents from the information
        // in the snapshot document - we cannot query the state itself as it's
        // too big and has no mappings
        for (int i = 0; i < docCount; ++i) {
            String stateId = snapshotId + '_' + i;
            bulkRequestBuilder.add(client.prepareDelete(stateIndexName, ModelState.TYPE.getPreferredName(), stateId));
            ++deletedModelStateCount;
        }

        bulkRequestBuilder.add(client.prepareDelete(AnomalyDetectorsIndex.jobResultsIndexName(modelSnapshot.getJobId()),
                ModelSnapshot.TYPE.getPreferredName(), snapshotId));
        ++deletedModelSnapshotCount;
    }

    /**
     * Delete all results marked as interim
     */
    public void deleteInterimResults() {
        String index = AnomalyDetectorsIndex.jobResultsIndexName(jobId);

        QueryBuilder qb = QueryBuilders.termQuery(Bucket.IS_INTERIM.getPreferredName(), true);

        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(Result.TYPE.getPreferredName())
                .setQuery(new ConstantScoreQueryBuilder(qb))
                .setFetchSource(false)
                .setScroll(SCROLL_CONTEXT_DURATION)
                .setSize(SCROLL_SIZE)
                .get();

        String scrollId = searchResponse.getScrollId();
        long totalHits = searchResponse.getHits().totalHits();
        long totalDeletedCount = 0;
        while (totalDeletedCount < totalHits) {
            for (SearchHit hit : searchResponse.getHits()) {
                LOGGER.trace("Search hit for result: {}", hit.getId());
                ++totalDeletedCount;
                addDeleteRequest(hit, index);
                ++deletedResultCount;
            }

            searchResponse = client.prepareSearchScroll(scrollId).setScroll(SCROLL_CONTEXT_DURATION).get();
        }
    }

    /**
     * Commit the deletions without enforcing the removal of data from disk
     */
    public void commit(ActionListener<BulkResponse> listener) {
        if (bulkRequestBuilder.numberOfActions() == 0) {
            listener.onResponse(new BulkResponse(new BulkItemResponse[0], 0L));
            return;
        }

        Level logLevel = quiet ? Level.DEBUG : Level.INFO;
        LOGGER.log(logLevel, "Requesting deletion of {} results, {} model snapshots and {} model state documents",
                deletedResultCount, deletedModelSnapshotCount, deletedModelStateCount);

        try {
            bulkRequestBuilder.execute(listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Blocking version of {@linkplain #commit(ActionListener)}
     */
    public void commit() {
        if (bulkRequestBuilder.numberOfActions() == 0) {
            return;
        }

        Level logLevel = quiet ? Level.DEBUG : Level.INFO;
        LOGGER.log(logLevel, "Requesting deletion of {} results, {} model snapshots and {} model state documents",
                deletedResultCount, deletedModelSnapshotCount, deletedModelStateCount);

        BulkResponse response = bulkRequestBuilder.get();
        if (response.hasFailures()) {
            LOGGER.debug("Bulk request has failures. {}", response.buildFailureMessage());
        }
    }

    /**
     * Repeats a scroll search adding the hits to the bulk delete request
     */
    private class RepeatingSearchScrollListener implements ActionListener<SearchResponse> {

        private final AtomicLong totalDeletedCount;
        private final String index;
        private final ActionListener<Boolean> scrollFinishedListener;

        RepeatingSearchScrollListener(String index, ActionListener<Boolean> scrollFinishedListener) {
            totalDeletedCount = new AtomicLong(0L);
            this.index = index;
            this.scrollFinishedListener = scrollFinishedListener;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            addDeleteRequestForSearchHits(searchResponse.getHits(), index);

            totalDeletedCount.addAndGet(searchResponse.getHits().hits().length);
            if (totalDeletedCount.get() < searchResponse.getHits().totalHits()) {
                client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(SCROLL_CONTEXT_DURATION).execute(this);
            }
            else {
                scrollFinishedListener.onResponse(true);
            }
        }

        @Override
        public void onFailure(Exception e) {
            scrollFinishedListener.onFailure(e);
        }
    };
}
