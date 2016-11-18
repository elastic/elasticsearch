/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.ModelState;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.BucketInfluencer;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.ModelDebugOutput;

import java.util.Objects;
import java.util.function.LongSupplier;

public class ElasticsearchBulkDeleter implements JobDataDeleter {
    private static final Logger LOGGER = Loggers.getLogger(ElasticsearchBulkDeleter.class);

    private static final int SCROLL_SIZE = 1000;
    private static final String SCROLL_CONTEXT_DURATION = "5m";

    private final Client client;
    private final String jobId;
    private final BulkRequestBuilder bulkRequestBuilder;
    private long deletedBucketCount;
    private long deletedRecordCount;
    private long deletedBucketInfluencerCount;
    private long deletedInfluencerCount;
    private long deletedModelSnapshotCount;
    private long deletedModelStateCount;
    private boolean quiet;

    public ElasticsearchBulkDeleter(Client client, String jobId, boolean quiet) {
        this.client = Objects.requireNonNull(client);
        this.jobId = Objects.requireNonNull(jobId);
        bulkRequestBuilder = client.prepareBulk();
        deletedBucketCount = 0;
        deletedRecordCount = 0;
        deletedBucketInfluencerCount = 0;
        deletedInfluencerCount = 0;
        deletedModelSnapshotCount = 0;
        deletedModelStateCount = 0;
        this.quiet = quiet;
    }

    public ElasticsearchBulkDeleter(Client client, String jobId) {
        this(client, jobId, false);
    }

    @Override
    public void deleteBucket(Bucket bucket) {
        deleteRecords(bucket);
        deleteBucketInfluencers(bucket);
        bulkRequestBuilder.add(
                client.prepareDelete(ElasticsearchPersister.getJobIndexName(jobId), Bucket.TYPE.getPreferredName(), bucket.getId()));
        ++deletedBucketCount;
    }

    @Override
    public void deleteRecords(Bucket bucket) {
        // Find the records using the time stamp rather than a parent-child
        // relationship.  The parent-child filter involves two queries behind
        // the scenes, and Elasticsearch documentation claims it's significantly
        // slower.  Here we rely on the record timestamps being identical to the
        // bucket timestamp.
        deleteTypeByBucket(bucket, AnomalyRecord.TYPE.getPreferredName(), () -> ++deletedRecordCount);
    }

    private void deleteTypeByBucket(Bucket bucket, String type, LongSupplier deleteCounter) {
        QueryBuilder query = QueryBuilders.termQuery(ElasticsearchMappings.ES_TIMESTAMP,
                bucket.getTimestamp().getTime());

        int done = 0;
        boolean finished = false;
        while (finished == false) {
            SearchResponse searchResponse = SearchAction.INSTANCE.newRequestBuilder(client)
                    .setIndices(ElasticsearchPersister.getJobIndexName(jobId))
                    .setTypes(type)
                    .setQuery(query)
                    .addSort(SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC))
                    .setSize(SCROLL_SIZE)
                    .setFrom(done)
                    .execute().actionGet();

            for (SearchHit hit : searchResponse.getHits()) {
                ++done;
                addDeleteRequest(hit);
                deleteCounter.getAsLong();
            }
            if (searchResponse.getHits().getTotalHits() == done) {
                finished = true;
            }
        }
    }

    private void addDeleteRequest(SearchHit hit) {
        DeleteRequestBuilder deleteRequest = DeleteAction.INSTANCE.newRequestBuilder(client)
                .setIndex(ElasticsearchPersister.getJobIndexName(jobId))
                .setType(hit.getType())
                .setId(hit.getId());
        SearchHitField parentField = hit.field(ElasticsearchMappings.PARENT);
        if (parentField != null) {
            deleteRequest.setParent(parentField.getValue().toString());
        }
        bulkRequestBuilder.add(deleteRequest);
    }

    public void deleteBucketInfluencers(Bucket bucket) {
        // Find the bucket influencers using the time stamp, relying on the
        // bucket influencer timestamps being identical to the bucket timestamp.
        deleteTypeByBucket(bucket, BucketInfluencer.TYPE.getPreferredName(), () -> ++deletedBucketInfluencerCount);
    }

    public void deleteInfluencers(Bucket bucket) {
        // Find the influencers using the time stamp, relying on the influencer
        // timestamps being identical to the bucket timestamp.
        deleteTypeByBucket(bucket, Influencer.TYPE.getPreferredName(), () -> ++deletedInfluencerCount);
    }

    public void deleteBucketByTime(Bucket bucket) {
        deleteTypeByBucket(bucket, Bucket.TYPE.getPreferredName(), () -> ++deletedBucketCount);
    }

    @Override
    public void deleteInfluencer(Influencer influencer) {
        String id = influencer.getId();
        if (id == null) {
            LOGGER.error("Cannot delete specific influencer without an ID",
                    // This means we get a stack trace to show where the request came from
                    new NullPointerException());
            return;
        }
        bulkRequestBuilder.add(
                client.prepareDelete(ElasticsearchPersister.getJobIndexName(jobId), Influencer.TYPE.getPreferredName(), id));
        ++deletedInfluencerCount;
    }

    @Override
    public void deleteModelSnapshot(ModelSnapshot modelSnapshot) {
        String snapshotId = modelSnapshot.getSnapshotId();
        int docCount = modelSnapshot.getSnapshotDocCount();
        String indexName = ElasticsearchPersister.getJobIndexName(jobId);
        // Deduce the document IDs of the state documents from the information
        // in the snapshot document - we cannot query the state itself as it's
        // too big and has no mappings
        for (int i = 0; i < docCount; ++i) {
            String stateId = snapshotId + '_' + i;
            bulkRequestBuilder.add(
                    client.prepareDelete(indexName, ModelState.TYPE, stateId));
            ++deletedModelStateCount;
        }

        bulkRequestBuilder.add(
                client.prepareDelete(indexName, ModelSnapshot.TYPE.getPreferredName(), snapshotId));
        ++deletedModelSnapshotCount;
    }

    @Override
    public void deleteModelDebugOutput(ModelDebugOutput modelDebugOutput) {
        String id = modelDebugOutput.getId();
        bulkRequestBuilder.add(
                client.prepareDelete(ElasticsearchPersister.getJobIndexName(jobId), ModelDebugOutput.TYPE.getPreferredName(), id));
    }

    @Override
    public void deleteModelSizeStats(ModelSizeStats modelSizeStats) {
        bulkRequestBuilder.add(client.prepareDelete(
                ElasticsearchPersister.getJobIndexName(jobId), ModelSizeStats.TYPE.getPreferredName(), modelSizeStats.getId()));
    }

    public void deleteInterimResults() {
        QueryBuilder qb = QueryBuilders.termQuery(Bucket.IS_INTERIM.getPreferredName(), true);

        SearchResponse searchResponse = client.prepareSearch(ElasticsearchPersister.getJobIndexName(jobId))
                .setTypes(Bucket.TYPE.getPreferredName(), AnomalyRecord.TYPE.getPreferredName(), Influencer.TYPE.getPreferredName(),
                        BucketInfluencer.TYPE.getPreferredName())
                .setQuery(qb)
                .addSort(SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC))
                .setScroll(SCROLL_CONTEXT_DURATION)
                .setSize(SCROLL_SIZE)
                .get();

        String scrollId = searchResponse.getScrollId();
        long totalHits = searchResponse.getHits().totalHits();
        long totalDeletedCount = 0;
        while (totalDeletedCount < totalHits) {
            for (SearchHit hit : searchResponse.getHits()) {
                LOGGER.trace("Search hit for bucket: " + hit.toString() + ", " + hit.getId());
                String type = hit.getType();
                if (type.equals(Bucket.TYPE)) {
                    ++deletedBucketCount;
                } else if (type.equals(AnomalyRecord.TYPE)) {
                    ++deletedRecordCount;
                } else if (type.equals(BucketInfluencer.TYPE)) {
                    ++deletedBucketInfluencerCount;
                } else if (type.equals(Influencer.TYPE)) {
                    ++deletedInfluencerCount;
                }
                ++totalDeletedCount;
                addDeleteRequest(hit);
            }

            searchResponse = client.prepareSearchScroll(scrollId).setScroll(SCROLL_CONTEXT_DURATION).get();
        }
    }

    /**
     * Commits the deletions and if {@code forceMerge} is {@code true}, it
     * forces a merge which removes the data from disk.
     */
    @Override
    public void commit(ActionListener<BulkResponse> listener) {
        if (bulkRequestBuilder.numberOfActions() == 0) {
            listener.onResponse(new BulkResponse(new BulkItemResponse[0], 0L));
            return;
        }

        if (!quiet) {
            LOGGER.debug("Requesting deletion of "
                    + deletedBucketCount + " buckets, "
                    + deletedRecordCount + " records, "
                    + deletedBucketInfluencerCount + " bucket influencers, "
                    + deletedInfluencerCount + " influencers, "
                    + deletedModelSnapshotCount + " model snapshots, "
                    + " and "
                    + deletedModelStateCount + " model state documents");
        }

        try {
            bulkRequestBuilder.execute(listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
