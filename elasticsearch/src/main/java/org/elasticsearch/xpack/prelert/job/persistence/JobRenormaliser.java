/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.Influencer;

import java.io.IOException;
import java.util.List;


/**
 * Interface for classes that update {@linkplain Bucket Buckets}
 * for a particular job with new normalised anomaly scores and
 * unusual scores
 */
public class JobRenormaliser extends AbstractComponent {

    private final Client client;
    private final JobResultsPersister jobResultsPersister;

    public JobRenormaliser(Settings settings, Client client, JobResultsPersister jobResultsPersister) {
        super(settings);
        this.client = client;
        this.jobResultsPersister = jobResultsPersister;
    }

    /**
     * Update the bucket with the changes that may result
     * due to renormalisation.
     *
     * @param bucket the bucket to update
     */
    public void updateBucket(Bucket bucket) {
        String jobId = bucket.getJobId();
        try {
            String indexName = JobResultsPersister.getJobIndexName(jobId);
            logger.trace("[{}] ES API CALL: index type {} to index {} with ID {}", jobId, Bucket.TYPE, indexName, bucket.getId());
            client.prepareIndex(indexName, Bucket.TYPE.getPreferredName(), bucket.getId())
                    .setSource(jobResultsPersister.serialiseWithJobId(Bucket.TYPE.getPreferredName(), bucket)).execute().actionGet();
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] Error updating bucket state", new Object[]{jobId}, e));
            return;
        }

        // If the update to the bucket was successful, also update the
        // standalone copies of the nested bucket influencers
        try {
            jobResultsPersister.persistBucketInfluencersStandalone(bucket.getJobId(), bucket.getId(), bucket.getBucketInfluencers(),
                    bucket.getTimestamp(), bucket.isInterim());
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] Error updating standalone bucket influencer state", new Object[]{jobId}, e));
            return;
        }
        jobResultsPersister.persistPerPartitionMaxProbabilities(bucket);
    }


    /**
     * Update the anomaly records for a particular bucket and job.
     * The anomaly records are updated with the values in the
     * <code>records</code> list.
     *
     * @param bucketId Id of the bucket to update
     * @param records The new record values
     */
    public void updateRecords(String jobId, String bucketId, List<AnomalyRecord> records) {
        try {
            // Now bulk update the records within the bucket
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            boolean addedAny = false;
            for (AnomalyRecord record : records) {
                String recordId = record.getId();
                String indexName = JobResultsPersister.getJobIndexName(jobId);
                logger.trace("[{}] ES BULK ACTION: update ID {} type {} in index {} using map of new values, for bucket {}",
                        jobId, recordId, AnomalyRecord.TYPE, indexName, bucketId);

                bulkRequest.add(
                        client.prepareIndex(indexName, AnomalyRecord.TYPE.getPreferredName(), recordId)
                                .setSource(jobResultsPersister.serialiseWithJobId(AnomalyRecord.TYPE.getPreferredName(), record)));

                addedAny = true;
            }

            if (addedAny) {
                logger.trace("[{}] ES API CALL: bulk request with {} actions", jobId, bulkRequest.numberOfActions());
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    logger.error("[{}] BulkResponse has errors: {}", jobId, bulkResponse.buildFailureMessage());
                }
            }
        } catch (IOException | ElasticsearchException e) {
            logger.error(new ParameterizedMessage("[{}] Error updating anomaly records", new Object[]{jobId}, e));
        }
    }

    /**
     * Update the influencer for a particular job
     */
    public void updateInfluencer(Influencer influencer) {
        jobResultsPersister.persistInfluencer(influencer);
    }
}

