/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.PerPartitionMaxProbabilities;

import java.util.List;


/**
 * Interface for classes that update {@linkplain Bucket Buckets}
 * for a particular job with new normalized anomaly scores and
 * unusual scores.
 *
 * Renormalized results must already have an ID.
 */
public class JobRenormalizedResultsPersister extends AbstractComponent {

    private final JobResultsPersister jobResultsPersister;

    public JobRenormalizedResultsPersister(Settings settings, JobResultsPersister jobResultsPersister) {
        super(settings);
        this.jobResultsPersister = jobResultsPersister;
    }

    /**
     * Update the bucket with the changes that may result
     * due to renormalization.
     *
     * @param bucket the bucket to update
     */
    public void updateBucket(Bucket bucket) {
        jobResultsPersister.bulkPersisterBuilder(bucket.getJobId()).persistBucket(bucket).executeRequest();
    }

    /**
     * Update the anomaly records for a particular job.
     * The anomaly records are updated with the values in <code>records</code> and
     * stored with the ID returned by {@link AnomalyRecord#getId()}
     *
     * @param jobId Id of the job to update
     * @param records The updated records
     */
    public void updateRecords(String jobId, List<AnomalyRecord> records) {
        jobResultsPersister.bulkPersisterBuilder(jobId).persistRecords(records).executeRequest();
    }

    /**
     * Create a {@link PerPartitionMaxProbabilities} object from this list of records and persist
     * with the given ID.
     *
     * @param jobId Id of the job to update
     * @param records Source of the new {@link PerPartitionMaxProbabilities} object
     */
    public void updatePerPartitionMaxProbabilities(String jobId, List<AnomalyRecord> records) {
        PerPartitionMaxProbabilities ppMaxProbs = new PerPartitionMaxProbabilities(records);
        jobResultsPersister.bulkPersisterBuilder(jobId).persistPerPartitionMaxProbabilities(ppMaxProbs).executeRequest();
    }

    /**
     * Update the influencer for a particular job.
     * The Influencer's are stored with the ID in {@link Influencer#getId()}
     *
     * @param jobId Id of the job to update
     * @param influencers The updated influencers
     */
    public void updateInfluencer(String jobId, List<Influencer> influencers) {
        jobResultsPersister.bulkPersisterBuilder(jobId).persistInfluencers(influencers).executeRequest();
    }
}

