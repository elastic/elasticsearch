/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.Influencer;

import java.util.List;


/**
 * Interface for classes that update {@linkplain Bucket Buckets}
 * for a particular job with new normalised anomaly scores and
 * unusual scores
 */
public interface JobRenormaliser
{
    /**
     * Update the bucket with the changes that may result
     * due to renormalisation.
     *
     * @param bucket the bucket to update
     */
    void updateBucket(Bucket bucket);


    /**
     * Update the anomaly records for a particular bucket and job.
     * The anomaly records are updated with the values in the
     * <code>records</code> list.
     *
     * @param bucketId Id of the bucket to update
     * @param records The new record values
     */
    void updateRecords(String bucketId, List<AnomalyRecord> records);

    /**
     * Update the influencer for a particular job
     */
    void updateInfluencer(Influencer influencer);
}

