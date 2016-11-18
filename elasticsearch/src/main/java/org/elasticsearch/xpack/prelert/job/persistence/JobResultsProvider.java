/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.persistence.InfluencersQueryBuilder.InfluencersQuery;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.CategoryDefinition;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.ModelDebugOutput;

import java.util.Optional;

public interface JobResultsProvider
{
    /**
     * Search for buckets with the parameters in the {@link BucketsQueryBuilder}
     * @return QueryPage of Buckets
     * @throws ResourceNotFoundException If the job id is no recognised
     */
    QueryPage<Bucket> buckets(String jobId, BucketsQueryBuilder.BucketsQuery query)
            throws ResourceNotFoundException;

    /**
     * Get the bucket at time <code>timestampMillis</code> from the job.
     *
     * @param jobId the job id
     * @param query The bucket query
     * @return QueryPage Bucket
     * @throws ResourceNotFoundException If the job id is not recognised
     */
    QueryPage<Bucket> bucket(String jobId, BucketQueryBuilder.BucketQuery query)
            throws ResourceNotFoundException;

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a large number of buckets of the given job
     *
     * @param jobId the id of the job for which buckets are requested
     * @return a bucket {@link BatchedDocumentsIterator}
     */
    BatchedDocumentsIterator<Bucket> newBatchedBucketsIterator(String jobId);

    /**
     * Expand a bucket to include the associated records.
     *
     * @param jobId the job id
     * @param includeInterim Include interim results
     * @param bucket The bucket to be expanded
     * @return The number of records added to the bucket
     */
    int expandBucket(String jobId, boolean includeInterim, Bucket bucket);

    /**
     * Get a page of {@linkplain CategoryDefinition}s for the given <code>jobId</code>.
     *
     * @param jobId the job id
     * @param from Skip the first N categories. This parameter is for paging
     * @param size Take only this number of categories
     * @return QueryPage of CategoryDefinition
     */
    QueryPage<CategoryDefinition> categoryDefinitions(String jobId, int from, int size);

    /**
     * Get the specific CategoryDefinition for the given job and category id.
     *
     * @param jobId the job id
     * @param categoryId Unique id
     * @return QueryPage CategoryDefinition
     */
    QueryPage<CategoryDefinition> categoryDefinition(String jobId, String categoryId);

    /**
     * Search for anomaly records with the parameters in the
     * {@link org.elasticsearch.xpack.prelert.job.persistence.RecordsQueryBuilder.RecordsQuery}
     * @return QueryPage of AnomalyRecords
     */
    QueryPage<AnomalyRecord> records(String jobId, RecordsQueryBuilder.RecordsQuery query);

    /**
     * Return a page of influencers for the given job and within the given date
     * range
     *
     * @param jobId
     *            The job ID for which influencers are requested
     * @param query
     *            the query
     * @return QueryPage of Influencer
     */
    QueryPage<Influencer> influencers(String jobId, InfluencersQuery query)
            throws ResourceNotFoundException;

    /**
     * Get the influencer for the given job for id
     *
     * @param jobId the job id
     * @param influencerId The unique influencer Id
     * @return Optional Influencer
     */
    Optional<Influencer> influencer(String jobId, String influencerId);

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a large number of influencers of the given job
     *
     * @param jobId the id of the job for which influencers are requested
     * @return an influencer {@link BatchedDocumentsIterator}
     */
    BatchedDocumentsIterator<Influencer> newBatchedInfluencersIterator(String jobId);

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a number of model snapshots of the given job
     *
     * @param jobId the id of the job for which model snapshots are requested
     * @return a model snapshot {@link BatchedDocumentsIterator}
     */
    BatchedDocumentsIterator<ModelSnapshot> newBatchedModelSnapshotIterator(String jobId);

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a number of ModelDebugOutputs of the given job
     *
     * @param jobId the id of the job for which model snapshots are requested
     * @return a model snapshot {@link BatchedDocumentsIterator}
     */
    BatchedDocumentsIterator<ModelDebugOutput> newBatchedModelDebugOutputIterator(String jobId);

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a number of ModelSizeStats of the given job
     *
     * @param jobId the id of the job for which model snapshots are requested
     * @return a model snapshot {@link BatchedDocumentsIterator}
     */
    BatchedDocumentsIterator<ModelSizeStats> newBatchedModelSizeStatsIterator(String jobId);
}
