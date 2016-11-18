/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.ModelDebugOutput;

public interface JobDataDeleter {
    /**
     * Delete a {@code Bucket} and its records
     *
     * @param bucket the bucket to delete
     */
    void deleteBucket(Bucket bucket);

    /**
     * Delete the records of a {@code Bucket}
     *
     * @param bucket the bucket whose records to delete
     */
    void deleteRecords(Bucket bucket);

    /**
     * Delete an {@code Influencer}
     *
     * @param influencer the influencer to delete
     */
    void deleteInfluencer(Influencer influencer);

    /**
     * Delete a {@code ModelSnapshot}
     *
     * @param modelSnapshot the model snapshot to delete
     */
    void deleteModelSnapshot(ModelSnapshot modelSnapshot);

    /**
     * Delete a {@code ModelDebugOutput} record
     *
     * @param modelDebugOutput to delete
     */
    void deleteModelDebugOutput(ModelDebugOutput modelDebugOutput);

    /**
     * Delete a {@code ModelSizeStats} record
     *
     * @param modelSizeStats to delete
     */
    void deleteModelSizeStats(ModelSizeStats modelSizeStats);

    /**
     * Commit the deletions without enforcing the removal of data from disk
     */
    void commit(ActionListener<BulkResponse> listener);
}
