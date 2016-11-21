/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.prelert.action.DeleteJobAction;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.lists.ListDocument;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

public interface JobProvider extends JobResultsProvider {

    /**
     * Get the persisted quantiles state for the job
     */
    Optional<Quantiles> getQuantiles(String jobId);

    /**
     * Get model snapshots for the job ordered by descending restore priority.
     *
     * @param jobId the job id
     * @param from  number of snapshots to from
     * @param size  number of snapshots to retrieve
     * @return page of model snapshots
     */
    QueryPage<ModelSnapshot> modelSnapshots(String jobId, int from, int size);

    /**
     * Get model snapshots for the job ordered by descending restore priority.
     *
     * @param jobId          the job id
     * @param from           number of snapshots to from
     * @param size           number of snapshots to retrieve
     * @param startEpochMs   earliest time to include (inclusive)
     * @param endEpochMs     latest time to include (exclusive)
     * @param sortField      optional sort field name (may be null)
     * @param sortDescending Sort in descending order
     * @param snapshotId     optional snapshot ID to match (null for all)
     * @param description    optional description to match (null for all)
     * @return page of model snapshots
     */
    QueryPage<ModelSnapshot> modelSnapshots(String jobId, int from, int size,
            String startEpochMs, String endEpochMs, String sortField, boolean sortDescending,
            String snapshotId, String description);

    /**
     * Update a persisted model snapshot metadata document to match the
     * argument supplied.
     *
     * @param jobId                 the job id
     * @param modelSnapshot         the updated model snapshot object to be stored
     * @param restoreModelSizeStats should the model size stats in this
     *                              snapshot be made the current ones for this job?
     */
    void updateModelSnapshot(String jobId, ModelSnapshot modelSnapshot,
            boolean restoreModelSizeStats);

    /**
     * Given a model snapshot, get the corresponding state and write it to the supplied
     * stream.  If there are multiple state documents they are separated using <code>'\0'</code>
     * when written to the stream.
     * @param jobId         the job id
     * @param modelSnapshot the model snapshot to be restored
     * @param restoreStream the stream to write the state to
     */
    void restoreStateToStream(String jobId, ModelSnapshot modelSnapshot, OutputStream restoreStream) throws IOException;

    /**
     * Get the job's model size stats.
     */
    Optional<ModelSizeStats> modelSizeStats(String jobId);

    /**
     * Get the job's data counts
     * @param jobId The job id
     * @return The dataCounts or default constructed object if not found
     */
    DataCounts dataCounts(String jobId);

    /**
     * Retrieves the list with the given {@code listId} from the datastore.
     *
     * @param listId the id of the requested list
     * @return the matching list if it exists
     */
    Optional<ListDocument> getList(String listId);

    /**
     * Get an auditor for the given job
     *
     * @param jobId the job id
     * @return the {@code Auditor}
     */
    Auditor audit(String jobId);

    /**
     * Save the details of the new job to the datastore.
     * Throws <code>JobIdAlreadyExistsException</code> if a job with the
     * same Id already exists.
     */
    // TODO: rename and move?
    void createJobRelatedIndices(Job job, ActionListener<Boolean> listener);

    /**
     * Delete all the job related documents from the database.
     */
    // TODO: should live together with createJobRelatedIndices (in case it moves)?
    void deleteJobRelatedIndices(String jobId, ActionListener<DeleteJobAction.Response> listener);
}
