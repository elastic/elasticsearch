/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.xpack.prelert.job.DataCounts;

/**
 * Update a job's dataCounts
 * i.e. the number of processed records, fields etc.
 */
public interface JobDataCountsPersister
{
    /**
     * Update the job's data counts stats and figures.
     *
     * @param jobId Job to update
     * @param counts The counts
     */
    void persistDataCounts(String jobId, DataCounts counts);
}
