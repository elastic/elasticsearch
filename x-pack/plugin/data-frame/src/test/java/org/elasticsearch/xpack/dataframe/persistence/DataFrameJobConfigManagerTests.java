/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.dataframe.job.DataFrameJobConfig;
import org.elasticsearch.xpack.dataframe.job.DataFrameJobConfigTests;
import org.junit.Before;

public class DataFrameJobConfigManagerTests extends DataFrameSingleNodeTestCase {

    private DataFrameJobConfigManager jobConfigManager;

    @Before
    public void createComponents() {
        jobConfigManager = new DataFrameJobConfigManager(client(), xContentRegistry());
    }

    public void testGetMissingJob() throws InterruptedException {
        // the index does not exist yet
        assertAsync(listener -> jobConfigManager.getJobConfiguration("not_there", listener), (DataFrameJobConfig) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_JOB, "not_there"), e.getMessage());
        });

        // create one job and test with an existing index
        assertAsync(listener -> jobConfigManager.putJobConfiguration(DataFrameJobConfigTests.randomDataFrameJobConfig(), listener), true,
                null, null);

        // same test, but different code path
        assertAsync(listener -> jobConfigManager.getJobConfiguration("not_there", listener), (DataFrameJobConfig) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_JOB, "not_there"), e.getMessage());
        });
    }

    public void testDeleteMissingJob() throws InterruptedException {
        // the index does not exist yet
        assertAsync(listener -> jobConfigManager.deleteJobConfiguration("not_there", listener), (Boolean) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_JOB, "not_there"), e.getMessage());
        });

        // create one job and test with an existing index
        assertAsync(listener -> jobConfigManager.putJobConfiguration(DataFrameJobConfigTests.randomDataFrameJobConfig(), listener), true,
                null, null);

        // same test, but different code path
        assertAsync(listener -> jobConfigManager.deleteJobConfiguration("not_there", listener), (Boolean) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_JOB, "not_there"), e.getMessage());
        });
    }

    public void testCreateReadDelete() throws InterruptedException {
        DataFrameJobConfig jobConfig = DataFrameJobConfigTests.randomDataFrameJobConfig();

        // create job
        assertAsync(listener -> jobConfigManager.putJobConfiguration(jobConfig, listener), true, null, null);

        // read job
        assertAsync(listener -> jobConfigManager.getJobConfiguration(jobConfig.getId(), listener), jobConfig, null, null);

        // try to create again
        assertAsync(listener -> jobConfigManager.putJobConfiguration(jobConfig, listener), (Boolean) null, null, e -> {
            assertEquals(ResourceAlreadyExistsException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_JOB_EXISTS, jobConfig.getId()), e.getMessage());
        });

        // delete job
        assertAsync(listener -> jobConfigManager.deleteJobConfiguration(jobConfig.getId(), listener), true, null, null);

        // delete again
        assertAsync(listener -> jobConfigManager.deleteJobConfiguration(jobConfig.getId(), listener), (Boolean) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_JOB, jobConfig.getId()), e.getMessage());
        });

        // try to get deleted job
        assertAsync(listener -> jobConfigManager.getJobConfiguration(jobConfig.getId(), listener), (DataFrameJobConfig) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_JOB, jobConfig.getId()), e.getMessage());
        });
    }
}
