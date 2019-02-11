/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

/**
 * Test that ML does not touch unnecessary indices when removing job index aliases
 */
public class JobStorageDeletionTaskIT extends BaseMlIntegTestCase {

    private static final String UNRELATED_INDEX = "unrelated-data";

    public void testUnrelatedIndexNotTouched() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);

        client().admin().indices().prepareCreate(UNRELATED_INDEX).get();

        enableIndexBlock(UNRELATED_INDEX, IndexMetaData.SETTING_READ_ONLY);

        Job.Builder job = createJob("delete-aliases-test-job", new ByteSizeValue(2, ByteSizeUnit.MB));
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
        awaitJobOpenedAndAssigned(job.getId(), null);

        DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(job.getId());
        deleteJobRequest.setForce(true);
        client().execute(DeleteJobAction.INSTANCE, deleteJobRequest).actionGet();

        // If the deletion of aliases touches the unrelated index with the block
        // then the line above will throw a ClusterBlockException

        disableIndexBlock(UNRELATED_INDEX, IndexMetaData.SETTING_READ_ONLY);
    }
}
