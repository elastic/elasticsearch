/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Date;

public class TransportDeleteJobActionTests extends ESTestCase {

    public void testJobIsDeletedFromState() {
        MlMetadata mlMetadata = MlMetadata.EMPTY_METADATA;

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata))
                .build();

        assertTrue(TransportDeleteJobAction.jobIsDeletedFromState("job_id_1", clusterState));

        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(BaseMlIntegTestCase.createScheduledJob("job_id_1").build(new Date()), false);
        mlMetadata = mlBuilder.build();
        clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata))
                .build();

        assertFalse(TransportDeleteJobAction.jobIsDeletedFromState("job_id_1", clusterState));
    }
}
