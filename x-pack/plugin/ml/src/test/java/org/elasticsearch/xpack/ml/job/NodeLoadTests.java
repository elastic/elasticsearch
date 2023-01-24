/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class NodeLoadTests extends ESTestCase {

    public void testIncrementCounts() {
        NodeLoad nodeLoad = NodeLoad.builder("test-node")
            .setMaxJobs(10)
            .incNumAssignedAnomalyDetectorJobs()
            .incNumAssignedDataFrameAnalyticsJobs()
            .incNumAssignedDataFrameAnalyticsJobs()
            .incNumAssignedNativeInferenceModels()
            .incNumAssignedNativeInferenceModels()
            .incNumAssignedNativeInferenceModels()
            .build();

        assertThat(nodeLoad.getNumAssignedJobsAndModels(), equalTo(6));
        assertThat(nodeLoad.remainingJobs(), equalTo(7));
    }
}
