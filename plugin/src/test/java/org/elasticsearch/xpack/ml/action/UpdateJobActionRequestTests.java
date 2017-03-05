/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

public class UpdateJobActionRequestTests
        extends AbstractStreamableTestCase<UpdateJobAction.Request> {

    @Override
    protected UpdateJobAction.Request createTestInstance() {
        String jobId = randomAsciiOfLength(10);
        // no need to randomize JobUpdate this is already tested in: JobUpdateTests
        JobUpdate.Builder jobUpdate = new JobUpdate.Builder(jobId);
        jobUpdate.setAnalysisLimits(new AnalysisLimits(100L, 100L));
        return new UpdateJobAction.Request(jobId, jobUpdate.build());
    }

    @Override
    protected UpdateJobAction.Request createBlankInstance() {
        return new UpdateJobAction.Request();
    }

}
