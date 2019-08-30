/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;

public class UpdateJobActionRequestTests
        extends AbstractWireSerializingTestCase<UpdateJobAction.Request> {

    @Override
    protected UpdateJobAction.Request createTestInstance() {
        String jobId = randomAlphaOfLength(10);
        // no need to randomize JobUpdate this is already tested in: JobUpdateTests
        JobUpdate.Builder jobUpdate = new JobUpdate.Builder(jobId);
        jobUpdate.setAnalysisLimits(new AnalysisLimits(100L, 100L));
        UpdateJobAction.Request request;
        if (randomBoolean()) {
            request = new UpdateJobAction.Request(jobId, jobUpdate.build());
        } else {
            // this call sets isInternal = true
            request = UpdateJobAction.Request.internal(jobId, jobUpdate.build());
        }

        return request;
    }

    @Override
    protected Writeable.Reader<UpdateJobAction.Request> instanceReader() {
        return UpdateJobAction.Request::new;
    }
}
