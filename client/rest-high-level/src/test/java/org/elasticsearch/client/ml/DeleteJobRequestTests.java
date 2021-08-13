/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.JobTests;
import org.elasticsearch.test.ESTestCase;

public class DeleteJobRequestTests extends ESTestCase {

    private DeleteJobRequest createTestInstance() {
        return new DeleteJobRequest(JobTests.randomValidJobId());
    }

    public void test_WithNullJobId() {
        NullPointerException ex = expectThrows(NullPointerException.class, () -> new DeleteJobRequest(null));
        assertEquals("[job_id] must not be null", ex.getMessage());

        ex = expectThrows(NullPointerException.class, () -> createTestInstance().setJobId(null));
        assertEquals("[job_id] must not be null", ex.getMessage());
    }
}
