/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.protocol.xpack.rollup.PutRollupJobRequest;
import org.elasticsearch.protocol.xpack.rollup.job.RollupJobConfig;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomGroupConfig;
import static org.hamcrest.Matchers.equalTo;

public class TransportPutRollupJobActionTests extends ESTestCase {

    public void testBadCron() {
        final PutRollupJobRequest putRollupJobRequest =
            new PutRollupJobRequest(new RollupJobConfig("_id", "index", "rollup", "0 * * *", 100, randomGroupConfig(random()), null, null));

        Exception e = expectThrows(IllegalArgumentException.class, () -> TransportPutRollupJobAction.validate(putRollupJobRequest));
        assertThat(e.getMessage(), equalTo("invalid cron expression [0 * * *]"));
    }
}
