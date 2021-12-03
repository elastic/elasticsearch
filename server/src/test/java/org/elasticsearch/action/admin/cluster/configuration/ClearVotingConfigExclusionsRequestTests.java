/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ClearVotingConfigExclusionsRequestTests extends ESTestCase {
    public void testSerialization() throws IOException {
        final ClearVotingConfigExclusionsRequest originalRequest = new ClearVotingConfigExclusionsRequest();
        if (randomBoolean()) {
            originalRequest.setWaitForRemoval(randomBoolean());
        }
        if (randomBoolean()) {
            originalRequest.setTimeout(TimeValue.timeValueMillis(randomLongBetween(0, 30000)));
        }
        final ClearVotingConfigExclusionsRequest deserialized = copyWriteable(
            originalRequest,
            writableRegistry(),
            ClearVotingConfigExclusionsRequest::new
        );
        assertThat(deserialized.getWaitForRemoval(), equalTo(originalRequest.getWaitForRemoval()));
        assertThat(deserialized.getTimeout(), equalTo(originalRequest.getTimeout()));
    }
}
