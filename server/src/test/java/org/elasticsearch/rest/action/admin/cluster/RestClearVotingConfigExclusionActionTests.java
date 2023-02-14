/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Map;

import static org.elasticsearch.rest.action.admin.cluster.RestClearVotingConfigExclusionsAction.resolveVotingConfigExclusionsRequest;

public class RestClearVotingConfigExclusionActionTests extends ESTestCase {

    public void testDefaultRequest() {
        final var request = resolveVotingConfigExclusionsRequest(
            new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
                .withPath("/_cluster/voting_config_exclusions")
                .withParams(Map.of())
                .build()
        );
        assertEquals(TimeValue.timeValueSeconds(30), request.masterNodeTimeout());
        assertEquals(TimeValue.timeValueSeconds(30), request.getTimeout());
        assertTrue(request.getWaitForRemoval());
    }

    public void testResolveRequestParameters() {
        final var request = resolveVotingConfigExclusionsRequest(
            new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
                .withPath("/_cluster/voting_config_exclusions")
                .withParams(Map.of("master_timeout", "60s", "wait_for_removal", "false"))
                .build()
        );
        assertEquals(TimeValue.timeValueMinutes(1), request.masterNodeTimeout());
        assertEquals(TimeValue.timeValueMinutes(1), request.getTimeout());
        assertFalse(request.getWaitForRemoval());
    }

}
