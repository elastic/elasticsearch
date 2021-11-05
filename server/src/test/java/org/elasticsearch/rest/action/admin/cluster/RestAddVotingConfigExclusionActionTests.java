/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class RestAddVotingConfigExclusionActionTests extends RestActionTestCase {

    private RestAddVotingConfigExclusionAction action;

    @Before
    public void setupAction() {
        action = new RestAddVotingConfigExclusionAction();
        controller().registerHandler(action);
    }

    public void testResolveVotingConfigExclusionsRequestNodeIds() {
        Map<String, String> params = new HashMap<>();
        params.put("node_ids", "node-1,node-2,node-3");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_cluster/voting_config_exclusions")
            .withParams(params)
            .build();

        AddVotingConfigExclusionsRequest addVotingConfigExclusionsRequest = action.resolveVotingConfigExclusionsRequest(request);
        String[] expected = { "node-1", "node-2", "node-3" };
        assertArrayEquals(expected, addVotingConfigExclusionsRequest.getNodeIds());
        assertArrayEquals(Strings.EMPTY_ARRAY, addVotingConfigExclusionsRequest.getNodeNames());
    }

    public void testResolveVotingConfigExclusionsRequestNodeNames() {
        Map<String, String> params = new HashMap<>();
        params.put("node_names", "node-1,node-2,node-3");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_cluster/voting_config_exclusions")
            .withParams(params)
            .build();

        AddVotingConfigExclusionsRequest addVotingConfigExclusionsRequest = action.resolveVotingConfigExclusionsRequest(request);
        String[] expected = { "node-1", "node-2", "node-3" };
        assertArrayEquals(Strings.EMPTY_ARRAY, addVotingConfigExclusionsRequest.getNodeIds());
        assertArrayEquals(expected, addVotingConfigExclusionsRequest.getNodeNames());
    }

}
