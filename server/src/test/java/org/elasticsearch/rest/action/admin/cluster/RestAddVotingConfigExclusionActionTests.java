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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.action.admin.cluster.RestAddVotingConfigExclusionAction.resolveVotingConfigExclusionsRequest;

public class RestAddVotingConfigExclusionActionTests extends ESTestCase {

    public void testResolveVotingConfigExclusionsRequestNodeIds() {
        Map<String, String> params = new HashMap<>();
        params.put("node_ids", "node-1,node-2,node-3");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cluster/voting_config_exclusions")
            .withParams(params)
            .build();

        AddVotingConfigExclusionsRequest addVotingConfigExclusionsRequest = resolveVotingConfigExclusionsRequest(request);
        String[] expected = { "node-1", "node-2", "node-3" };
        assertArrayEquals(expected, addVotingConfigExclusionsRequest.getNodeIds());
        assertArrayEquals(Strings.EMPTY_ARRAY, addVotingConfigExclusionsRequest.getNodeNames());
        assertEquals(TimeValue.timeValueSeconds(30), addVotingConfigExclusionsRequest.getTimeout());
        assertEquals(TimeValue.timeValueSeconds(30), addVotingConfigExclusionsRequest.masterNodeTimeout());
    }

    public void testResolveVotingConfigExclusionsRequestNodeNames() {
        Map<String, String> params = new HashMap<>();
        params.put("node_names", "node-1,node-2,node-3");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cluster/voting_config_exclusions")
            .withParams(params)
            .build();

        AddVotingConfigExclusionsRequest addVotingConfigExclusionsRequest = resolveVotingConfigExclusionsRequest(request);
        String[] expected = { "node-1", "node-2", "node-3" };
        assertArrayEquals(Strings.EMPTY_ARRAY, addVotingConfigExclusionsRequest.getNodeIds());
        assertArrayEquals(expected, addVotingConfigExclusionsRequest.getNodeNames());
    }

    public void testResolveVotingConfigExclusionsRequestTimeout() {
        Map<String, String> params = new HashMap<>();
        params.put("node_names", "node-1,node-2,node-3");
        params.put("timeout", "60s");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cluster/voting_config_exclusions")
            .withParams(params)
            .build();

        AddVotingConfigExclusionsRequest addVotingConfigExclusionsRequest = resolveVotingConfigExclusionsRequest(request);
        assertEquals(TimeValue.timeValueMinutes(1), addVotingConfigExclusionsRequest.getTimeout());
        assertEquals(TimeValue.timeValueSeconds(30), addVotingConfigExclusionsRequest.masterNodeTimeout());
    }

    public void testResolveVotingConfigExclusionsRequestMasterTimeout() {
        Map<String, String> params = new HashMap<>();
        params.put("node_names", "node-1,node-2,node-3");
        params.put("master_timeout", "60s");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cluster/voting_config_exclusions")
            .withParams(params)
            .build();

        AddVotingConfigExclusionsRequest addVotingConfigExclusionsRequest = resolveVotingConfigExclusionsRequest(request);
        assertEquals(TimeValue.timeValueSeconds(30), addVotingConfigExclusionsRequest.getTimeout());
        assertEquals(TimeValue.timeValueMinutes(1), addVotingConfigExclusionsRequest.masterNodeTimeout());
    }

    public void testResolveVotingConfigExclusionsRequestTimeoutAndMasterTimeout() {
        Map<String, String> params = new HashMap<>();
        params.put("node_names", "node-1,node-2,node-3");
        params.put("timeout", "60s");
        params.put("master_timeout", "120s");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cluster/voting_config_exclusions")
            .withParams(params)
            .build();

        AddVotingConfigExclusionsRequest addVotingConfigExclusionsRequest = resolveVotingConfigExclusionsRequest(request);
        assertEquals(TimeValue.timeValueMinutes(1), addVotingConfigExclusionsRequest.getTimeout());
        assertEquals(TimeValue.timeValueMinutes(2), addVotingConfigExclusionsRequest.masterNodeTimeout());
    }

}
