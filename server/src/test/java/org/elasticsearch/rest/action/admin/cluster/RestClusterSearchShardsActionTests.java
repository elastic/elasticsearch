/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Map;

public class RestClusterSearchShardsActionTests extends RestActionTestCase {
    private RestClusterSearchShardsAction action;

    @Before
    public void setUpAction() {
        action = new RestClusterSearchShardsAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(TransportClusterSearchShardsAction.TYPE, actionType);
            return emptyResponse();
        });
    }

    public void testParseSearchShardsRequestWithSliceParam() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            ClusterSearchShardsRequest clusterRequest = (ClusterSearchShardsRequest) request;
            assertEquals("s1,s2", clusterRequest.routing());
            assertTrue(clusterRequest.isRoutingFromSlice());
            assertEquals("s1,s2", clusterRequest.searchSlice());
            return emptyResponse();
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search_shards")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1,s2"))
            .build();
        action.handleRequest(request, new FakeRestChannel(request, randomBoolean()), verifyingClient);
    }

    public void testParseSearchShardsRequestWithSliceAllParam() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            ClusterSearchShardsRequest clusterRequest = (ClusterSearchShardsRequest) request;
            assertNull(clusterRequest.routing());
            assertTrue(clusterRequest.isRoutingFromSlice());
            assertEquals(SliceIndexing.SLICE_ALL, clusterRequest.searchSlice());
            return emptyResponse();
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search_shards")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, SliceIndexing.SLICE_ALL))
            .build();
        action.handleRequest(request, new FakeRestChannel(request, randomBoolean()), verifyingClient);
    }

    public void testParseSearchShardsRequestRejectsRoutingAndSliceTogether() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search_shards")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1", "routing", "r1"))
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
        assertEquals("[routing] is not allowed together with [_slice]", e.getMessage());
    }

    public void testParseSearchShardsRequestRejectsSliceWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_search_shards")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1"))
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
        assertEquals("request does not support [_slice]", e.getMessage());
    }

    private static ClusterSearchShardsResponse emptyResponse() {
        return new ClusterSearchShardsResponse(new ClusterSearchShardsGroup[0], new DiscoveryNode[0], Map.of());
    }
}
