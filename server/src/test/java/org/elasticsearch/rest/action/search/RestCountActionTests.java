/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;

import java.util.Map;

public class RestCountActionTests extends RestActionTestCase {

    public void testApplyRoutingOrSliceWithSliceParam() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_count")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1,s2"))
            .build();
        SearchRequest countRequest = new SearchRequest();
        RestCountAction.applyRoutingOrSliceForCountRequest(request, countRequest);
        assertEquals("s1,s2", countRequest.routing());
        assertTrue(countRequest.isRoutingFromSlice());
        assertEquals("s1,s2", countRequest.searchSlice());
    }

    public void testApplyRoutingOrSliceWithSliceAllParam() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_count")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, SliceIndexing.SLICE_ALL))
            .build();
        SearchRequest countRequest = new SearchRequest();
        RestCountAction.applyRoutingOrSliceForCountRequest(request, countRequest);
        assertNull(countRequest.routing());
        assertTrue(countRequest.isRoutingFromSlice());
        assertEquals(SliceIndexing.SLICE_ALL, countRequest.searchSlice());
    }

    public void testApplyRoutingOrSliceRejectsRoutingAndSliceTogether() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_count")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1", "routing", "r1"))
            .build();
        SearchRequest countRequest = new SearchRequest();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestCountAction.applyRoutingOrSliceForCountRequest(request, countRequest)
        );
        assertEquals("[routing] is not allowed together with [_slice]", e.getMessage());
    }

    public void testApplyRoutingOrSliceRejectsSliceWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_count")
            .withParams(Map.of(SliceIndexing.PARAM_NAME, "s1"))
            .build();
        SearchRequest countRequest = new SearchRequest();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestCountAction.applyRoutingOrSliceForCountRequest(request, countRequest)
        );
        assertEquals("request does not support [_slice]", e.getMessage());
    }
}
