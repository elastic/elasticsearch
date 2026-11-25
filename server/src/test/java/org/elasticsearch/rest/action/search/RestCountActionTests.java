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

    public void testApplyRoutingOrSliceWithPathMultiSlice() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/my-index/tenant-a,tenant-b/_count")
            .withParams(Map.of("index", "my-index", SliceIndexing.FIELD_NAME, "tenant-a,tenant-b"))
            .build();
        SearchRequest countRequest = new SearchRequest();
        RestCountAction.applyRoutingOrSliceForCountRequest(request, countRequest);
        assertEquals("tenant-a,tenant-b", countRequest.routing());
        assertTrue(countRequest.isRoutingFromSlice());
        assertEquals("tenant-a,tenant-b", countRequest.searchSlice());
    }

    public void testApplyRoutingOrSliceRejectsRoutingAndSliceTogether() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/my-index/tenant-a/_count")
            .withParams(Map.of("index", "my-index", SliceIndexing.FIELD_NAME, "tenant-a", "routing", "r1"))
            .build();
        SearchRequest countRequest = new SearchRequest();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestCountAction.applyRoutingOrSliceForCountRequest(request, countRequest)
        );
        assertEquals("[routing] is not allowed together with [_slice]", e.getMessage());
    }
}
