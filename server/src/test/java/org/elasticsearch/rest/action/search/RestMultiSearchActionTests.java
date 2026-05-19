/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class RestMultiSearchActionTests extends RestActionTestCase {

    public void testParseRequestWithTopLevelSliceParam() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = request("""
            {}
            {}
            """, Map.of(SliceIndexing.PARAM_NAME, "s1"));
        MultiSearchRequest multiSearchRequest = RestMultiSearchAction.parseRequest(
            request,
            true,
            new UsageService().getSearchUsageHolder(),
            nf -> false,
            Optional.empty()
        );
        assertEquals(1, multiSearchRequest.requests().size());
        assertEquals("s1", multiSearchRequest.requests().getFirst().routing());
        assertTrue(multiSearchRequest.requests().getFirst().isRoutingFromSlice());
        assertEquals("s1", multiSearchRequest.requests().getFirst().searchSlice());
    }

    public void testParseRequestRejectsTopLevelSliceParamWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = request("""
            {}
            {}
            """, Map.of(SliceIndexing.PARAM_NAME, "s1"));
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> RestMultiSearchAction.parseRequest(
                request,
                true,
                new UsageService().getSearchUsageHolder(),
                nf -> false,
                Optional.empty()
            )
        );
        assertEquals("request does not support [_slice]", ex.getMessage());
    }

    public void testParseRequestRejectsTopLevelRoutingAndSliceTogether() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = request("""
            {}
            {}
            """, Map.of("routing", "r1", SliceIndexing.PARAM_NAME, "s1"));
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> RestMultiSearchAction.parseRequest(
                request,
                true,
                new UsageService().getSearchUsageHolder(),
                nf -> false,
                Optional.empty()
            )
        );
        assertEquals("[routing] is not allowed together with [_slice]", ex.getMessage());
    }

    private RestRequest request(String body, Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_msearch")
            .withContent(new BytesArray(body), org.elasticsearch.xcontent.XContentType.JSON)
            .withParams(params)
            .build();
    }
}
