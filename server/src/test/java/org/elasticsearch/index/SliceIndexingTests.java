/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SliceIndexingTests extends ESTestCase {

    public void testValidateUserSliceValueAcceptsSafeValues() {
        SliceIndexing.validateUserSliceValue("s1");
        SliceIndexing.validateUserSliceValue("tenant-1");
        SliceIndexing.validateUserSliceValue("tenant.group:01");
        SliceIndexing.validateUserSliceValue("SOME_SLICE");
    }

    public void testValidateUserSliceValueRejectsReservedValue() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.validateUserSliceValue(SliceIndexing.SLICE_ALL)
        );
        assertThat(ex.getMessage(), containsString("invalid [_slice] value"));
        assertThat(ex.getMessage(), containsString("reserved"));
    }

    public void testValidateUserSliceValueRejectsIllegalCharacters() {
        assertInvalid("slice,1");
        assertInvalid("slice*1");
        assertInvalid("slice?1");
        assertInvalid("slice 1");
        assertInvalid(".slice1");
        assertInvalid("-slice1");
        assertInvalid("_slice1");
        assertInvalid("slice1.");
        assertInvalid("slice1-");
        assertInvalid("slice1_");
    }

    public void testValidateUserSliceValueRejectsEmptyAndTooLong() {
        assertInvalid("");
        assertInvalid("a".repeat(129));
    }

    public void testParseRoutingOrSliceReturnsRoutingWhenSliceAbsent() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("routing", "r1")).build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("r1"));
        assertThat(parsed.fromSlice(), equalTo(false));
    }

    public void testParseRoutingOrSliceReturnsSliceWhenPresent() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("_slice", "s1")).build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("s1"));
        assertThat(parsed.fromSlice(), equalTo(true));
    }

    public void testParseRoutingOrSliceRejectsWhenBothPresent() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("routing", "r1", "_slice", "s1")).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("[routing] is not allowed together with [_slice]"));
    }

    public void testParseRoutingOrSliceRejectsSliceWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("_slice", "s1")).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("request does not support [_slice]"));
    }

    public void testParseRoutingOrSliceFromPathSlice() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/tenant-a/_doc/1")
            .withParams(Map.of("index", "my-index", "id", "1", "_slice", "tenant-a"))
            .build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("tenant-a"));
        assertThat(parsed.fromSlice(), equalTo(true));
    }

    public void testParseRoutingOrSliceRejectsSliceQueryParam() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/_doc/1?_slice=tenant-a")
            .withParams(Map.of("index", "my-index", "id", "1"))
            .build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("query parameter is not supported"));
    }

    public void testToSearchSlice() {
        assertNull(SliceIndexing.toSearchSlice("r1", false));
        assertNull(SliceIndexing.toSearchSlice(null, false));
        assertThat(SliceIndexing.toSearchSlice("s1", true), equalTo("s1"));
        assertThat(SliceIndexing.toSearchSlice(null, true), equalTo(SliceIndexing.SLICE_ALL));
    }

    public void testSliceToRouting() {
        assertThat(SliceIndexing.sliceToRouting("s1"), equalTo("s1"));
        assertThat(SliceIndexing.sliceToRouting("s1,s2"), equalTo("s1,s2"));
        assertNull(SliceIndexing.sliceToRouting(SliceIndexing.SLICE_ALL));
    }

    public void testParseSearchRoutingFromPathSlice() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/tenant-a/_search")
            .withParams(Map.of("index", "my-index", "_slice", "tenant-a"))
            .build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseSearchRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("tenant-a"));
        assertThat(parsed.fromSlice(), equalTo(true));
    }

    public void testParseSearchRoutingFromMultiSlicePath() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/tenant-a,tenant-b/_search")
            .withParams(Map.of("index", "my-index", "_slice", "tenant-a,tenant-b"))
            .build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseSearchRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("tenant-a,tenant-b"));
        assertThat(parsed.fromSlice(), equalTo(true));
    }

    public void testParseSearchRoutingFromPathSliceAll() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/_all/_search")
            .withParams(Map.of("index", "my-index", "_slice", SliceIndexing.SLICE_ALL))
            .build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseSearchRoutingOrSliceWithProvenance(request);
        assertNull(parsed.routing());
        assertThat(parsed.fromSlice(), equalTo(true));
    }

    public void testParseSearchRoutingRejectsSliceQueryParam() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/_search?_slice=tenant-a")
            .withParams(Map.of("index", "my-index"))
            .build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseSearchRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("query parameter is not supported for search"));
    }

    public void testParseSearchRoutingRejectsPathSliceWithQuerySlice() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/tenant-a/_search?_slice=tenant-b")
            .withParams(Map.of("index", "my-index", "_slice", "tenant-a"))
            .build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseSearchRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("query parameter is not supported for search"));
    }

    public void testParseSearchRoutingFromPathSliceWithMultiIndex() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/index-a,index-b/tenant-a/_search")
            .withParams(Map.of("index", "index-a,index-b", "_slice", "tenant-a"))
            .build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseSearchRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("tenant-a"));
        assertThat(parsed.fromSlice(), equalTo(true));
    }

    public void testParseSearchRoutingRejectsPathSliceWithRouting() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/tenant-a/_search")
            .withParams(Map.of("index", "my-index", "_slice", "tenant-a", "routing", "r1"))
            .build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseSearchRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("[routing] is not allowed together with [_slice]"));
    }

    public void testIsPathBasedSliceSearch() {
        assertTrue(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/tenant-a/_search")));
        assertTrue(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/tenant-a/_count")));
        assertTrue(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/tenant-a/_search/template")));
        assertTrue(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/tenant-a/_fleet/_fleet_search")));
        assertFalse(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/_search")));
        assertFalse(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/_search")));
        assertFalse(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/_search/template")));
        assertFalse(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/_fleet/_fleet_search")));
    }

    private RestRequest pathSliceRequest(String path) {
        return new FakeRestRequest.Builder(xContentRegistry()).withPath(path).build();
    }

    private static void assertInvalid(String value) {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> SliceIndexing.validateUserSliceValue(value));
        assertThat(ex.getMessage(), containsString("invalid [_slice] value"));
    }
}
