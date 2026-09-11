/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
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
        assertThat(ex.getMessage(), containsString("invalid [slice] value"));
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
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("slice", "s1")).build();
        SliceIndexing.ParsedRouting parsed = SliceIndexing.parseRoutingOrSliceWithProvenance(request);
        assertThat(parsed.routing(), equalTo("s1"));
        assertThat(parsed.fromSlice(), equalTo(true));
    }

    public void testParseRoutingOrSliceRejectsWhenBothPresent() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("routing", "r1", "slice", "s1")).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("[routing] is not allowed together with [slice]"));
    }

    public void testParseRoutingOrSliceRejectsSliceWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("slice", "s1")).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SliceIndexing.parseRoutingOrSliceWithProvenance(request)
        );
        assertThat(ex.getMessage(), containsString("request does not support [slice]"));
    }

    public void testParsedRoutingToSearchSlice() {
        assertNull(new SliceIndexing.ParsedRouting("r1", false).toSearchSlice());
        assertThat(new SliceIndexing.ParsedRouting("s1", true).toSearchSlice(), equalTo("s1"));
        assertThat(new SliceIndexing.ParsedRouting(null, true).toSearchSlice(), equalTo(SliceIndexing.SLICE_ALL));
    }

    public void testApplySearchRoutingOrSliceUsesMutuallyExclusiveSetters() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        SearchRequest searchRequest = new SearchRequest();
        SliceIndexing.applySearchRoutingOrSlice(new SliceIndexing.ParsedRouting("s1", true), searchRequest);
        assertThat(searchRequest.searchSlice(), equalTo("s1"));
        assertThat(searchRequest.routing(), equalTo("s1"));
        assertTrue(searchRequest.isRoutingFromSlice());

        searchRequest = new SearchRequest();
        SliceIndexing.applySearchRoutingOrSlice(new SliceIndexing.ParsedRouting("r1", false), searchRequest);
        assertNull(searchRequest.searchSlice());
        assertThat(searchRequest.routing(), equalTo("r1"));
        assertFalse(searchRequest.isRoutingFromSlice());

        OpenPointInTimeRequest pitRequest = new OpenPointInTimeRequest("idx");
        SliceIndexing.applySearchRoutingOrSlice(new SliceIndexing.ParsedRouting(null, true), pitRequest);
        assertThat(pitRequest.searchSlice(), equalTo(SliceIndexing.SLICE_ALL));
        assertNull(pitRequest.routing());
        assertTrue(pitRequest.isRoutingFromSlice());
    }

    private static void assertInvalid(String value) {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> SliceIndexing.validateUserSliceValue(value));
        assertThat(ex.getMessage(), containsString("invalid [slice] value"));
    }
}
