/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

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

    private static String randomSliceValue() {
        return randomAlphaOfLengthBetween(1, 40) + randomFrom("", "-" + randomAlphaOfLength(3), ":" + randomInt(999));
    }

    public void testSliceHashIsUnsigned32Bit() {
        for (int i = 0; i < 100; i++) {
            long hash = SliceIndexing.sliceHash(randomSliceValue());
            assertThat(hash, greaterThanOrEqualTo(0L));
            assertThat(hash, lessThan(1L << 32));
        }
    }

    public void testSliceKeyRoundTrip() {
        for (int i = 0; i < 100; i++) {
            String slice = randomSliceValue();
            BytesRef key = SliceIndexing.encodeSliceKey(slice);
            assertThat(key.length, equalTo(Integer.BYTES + slice.getBytes(StandardCharsets.UTF_8).length));
            assertThat(SliceIndexing.sliceFromKey(key), equalTo(slice));
            assertThat(SliceIndexing.sliceHashFromKey(key), equalTo(SliceIndexing.sliceHash(slice)));
            assertThat(SliceIndexing.encodeSliceKey(new BytesRef(slice)), equalTo(key));
            assertThat(SliceIndexing.sliceHash(new BytesRef(slice)), equalTo(SliceIndexing.sliceHash(slice)));
        }
    }

    /** Bytewise key order must equal {@code (unsigned hash, slice)} order, so segments are laid out by hash prefix. */
    public void testSliceKeyByteOrderMatchesHashOrder() {
        Set<String> unique = new HashSet<>();
        while (unique.size() < 1000) {
            unique.add(randomSliceValue());
        }
        List<String> slices = new ArrayList<>(unique);
        List<String> byHash = new ArrayList<>(slices);
        byHash.sort(Comparator.comparingLong((String s) -> SliceIndexing.sliceHash(s)).thenComparing(s -> new BytesRef(s)));
        List<String> byKey = new ArrayList<>(slices);
        byKey.sort(Comparator.comparing((String s) -> SliceIndexing.encodeSliceKey(s)));
        assertThat(byKey, equalTo(byHash));
    }
}
