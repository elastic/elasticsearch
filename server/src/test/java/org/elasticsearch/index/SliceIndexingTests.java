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
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
        assertTrue(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/tenant-a/_delete_by_query")));
        assertTrue(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/tenant-a/_update_by_query")));
        assertFalse(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/_delete_by_query")));
        assertFalse(SliceIndexing.isPathBasedSliceSearch(pathSliceRequest("/my-index/_update_by_query")));
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

    public void testPrefixTermPutsTheSlicePrefixFirst() {
        final BytesRef prefixed = SliceIndexing.prefixTerm("tenant-a", new BytesRef("value"));
        assertThat(prefixed.utf8ToString(), equalTo(SliceIndexing.termPrefix("tenant-a") + "value"));
    }

    /** The prefix is the slice hash as hex, so it is the same width for every slice and shares the key's identity. */
    public void testTermPrefixIsTheSliceHashInFixedWidthHex() {
        for (String slice : List.of("a", "tenant-a", "0123456789abcdef", randomAlphaOfLengthBetween(1, 64))) {
            final String prefix = SliceIndexing.termPrefix(slice);
            assertThat(prefix.length(), equalTo(SliceIndexing.TERM_PREFIX_LENGTH));
            assertThat(prefix, equalTo(String.format(Locale.ROOT, "%08x", SliceIndexing.sliceHash(slice))));
            // one byte a character, which is what lets the token-stream and bytes paths agree
            assertThat(SliceIndexing.termPrefixBytes(slice).length, equalTo(SliceIndexing.TERM_PREFIX_LENGTH));
            // the same hash the slice key is built from
            assertThat(SliceIndexing.sliceHashFromKey(SliceIndexing.encodeSliceKey(slice)), equalTo(SliceIndexing.sliceHash(slice)));
        }
    }

    public void testStripTermPrefixRoundTrips() {
        for (String slice : List.of("a", "tenant-a", randomAlphaOfLengthBetween(1, 32))) {
            for (String term : List.of("", "x", "value", "a|b", randomAlphaOfLengthBetween(1, 32))) {
                final BytesRef original = new BytesRef(term);
                final BytesRef roundTripped = SliceIndexing.stripTermPrefix(SliceIndexing.prefixTerm(slice, original));
                assertThat(roundTripped, equalTo(original));
            }
        }
    }

    /** A fixed width means a term containing the prefix characters is still stripped at the right place. */
    public void testStripCutsAtTheFixedWidth() {
        final BytesRef prefixed = SliceIndexing.prefixTerm("s1", new BytesRef("00000000value"));
        assertThat(SliceIndexing.stripTermPrefix(prefixed).utf8ToString(), equalTo("00000000value"));
    }

    public void testPrefixingPreservesOrderWithinASlice() {
        final List<String> terms = List.of("a", "aa", "ab", "b", "z");
        final List<BytesRef> prefixed = terms.stream().map(t -> SliceIndexing.prefixTerm("tenant-a", new BytesRef(t))).toList();
        final List<BytesRef> sorted = prefixed.stream().sorted(BytesRef::compareTo).toList();
        assertThat(sorted, equalTo(prefixed));
    }

    /** Terms group by slice prefix, so each slice occupies a contiguous run of the dictionary. */
    public void testSlicesSortAsContiguousRuns() {
        final List<String> slices = List.of("s1", "s2", "s3");
        final List<BytesRef> all = new ArrayList<>();
        for (String slice : slices) {
            for (String term : List.of("a", "b")) {
                all.add(SliceIndexing.prefixTerm(slice, new BytesRef(term)));
            }
        }
        all.sort(BytesRef::compareTo);
        // whichever order the hashes fall in, the two terms of a slice are adjacent
        for (int i = 0; i < all.size(); i += 2) {
            final BytesRef first = all.get(i);
            final BytesRef second = all.get(i + 1);
            assertThat(prefixOf(first), equalTo(prefixOf(second)));
        }
    }

    private static String prefixOf(BytesRef term) {
        return new BytesRef(term.bytes, term.offset, SliceIndexing.TERM_PREFIX_LENGTH).utf8ToString();
    }

    public void testStripLeavesATooShortTermAlone() {
        final BytesRef plain = new BytesRef("short");
        assertThat(SliceIndexing.stripTermPrefix(plain), equalTo(plain));
    }

    private static String randomSliceValue() {
        return randomAlphaOfLengthBetween(1, 40) + randomFrom("", "-" + randomAlphaOfLength(3), ":" + randomInt(999));
    }

    /** The BytesRef overloads must agree with the String ones (and so with routing) for offset slices, long values and non-ASCII. */
    public void testBytesRefOverloadsMatchStringOverloads() {
        for (int i = 0; i < 100; i++) {
            String slice = switch (randomIntBetween(0, 2)) {
                case 0 -> randomSliceValue();
                case 1 -> randomAlphaOfLengthBetween(129, 700);
                case 2 -> randomRealisticUnicodeOfLengthBetween(1, 40);
                default -> throw new AssertionError();
            };
            byte[] utf8 = slice.getBytes(StandardCharsets.UTF_8);
            int pad = randomIntBetween(0, 5);
            byte[] padded = new byte[pad + utf8.length + pad];
            System.arraycopy(utf8, 0, padded, pad, utf8.length);
            BytesRef ref = new BytesRef(padded, pad, utf8.length);
            assertThat(SliceIndexing.sliceHash(ref), equalTo(SliceIndexing.sliceHash(slice)));
            assertThat(SliceIndexing.encodeSliceKey(ref), equalTo(SliceIndexing.encodeSliceKey(slice)));
        }
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
