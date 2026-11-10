/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

/** Exercises typed equality, scalar enforcement, and reservation ownership independently of page allocation. */
public class SequenceKeyCodecTests extends ESTestCase {
    public void testCompositeBoundariesAndArbitraryStrings() {
        var breaker = breaker("1mb");
        try (var codec = codec(List.of(DataType.KEYWORD, DataType.KEYWORD), breaker)) {
            assertNotEquals(key(codec, "ab", "c"), key(codec, "a", "bc"));
            assertNotEquals(key(codec, "", "a|b"), key(codec, "|a", "b"));
            assertNotEquals(key(codec, "a\u0000", "b"), key(codec, "a", "\u0000b"));
            String value = randomUnicodeOfLength(50) + "|:\u0000\uD83D\uDE00";
            assertEquals(key(codec, value, ""), key(codec, value, ""));
            assertNotEquals(key(codec, value, ""), key(codec, value + "x", ""));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testIntegralPrecisionAndTypeBoundaries() {
        var breaker = breaker("1mb");
        for (DataType type : List.of(DataType.LONG, DataType.UNSIGNED_LONG, DataType.DATETIME, DataType.DATE_NANOS)) {
            try (var codec = codec(List.of(type), breaker)) {
                assertNotEquals(key(codec, 9_007_199_254_740_992L), key(codec, 9_007_199_254_740_993L));
                assertNotEquals(key(codec, Long.MIN_VALUE), key(codec, Long.MAX_VALUE));
                assertEquals(key(codec, -1L), key(codec, -1L));
            }
        }
        try (var integer = codec(List.of(DataType.INTEGER), breaker); var longKey = codec(List.of(DataType.LONG), breaker)) {
            assertNotEquals(key(integer, 1), key(longKey, 1L));
            assertNotEquals(key(integer, Integer.MIN_VALUE), key(integer, Integer.MAX_VALUE));
        }
        try (var longKey = codec(List.of(DataType.LONG), breaker); var dateKey = codec(List.of(DataType.DATETIME), breaker)) {
            assertNotEquals(key(longKey, 1L), key(dateKey, 1L));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testBooleansAndDoubleEquality() {
        var breaker = breaker("1mb");
        try (var bool = codec(List.of(DataType.BOOLEAN), breaker); var doubles = codec(List.of(DataType.DOUBLE), breaker)) {
            assertNotEquals(key(bool, true), key(bool, false));
            assertNotEquals(key(doubles, -0.0), key(doubles, 0.0));
            assertNotEquals(key(doubles, Double.NEGATIVE_INFINITY), key(doubles, Double.POSITIVE_INFINITY));
            assertEquals(key(doubles, Double.NaN), key(doubles, Double.longBitsToDouble(0x7ff8000000000001L)));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testNaturalTupleOrderingIsIndependentOfEqualityEncoding() {
        var breaker = breaker("1mb");
        try (var text = codec(List.of(DataType.KEYWORD), breaker)) {
            assertTrue(SequenceKeyCodec.compare(key(text, "aa"), key(text, "b")) < 0);
            assertTrue(SequenceKeyCodec.compare(key(text, "\uD83D\uDE00"), key(text, "\uE000")) < 0);
            for (int iteration = 0; iteration < 100; iteration++) {
                String left = randomUnicodeOfLengthBetween(0, 20);
                String right = randomUnicodeOfLengthBetween(0, 20);
                assertEquals(
                    Integer.signum(left.compareTo(right)),
                    Integer.signum(SequenceKeyCodec.compare(key(text, left), key(text, right)))
                );
            }
        }
        try (var numbers = codec(List.of(DataType.LONG), breaker)) {
            assertTrue(SequenceKeyCodec.compare(key(numbers, Long.MIN_VALUE), key(numbers, Long.MAX_VALUE)) < 0);
            assertTrue(SequenceKeyCodec.compare(key(numbers, -1L), key(numbers, 0L)) < 0);
        }
        try (var numbers = codec(List.of(DataType.UNSIGNED_LONG), breaker)) {
            assertTrue(SequenceKeyCodec.compare(key(numbers, Long.MIN_VALUE), key(numbers, 0L)) < 0);
            assertTrue(SequenceKeyCodec.compare(key(numbers, 0L), key(numbers, Long.MAX_VALUE)) < 0);
        }
        try (var numbers = codec(List.of(DataType.DOUBLE), breaker)) {
            assertTrue(SequenceKeyCodec.compare(key(numbers, -0.0), key(numbers, +0.0)) < 0);
            assertTrue(SequenceKeyCodec.compare(key(numbers, Double.NEGATIVE_INFINITY), key(numbers, Double.POSITIVE_INFINITY)) < 0);
            assertEquals(0, SequenceKeyCodec.compare(key(numbers, Double.NaN), key(numbers, Double.NaN)));
        }
        try (var tuple = codec(List.of(DataType.KEYWORD, DataType.INTEGER), breaker)) {
            BytesRef first = key(tuple, "same", -1);
            BytesRef second = key(tuple, "same", 1);
            byte[] storage = new byte[first.length + 2];
            System.arraycopy(first.bytes, first.offset, storage, 1, first.length);
            assertTrue(SequenceKeyCodec.compare(new BytesRef(storage, 1, first.length), second) < 0);
            assertEquals(0, SequenceKeyCodec.compare(first, first));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testBytesTypesAndOffsets() {
        var breaker = breaker("1mb");
        for (DataType type : List.of(DataType.KEYWORD, DataType.IP, DataType.VERSION)) {
            try (var codec = codec(List.of(type), breaker)) {
                byte[] storage = new byte[] { 9, 1, 0, 2, 9 };
                assertEquals(key(codec, new BytesRef(storage, 1, 3)), key(codec, new BytesRef(new byte[] { 1, 0, 2 })));
                assertNotEquals(key(codec, new BytesRef(new byte[] { 1, 0, 2 })), key(codec, new BytesRef(new byte[] { 1, 0, 3 })));
            }
        }
        try (var keyword = codec(List.of(DataType.KEYWORD), breaker); var version = codec(List.of(DataType.VERSION), breaker)) {
            assertNotEquals(key(keyword, new BytesRef("1.2.3")), key(version, new BytesRef("1.2.3")));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testNullExclusionAndMultiValueRequiresExpansion() {
        var breaker = breaker("1mb");
        try (
            var codec = codec(List.of(DataType.KEYWORD, DataType.LONG), breaker);
            Page nullRow = page("a", null);
            Page multi = page(List.of("a", "b"), 1L);
            Page nullAndMulti = page(List.of("a", "b"), null)
        ) {
            long baseline = breaker.getUsed();
            assertNull(codec.encode(nullRow, 0));
            for (Page input : List.of(multi, nullAndMulti)) {
                assertThat(
                    expectThrows(IllegalArgumentException.class, () -> codec.encode(input, 0)).getMessage(),
                    containsString("use MV_EXPAND inside the stage")
                );
            }
            assertEquals(baseline, breaker.getUsed());
        }
        try (var codec = codec(List.of(DataType.NULL), breaker); Page nullRow = page((Object) null)) {
            assertNull(codec.encode(nullRow, 0));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testKeylessAndOwnedCopy() {
        var breaker = breaker("1mb");
        try (var codec = codec(List.of(), breaker); Page input = page(); var key = codec.encode(input, 0)) {
            assertEquals(new BytesRef(), key.bytes());
        }
        var codec = codec(List.of(DataType.KEYWORD), breaker);
        Page input = page("source");
        var encoded = codec.encode(input, 0);
        BytesRef expected = BytesRef.deepCopyOf(encoded.bytes());
        input.releaseBlocks();
        codec.close();
        assertEquals(expected, encoded.bytes());
        assertTrue(breaker.getUsed() > 0);
        encoded.close();
        encoded.close();
        codec.close();
        expectThrows(IllegalStateException.class, encoded::bytes);
        expectThrows(IllegalStateException.class, () -> codec.encode(input, 0));
        assertEquals(0, breaker.getUsed());
    }

    public void testBreakerFailureDoesNotLeakOrPoisonCodec() {
        var breaker = breaker("1kb");
        try (var codec = codec(List.of(DataType.KEYWORD), breaker); Page input = page("x".repeat(4096))) {
            long baseline = breaker.getUsed();
            expectThrows(CircuitBreakingException.class, () -> codec.encode(input, 0));
            assertEquals(baseline, breaker.getUsed());
            assertEquals(key(codec, "a"), key(codec, "a"));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testConfigurationAndRowValidation() {
        var breaker = breaker("1mb");
        expectThrows(IllegalArgumentException.class, () -> new SequenceKeyCodec(List.of(DataType.KEYWORD), new int[0], breaker));
        expectThrows(IllegalArgumentException.class, () -> new SequenceKeyCodec(List.of(DataType.TEXT), new int[] { 0 }, breaker));
        expectThrows(IllegalArgumentException.class, () -> new SequenceKeyCodec(List.of(DataType.LONG), new int[] { -1 }, breaker));
        try (var codec = codec(List.of(DataType.LONG), breaker); Page input = page(1L)) {
            expectThrows(IllegalArgumentException.class, () -> codec.encode(input, -1));
            expectThrows(IllegalArgumentException.class, () -> codec.encode(input, 1));
        }
        assertEquals(0, breaker.getUsed());
    }

    private static SequenceKeyCodec codec(List<DataType> types, CircuitBreaker breaker) {
        return new SequenceKeyCodec(types, IntStream.range(0, types.size()).toArray(), breaker);
    }

    private static BytesRef key(SequenceKeyCodec codec, Object... values) {
        try (Page input = page(values); var key = codec.encode(input, 0)) {
            return BytesRef.deepCopyOf(key.bytes());
        }
    }

    private static Page page(Object... values) {
        // Input-page allocation is isolated from the real request breaker used to verify key admission and release.
        return new Page(1, BlockUtils.fromArrayRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    private static CircuitBreaker breaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }
}
