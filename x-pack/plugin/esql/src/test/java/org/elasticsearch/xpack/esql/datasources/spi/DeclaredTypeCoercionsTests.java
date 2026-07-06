/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Pins the declared-type coercion contract: the castability predicate
 * ({@link DeclaredTypeCoercions#supports} — the field mappers' bulk-ingest coercion set), the
 * fused-decode subset ({@link DeclaredTypeCoercions#fusedInDecode}), the block-level coercion
 * engine ({@link DeclaredTypeCoercions#castBlock} — value semantics, per-cell bulk leniency,
 * multi-value handling), and the shared string&rarr;datetime scalar
 * ({@link DeclaredTypeCoercions#parseDatetimeMillis}). This class is the single source of truth
 * every external reader and the resolver consult, so any drift — a pair the resolver admits that
 * the engine cannot coerce, or vice versa — must fail here rather than surface as a silent-null
 * or a deep engine error.
 */
public class DeclaredTypeCoercionsTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    /**
     * The full (physical, declared) matrix over the types the file mappers produce plus the
     * declarable set, checked against an independently-written expectation of the mapper-ingest
     * coercion rules (so a change to either side is caught).
     */
    public void testSupportsPinnedToMapperCoercionSet() {
        Set<DataType> types = Set.of(
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.INTEGER,
            DataType.LONG,
            DataType.DOUBLE,
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.DATE_NANOS,
            DataType.IP,
            DataType.UNSIGNED_LONG,
            DataType.UNSUPPORTED,
            DataType.NULL
        );
        for (DataType from : types) {
            for (DataType to : types) {
                assertThat(
                    "supports(" + from.typeName() + ", " + to.typeName() + ")",
                    DeclaredTypeCoercions.supports(from, to),
                    equalTo(expectedSupported(from, to))
                );
            }
        }
    }

    /** The expected mapper-ingest coercion set, written out longhand so it is not a copy of the implementation. */
    private static boolean expectedSupported(DataType from, DataType to) {
        if (from == to) {
            return true; // identity is always a no-op coercion
        }
        if (from == DataType.UNSUPPORTED || from == DataType.NULL || to == DataType.UNSUPPORTED || to == DataType.NULL) {
            return false; // nothing to decode, hence nothing to coerce
        }
        boolean fromString = from == DataType.KEYWORD || from == DataType.TEXT;
        // decoded temporals are epoch longs, so they behave like whole numbers toward numeric targets
        boolean fromWhole = from == DataType.INTEGER
            || from == DataType.LONG
            || from == DataType.UNSIGNED_LONG
            || from == DataType.DATETIME
            || from == DataType.DATE_NANOS;
        return switch (to) {
            case KEYWORD, TEXT -> true; // ingest stringifies any scalar
            case LONG, INTEGER, DOUBLE, UNSIGNED_LONG -> fromString || fromWhole || from == DataType.DOUBLE;
            case BOOLEAN -> fromString; // number->boolean does not ingest
            case DATETIME -> fromString || from == DataType.INTEGER || from == DataType.LONG || from == DataType.UNSIGNED_LONG;
            // string parse, or the millis->nanos widen a date_nanos field runs on an epoch-millis
            // token at ingest (also cross-file DATETIME + DATE_NANOS unification); a raw long stays
            // out — ambiguous between millis and nanos
            case DATE_NANOS -> fromString || from == DataType.DATETIME;
            case IP -> fromString;
            default -> false;
        };
    }

    /**
     * Drives the ACTUAL mapper coercion primitives — {@link NumberFieldMapper.NumberType#parse}
     * with {@code coerce=true}, {@link Booleans#parseBoolean(String)} (the boolean mapper's strict
     * token rule), {@link InetAddresses#forString} (the ip mapper's parse) — over a representative
     * well-formed value per source type and asserts {@link DeclaredTypeCoercions#supports} agrees
     * with what the mapper accepts or rejects, so the hand-encoded pair set cannot drift from the
     * real mapper rules. Scoped to the numeric / boolean / ip targets whose mapper primitive is a
     * pure value function; the temporal targets are format-driven (declared format, epoch-millis
     * reinterpret) and their semantics are pinned by the parse/cast tests in this class instead —
     * a raw mapper probe there would accept {@code epoch_millis} longs into {@code date_nanos},
     * exactly the millis-vs-nanos ambiguity {@code supports} deliberately excludes.
     */
    public void testSupportsAgreesWithActualMapperAcceptance() {
        List<DataType> sources = List.of(
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.INTEGER,
            DataType.LONG,
            DataType.UNSIGNED_LONG,
            DataType.DOUBLE,
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.DATE_NANOS
        );
        List<DataType> targets = List.of(DataType.LONG, DataType.INTEGER, DataType.DOUBLE, DataType.BOOLEAN, DataType.IP);
        for (DataType from : sources) {
            for (DataType to : targets) {
                if (from == to) {
                    continue; // identity is not a coercion; nothing to probe
                }
                Object value = representativeValue(from, to);
                assertThat(
                    "supports(" + from.typeName() + ", " + to.typeName() + ") must agree with the mapper on [" + value + "]",
                    DeclaredTypeCoercions.supports(from, to),
                    equalTo(mapperAccepts(to, value))
                );
            }
        }
    }

    /**
     * A representative decoded value of {@code from} in the Java shape the block value-readers
     * produce, well-formed for {@code to}'s domain where the source can express one (a string
     * source picks the token by target — {@code "true"} for boolean, an address for ip — since
     * string coercibility is per-token by nature).
     */
    private static Object representativeValue(DataType from, DataType to) {
        if (from == DataType.KEYWORD || from == DataType.TEXT) {
            return switch (to) {
                case BOOLEAN -> "true";
                case IP -> "10.20.30.40";
                default -> "42";
            };
        }
        return switch (from) {
            case INTEGER -> 42;
            case LONG -> 42L;
            case UNSIGNED_LONG -> BigInteger.valueOf(42);
            case DOUBLE -> 42.5d;
            case BOOLEAN -> Boolean.TRUE;
            // decoded temporals are epoch longs; keep the representative small so pair-level
            // support isn't conflated with a per-value range failure on narrow targets
            case DATETIME, DATE_NANOS -> 42L;
            default -> throw new AssertionError("no representative for " + from);
        };
    }

    /** Whether the target type's actual mapper primitive ingests {@code value}. */
    private static boolean mapperAccepts(DataType to, Object value) {
        try {
            switch (to) {
                case LONG -> NumberFieldMapper.NumberType.LONG.parse(value, true);
                case INTEGER -> NumberFieldMapper.NumberType.INTEGER.parse(value, true);
                case DOUBLE -> NumberFieldMapper.NumberType.DOUBLE.parse(value, true);
                // the boolean/ip mappers ingest the token's text; both delegate to these primitives
                case BOOLEAN -> Booleans.parseBoolean(value.toString());
                case IP -> InetAddresses.forString(value.toString());
                default -> throw new AssertionError("no mapper primitive probed for " + to);
            }
            return true;
        } catch (IllegalArgumentException e) { // includes NumberFormatException
            return false;
        }
    }

    /** The pairs this change opened up, called out explicitly so the contract reads as a test. */
    public void testWidenedPairsSupported() {
        assertTrue("long->double coerces like bulk ingest", DeclaredTypeCoercions.supports(DataType.LONG, DataType.DOUBLE));
        assertTrue("integer->double coerces like bulk ingest", DeclaredTypeCoercions.supports(DataType.INTEGER, DataType.DOUBLE));
        assertTrue("string->long parses", DeclaredTypeCoercions.supports(DataType.KEYWORD, DataType.LONG));
        assertTrue("string->double parses", DeclaredTypeCoercions.supports(DataType.KEYWORD, DataType.DOUBLE));
        assertTrue("string->integer parses", DeclaredTypeCoercions.supports(DataType.KEYWORD, DataType.INTEGER));
        assertTrue("string->boolean parses", DeclaredTypeCoercions.supports(DataType.KEYWORD, DataType.BOOLEAN));
        assertTrue("string->ip parses", DeclaredTypeCoercions.supports(DataType.KEYWORD, DataType.IP));
        assertTrue("long->integer narrows with range check", DeclaredTypeCoercions.supports(DataType.LONG, DataType.INTEGER));
        assertTrue("datetime->long reads epoch millis", DeclaredTypeCoercions.supports(DataType.DATETIME, DataType.LONG));
    }

    /**
     * String targets are closed over the decodable source set: a type the readers have no value
     * reader for (version, geo_point, ...) must be rejected by {@code supports} rather than
     * admitted and then hard-thrown inside {@code castBlock}'s value reader.
     */
    public void testStringTargetsClosedOverDecodableSources() {
        for (DataType to : List.of(DataType.KEYWORD, DataType.TEXT)) {
            assertFalse("version blocks have no value reader", DeclaredTypeCoercions.supports(DataType.VERSION, to));
            assertFalse("geo_point blocks have no value reader", DeclaredTypeCoercions.supports(DataType.GEO_POINT, to));
        }
    }

    /** Pairs even ingest cannot coerce — the resolver's reject path stays reachable through these. */
    public void testInconvertiblePairsRejected() {
        assertFalse("timestamp->ip has no ingest coercion", DeclaredTypeCoercions.supports(DataType.DATETIME, DataType.IP));
        assertFalse("long->ip has no ingest coercion", DeclaredTypeCoercions.supports(DataType.LONG, DataType.IP));
        assertFalse("number->boolean does not ingest", DeclaredTypeCoercions.supports(DataType.LONG, DataType.BOOLEAN));
        assertFalse("double->datetime has no epoch encoding", DeclaredTypeCoercions.supports(DataType.DOUBLE, DataType.DATETIME));
        assertFalse("unsupported physical columns cannot decode", DeclaredTypeCoercions.supports(DataType.UNSUPPORTED, DataType.KEYWORD));
    }

    /** The fused pairs are exactly the historical natively-decoded set, and every fused pair is supported. */
    public void testFusedPairsAreSupportedSubset() {
        Set<DataType> types = Set.of(
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.INTEGER,
            DataType.LONG,
            DataType.DOUBLE,
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.IP,
            DataType.UNSIGNED_LONG
        );
        for (DataType from : types) {
            for (DataType to : types) {
                if (DeclaredTypeCoercions.fusedInDecode(from, to)) {
                    assertTrue(
                        "fused pair must be supported: " + from.typeName() + "->" + to.typeName(),
                        DeclaredTypeCoercions.supports(from, to)
                    );
                }
            }
        }
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.INTEGER, DataType.LONG));
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.LONG, DataType.DATETIME));
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.KEYWORD, DataType.DATETIME));
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.KEYWORD, DataType.TEXT));
        assertFalse("long->double runs through castBlock", DeclaredTypeCoercions.fusedInDecode(DataType.LONG, DataType.DOUBLE));
        assertFalse("string->long runs through castBlock", DeclaredTypeCoercions.fusedInDecode(DataType.KEYWORD, DataType.LONG));
    }

    // ---- castBlock value semantics ----

    public void testCastLongToDouble() {
        try (Block source = blockFactory.newLongArrayVector(new long[] { 1L, -42L, 9007199254740993L }, 3).asBlock()) {
            try (Block cast = castStrict(source, DataType.LONG, DataType.DOUBLE)) {
                DoubleBlock doubles = (DoubleBlock) cast;
                assertThat(doubles.getDouble(0), equalTo(1.0));
                assertThat(doubles.getDouble(1), equalTo(-42.0));
                // bulk ingest of a long beyond 2^53 into a double loses precision the same way
                assertThat(doubles.getDouble(2), equalTo(9007199254740992.0));
            }
        }
    }

    public void testCastStringToLongDoubleBooleanIp() {
        try (Block src = bytesBlock("42"); Block longs = castStrict(src, DataType.KEYWORD, DataType.LONG)) {
            assertThat(((LongBlock) longs).getLong(0), equalTo(42L));
        }
        try (Block src = bytesBlock("1e2"); Block doubles = castStrict(src, DataType.KEYWORD, DataType.DOUBLE)) {
            assertThat(((DoubleBlock) doubles).getDouble(0), equalTo(100.0));
        }
        try (Block src = bytesBlock("true"); Block booleans = castStrict(src, DataType.KEYWORD, DataType.BOOLEAN)) {
            assertTrue(((BooleanBlock) booleans).getBoolean(0));
        }
        try (Block src = bytesBlock("10.20.30.40"); Block ips = castStrict(src, DataType.KEYWORD, DataType.IP)) {
            BytesRef ip = ((BytesRefBlock) ips).getBytesRef(0, new BytesRef());
            assertThat(ip, equalTo(StringUtils.parseIP("10.20.30.40")));
        }
    }

    /**
     * The convergence contract (audit F-NUM / F-CAST-INT): a declared numeric read is value-identical
     * to the ES|QL {@code ::} cast, which <b>rounds</b> string&rarr;whole-number (the former
     * {@code NumberType.parse} path truncated). Fractional, scientific, and signed tokens are accepted.
     * {@code unsigned_long} truncates — the documented per-target split ({@code ::long} rounds,
     * {@code ::unsigned_long} truncates).
     */
    public void testDeclaredNumericReadMatchesCastEngineRounding() {
        assertLongCast("1.9", 2L);   // == "1.9"::long (was 1 under truncate)
        assertLongCast("-1.9", -2L);
        assertLongCast("2.5", 3L);
        assertLongCast("1e3", 1000L);
        assertLongCast("+5", 5L);
        assertLongCast("42", 42L);
        assertIntCast("1.9", 2);
        assertIntCast("-2.6", -3);
        try (Block src = bytesBlock("1.9"); Block d = castStrict(src, DataType.KEYWORD, DataType.DOUBLE)) {
            assertThat(((DoubleBlock) d).getDouble(0), equalTo(1.9));
        }
        // unsigned_long truncates where long rounds (F-CAST-INT)
        try (Block src = bytesBlock("2.5"); Block ul = castStrict(src, DataType.KEYWORD, DataType.UNSIGNED_LONG)) {
            assertThat(NumericUtils.unsignedLongAsNumber(((LongBlock) ul).getLong(0)).longValue(), equalTo(2L));
        }
    }

    /**
     * The declared {@code boolean} read is STRICT and case-insensitive ({@code true}/{@code false} in
     * any case; every other token fails). This deliberately diverges from {@code ::boolean}, which maps
     * a non-{@code true} token silently to {@code false} — the read rejects the token loudly instead
     * (lenient mode nulls the cell + warns; a silent {@code false} is the wrong-answer class we avoid).
     */
    public void testDeclaredBooleanReadIsStrictCaseInsensitive() {
        for (String t : List.of("true", "TRUE", "True", "tRuE")) {
            try (Block src = bytesBlock(t); Block b = castStrict(src, DataType.KEYWORD, DataType.BOOLEAN)) {
                assertTrue("[" + t + "] -> true", ((BooleanBlock) b).getBoolean(0));
            }
        }
        for (String f : List.of("false", "FALSE", "False")) {
            try (Block src = bytesBlock(f); Block b = castStrict(src, DataType.KEYWORD, DataType.BOOLEAN)) {
                assertFalse("[" + f + "] -> false", ((BooleanBlock) b).getBoolean(0));
            }
        }
        for (String bad : List.of("yes", "1", "abc", "t", "")) {
            List<String> warnings = new ArrayList<>();
            try (
                Block src = bytesBlock(bad);
                Block b = DeclaredTypeCoercions.castBlock(
                    src,
                    DataType.KEYWORD,
                    DataType.BOOLEAN,
                    null,
                    blockFactory,
                    "col",
                    capturing(warnings)
                )
            ) {
                assertTrue("[" + bad + "] is not a boolean -> null (never silent false)", b.isNull(0));
                assertThat(warnings, hasSize(1));
            }
        }
    }

    /**
     * A declared {@code double} read returns the IEEE value the token names — {@code NaN} /
     * {@code Infinity} / {@code -Infinity} pass through, matching the native columnar double read and
     * CSV. The string-coercion arm must NOT apply the double mapper's finite-only rule (that is an
     * index-time constraint); an external read preserves the file's value. Garbage still fails.
     */
    public void testCastStringToDoublePreservesNonFiniteValues() {
        try (Block src = bytesBlock("NaN"); Block d = castStrict(src, DataType.KEYWORD, DataType.DOUBLE)) {
            assertTrue("NaN token -> NaN", Double.isNaN(((DoubleBlock) d).getDouble(0)));
        }
        try (Block src = bytesBlock("Infinity"); Block d = castStrict(src, DataType.KEYWORD, DataType.DOUBLE)) {
            assertEquals(Double.POSITIVE_INFINITY, ((DoubleBlock) d).getDouble(0), 0.0);
        }
        try (Block src = bytesBlock("-Infinity"); Block d = castStrict(src, DataType.KEYWORD, DataType.DOUBLE)) {
            assertEquals(Double.NEGATIVE_INFINITY, ((DoubleBlock) d).getDouble(0), 0.0);
        }
        // A genuinely unparseable token still fails (fail_fast throws).
        try (Block src = bytesBlock("notanumber")) {
            expectThrows(
                IllegalArgumentException.class,
                () -> DeclaredTypeCoercions.castBlock(src, DataType.KEYWORD, DataType.DOUBLE, null, blockFactory, null, null).close()
            );
        }
    }

    public void testCastTemporalToWholeNumber() {
        // A temporal source arrives as a raw epoch Long; supports(DATETIME/DATE_NANOS, long/integer) is true,
        // so castBlock MUST coerce it — identity to long, range-checked narrow to int — with no structural
        // failure. Regression for the numericCoercer ClassCastException (DATETIME converter expected a
        // ZonedDateTime) and the missing-DATE_NANOS-converter IAE.
        long epochMillis = 1704067200000L; // 2024-01-01, well above Integer.MAX_VALUE
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { epochMillis }, 1).asBlock();
            Block cast = castStrict(src, DataType.DATETIME, DataType.LONG)
        ) {
            assertEquals(epochMillis, ((LongBlock) cast).getLong(0));
        }
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { epochMillis }, 1).asBlock();
            Block cast = castStrict(src, DataType.DATE_NANOS, DataType.LONG)
        ) {
            assertEquals(epochMillis, ((LongBlock) cast).getLong(0));
        }
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { 42L }, 1).asBlock();
            Block cast = castStrict(src, DataType.DATETIME, DataType.INTEGER)
        ) {
            assertEquals(42, ((org.elasticsearch.compute.data.IntBlock) cast).getInt(0));
        }
        // A real epoch overflows int: a per-value coercion failure (warn+null), NOT a structural throw.
        List<String> warnings = new ArrayList<>();
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { epochMillis }, 1).asBlock();
            Block cast = DeclaredTypeCoercions.castBlock(
                src,
                DataType.DATETIME,
                DataType.INTEGER,
                null,
                blockFactory,
                "col",
                capturing(warnings)
            )
        ) {
            assertTrue("epoch overflows int -> null", cast.isNull(0));
            assertThat(warnings, hasSize(1));
        }
    }

    private void assertLongCast(String token, long expected) {
        try (Block src = bytesBlock(token); Block longs = castStrict(src, DataType.KEYWORD, DataType.LONG)) {
            assertThat("[" + token + "]::long", ((LongBlock) longs).getLong(0), equalTo(expected));
        }
    }

    private void assertIntCast(String token, int expected) {
        try (Block src = bytesBlock(token); Block ints = castStrict(src, DataType.KEYWORD, DataType.INTEGER)) {
            assertThat("[" + token + "]::integer", ((org.elasticsearch.compute.data.IntBlock) ints).getInt(0), equalTo(expected));
        }
    }

    public void testCastStringToUnsignedLongSignFlipEncodes() {
        try (Block source = bytesBlock("18446744073709551615")) { // 2^64 - 1
            try (Block cast = castStrict(source, DataType.KEYWORD, DataType.UNSIGNED_LONG)) {
                long encoded = ((LongBlock) cast).getLong(0);
                assertThat(NumericUtils.unsignedLongAsNumber(encoded).toString(), equalTo("18446744073709551615"));
            }
        }
    }

    public void testCastNumberToKeywordStringifiesToken() {
        try (Block source = blockFactory.newLongArrayVector(new long[] { 42L }, 1).asBlock()) {
            try (Block cast = castStrict(source, DataType.LONG, DataType.KEYWORD)) {
                assertThat(((BytesRefBlock) cast).getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("42"));
            }
        }
    }

    public void testCastIpToKeywordRendersAddressText() {
        // An ip block holds the mapper's 16-byte encoded form; the only coercion out of ip is
        // stringification, which must carry the address text — never the raw encoding bytes.
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1)) {
            builder.appendBytesRef(StringUtils.parseIP("10.20.30.40"));
            try (Block source = builder.build()) {
                try (Block cast = castStrict(source, DataType.IP, DataType.KEYWORD)) {
                    assertThat(((BytesRefBlock) cast).getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("10.20.30.40"));
                }
            }
        }
    }

    public void testCastDatetimeToKeywordIsIso() {
        try (Block source = blockFactory.newLongArrayVector(new long[] { 971211336000L }, 1).asBlock()) {
            try (Block cast = castStrict(source, DataType.DATETIME, DataType.KEYWORD)) {
                assertThat(((BytesRefBlock) cast).getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("2000-10-10T20:55:36.000Z"));
            }
        }
    }

    public void testCastDatetimeToDateNanosWidensAndRangeChecks() {
        // The millis->nanos widen (also what cross-file DATETIME + DATE_NANOS unification runs):
        // in-range instants multiply by 1e6; a pre-epoch or post-2262 instant is unrepresentable
        // in date_nanos and nulls the cell with a warning — the TO_DATE_NANOS range rule
        // (DateUtils.toNanoSeconds), never a silent long overflow.
        List<String> warnings = new ArrayList<>();
        SkipWarnings sink = capturing(warnings);
        long outOfRange = Long.MAX_VALUE / 1_000L; // ~year 292M — far past the 2262 nanos horizon
        try (Block source = blockFactory.newLongArrayVector(new long[] { 971211336000L, -1L, outOfRange }, 3).asBlock()) {
            try (
                Block cast = DeclaredTypeCoercions.castBlock(source, DataType.DATETIME, DataType.DATE_NANOS, null, blockFactory, "ts", sink)
            ) {
                LongBlock nanos = (LongBlock) cast;
                assertThat(nanos.getLong(nanos.getFirstValueIndex(0)), equalTo(971211336000L * 1_000_000L));
                assertTrue("pre-epoch instant is unrepresentable in date_nanos", nanos.isNull(1));
                assertTrue("post-2262 instant must not silently overflow", nanos.isNull(2));
            }
        }
        assertThat(warnings, hasSize(2));
        assertThat(warnings.get(0), containsString("ts"));
        assertThat(warnings.get(0), containsString("declared type [date_nanos]"));
    }

    public void testCastStringToDatetimeHonorsDeclaredFormat() {
        DateFormatter fmt = DateFormatter.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
        try (Block source = bytesBlock("10/Oct/2000:13:55:36 -0700")) {
            try (Block cast = DeclaredTypeCoercions.castBlock(source, DataType.KEYWORD, DataType.DATETIME, fmt, blockFactory, null, null)) {
                assertThat(((LongBlock) cast).getLong(0), equalTo(971211336000L));
            }
        }
    }

    // ---- per-cell bulk leniency ----

    public void testLenientCoercionNullsCellAndWarns() {
        List<String> warnings = new ArrayList<>();
        SkipWarnings sink = capturing(warnings);
        try (Block source = bytesBlock("41", "not-a-number", "43")) {
            try (Block cast = DeclaredTypeCoercions.castBlock(source, DataType.KEYWORD, DataType.LONG, null, blockFactory, "col", sink)) {
                LongBlock longs = (LongBlock) cast;
                assertThat(longs.getLong(longs.getFirstValueIndex(0)), equalTo(41L));
                assertTrue("the bad cell is null, not a wrong value and not a failure", longs.isNull(1));
                assertThat(longs.getLong(longs.getFirstValueIndex(2)), equalTo(43L));
            }
        }
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0), containsString("col"));
        assertThat(warnings.get(0), containsString("declared type [long]"));
        assertThat(warnings.get(0), containsString("returning null"));
    }

    public void testLenientOverflowNullsCellAndWarns() {
        List<String> warnings = new ArrayList<>();
        SkipWarnings sink = capturing(warnings);
        try (Block source = blockFactory.newLongArrayVector(new long[] { 7L, Long.MAX_VALUE }, 2).asBlock()) {
            try (Block cast = DeclaredTypeCoercions.castBlock(source, DataType.LONG, DataType.INTEGER, null, blockFactory, "col", sink)) {
                assertThat(((org.elasticsearch.compute.data.IntBlock) cast).getInt(0), equalTo(7));
                assertTrue("out-of-range narrows to null, never truncates silently", cast.isNull(1));
            }
        }
        assertThat(warnings, hasSize(1));
        // The overflow now flows through the :: cast engine's range check (safeToInt), whose
        // message names the target type — declared read == ::integer.
        assertThat(warnings.get(0), containsString("out of [integer] range"));
    }

    public void testStrictCoercionThrows() {
        try (Block source = bytesBlock("not-a-number")) {
            // Reusing the :: cast engine, an unparseable token throws InvalidArgumentException
            // (a QlClientException -> HTTP 400), not a raw IllegalArgumentException (500).
            expectThrows(
                InvalidArgumentException.class,
                () -> DeclaredTypeCoercions.castBlock(source, DataType.KEYWORD, DataType.LONG, null, blockFactory, null, null).close()
            );
        }
    }

    public void testMultiValuePositionNullsWholePositionOnFailure() {
        List<String> warnings = new ArrayList<>();
        SkipWarnings sink = capturing(warnings);
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("1"));
            builder.appendBytesRef(new BytesRef("oops"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("2"));
            builder.appendBytesRef(new BytesRef("3"));
            builder.endPositionEntry();
            try (Block source = builder.build()) {
                try (
                    Block cast = DeclaredTypeCoercions.castBlock(source, DataType.KEYWORD, DataType.LONG, null, blockFactory, "col", sink)
                ) {
                    assertTrue("bulk semantics null the whole field, not one element", cast.isNull(0));
                    LongBlock longs = (LongBlock) cast;
                    assertThat(longs.getValueCount(1), equalTo(2));
                    int first = longs.getFirstValueIndex(1);
                    assertThat(longs.getLong(first), equalTo(2L));
                    assertThat(longs.getLong(first + 1), equalTo(3L));
                }
            }
        }
        assertThat(warnings, hasSize(1));
    }

    public void testNullsAndIdentityPreserved() {
        try (Block source = blockFactory.newConstantNullBlock(3)) {
            try (Block cast = castStrict(source, DataType.LONG, DataType.DOUBLE)) {
                assertTrue(cast.areAllValuesNull());
                assertThat(cast.getPositionCount(), equalTo(3));
            }
        }
        try (Block source = blockFactory.newLongArrayVector(new long[] { 5L }, 1).asBlock()) {
            try (Block same = castStrict(source, DataType.LONG, DataType.LONG)) {
                assertThat(((LongBlock) same).getLong(0), equalTo(5L));
            }
        }
    }

    // ---- parseDatetimeMillis (shared string->datetime scalar) ----

    public void testParseDatetimeMillisDeclaredFormatZoneAware() {
        // Same token + format the CSV/NDJSON reader tests pin: the -0700 offset is honored, landing at 20:55:36Z.
        DateFormatter fmt = DateFormatter.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
        assertThat(DeclaredTypeCoercions.parseDatetimeMillis("10/Oct/2000:13:55:36 -0700", fmt), equalTo(971211336000L));
    }

    public void testParseDatetimeMillisIsoDefaultWhenNoFormat() {
        // No declared format falls back to strict_date_optional_time (the TO_DATETIME default).
        assertThat(DeclaredTypeCoercions.parseDatetimeMillis("2000-10-10T20:55:36Z", null), equalTo(971211336000L));
    }

    public void testParseDatetimeMillisThrowsOnGarbage() {
        expectThrows(IllegalArgumentException.class, () -> DeclaredTypeCoercions.parseDatetimeMillis("not-a-date", null));
    }

    // ---- helpers ----

    private Block castStrict(Block source, DataType from, DataType to) {
        return DeclaredTypeCoercions.castBlock(source, from, to, null, blockFactory, null, null);
    }

    private Block bytesBlock(String... values) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            return builder.build();
        }
    }

    private static SkipWarnings capturing(List<String> into) {
        return new SkipWarnings("summary") {
            @Override
            public void add(String detail) {
                into.add(detail);
            }
        };
    }
}
