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
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.datasources.DeclaredSchemaValidator;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

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
            // numeric sources ride the unit rule (format names the unit, else the type does: millis)
            case DATETIME -> fromString
                || from == DataType.INTEGER
                || from == DataType.LONG
                || from == DataType.UNSIGNED_LONG
                || from == DataType.DOUBLE
                // an instant the file already typed: the unit is known, so nanos->millis narrows
                || from == DataType.DATE_NANOS;
            // string parse, the millis->nanos widen a date_nanos field runs on an epoch-millis token
            // at ingest (also cross-file DATETIME + DATE_NANOS unification), or a numeric source on
            // the same unit rule (format names the unit, else the type does: nanos)
            case DATE_NANOS -> fromString
                || from == DataType.DATETIME
                || from == DataType.INTEGER
                || from == DataType.LONG
                || from == DataType.UNSIGNED_LONG;
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
        assertTrue(
            "double->datetime rounds to epoch millis (the ::datetime semantic)",
            DeclaredTypeCoercions.supports(DataType.DOUBLE, DataType.DATETIME)
        );
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

        // The 2-arg predicate is the no-format overload, so the fused set is unchanged for a column with no format.
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.LONG, DataType.DATETIME, false));
        // A declared format defuses the epoch-millis reinterpret onto castBlock, which parses through the format.
        assertFalse(
            "long->datetime with a declared format runs through castBlock",
            DeclaredTypeCoercions.fusedInDecode(DataType.LONG, DataType.DATETIME, true)
        );
        // string->datetime stays fused either way: its fused BINARY decode arm already threads the declared formatter.
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.KEYWORD, DataType.DATETIME, false));
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.KEYWORD, DataType.DATETIME, true));
        // the format flag does not fuse a pair the no-format overload leaves unfused
        assertFalse(DeclaredTypeCoercions.fusedInDecode(DataType.LONG, DataType.DOUBLE, true));
        // integer->long and keyword<->text are format-agnostic
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.INTEGER, DataType.LONG, true));
        assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.KEYWORD, DataType.TEXT, true));
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
     * A physical DOUBLE column declared {@code long}/{@code integer} rounds like {@code ::long}/{@code ::integer}
     * (not truncates) — the "declared read is value-identical to the cast engine" claim, exercised for a numeric
     * (non-string) source. {@code testDeclaredNumericReadMatchesCastEngineRounding} only proves it for strings.
     */
    public void testCastDoubleToLongAndIntegerRounds() {
        try (
            Block src = blockFactory.newDoubleArrayVector(new double[] { 2.5, -1.9, 1000.0 }, 3).asBlock();
            Block cast = castStrict(src, DataType.DOUBLE, DataType.LONG)
        ) {
            LongBlock l = (LongBlock) cast;
            assertEquals(3L, l.getLong(0));   // 2.5 rounds to 3, not truncates to 2
            assertEquals(-2L, l.getLong(1));  // -1.9 rounds to -2
            assertEquals(1000L, l.getLong(2));
        }
        try (
            Block src = blockFactory.newDoubleArrayVector(new double[] { 2.5, -2.6 }, 2).asBlock();
            Block cast = castStrict(src, DataType.DOUBLE, DataType.INTEGER)
        ) {
            org.elasticsearch.compute.data.IntBlock i = (org.elasticsearch.compute.data.IntBlock) cast;
            assertEquals(3, i.getInt(0));
            assertEquals(-3, i.getInt(1));
        }
    }

    /**
     * Whole-number sources (integer/long, and the temporal epochs that surface as longs) coerce into
     * {@code unsigned_long} — value-preserving, sign-flip-encoded. Only the string→unsigned_long arm was
     * exercised before ({@code testCastStringToUnsignedLongSignFlipEncodes}).
     */
    public void testCastWholeNumberSourcesToUnsignedLong() {
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { 42L }, 1).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.UNSIGNED_LONG)
        ) {
            assertThat(NumericUtils.unsignedLongAsNumber(((LongBlock) cast).getLong(0)).longValue(), equalTo(42L));
        }
        try (
            Block src = blockFactory.newIntArrayVector(new int[] { 7 }, 1).asBlock();
            Block cast = castStrict(src, DataType.INTEGER, DataType.UNSIGNED_LONG)
        ) {
            assertThat(NumericUtils.unsignedLongAsNumber(((LongBlock) cast).getLong(0)).longValue(), equalTo(7L));
        }
        // A negative whole number has no unsigned representation: lenient nulls + warns, never a wrap-around value.
        List<String> warnings = new ArrayList<>();
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { -1L }, 1).asBlock();
            Block cast = DeclaredTypeCoercions.castBlock(
                src,
                DataType.LONG,
                DataType.UNSIGNED_LONG,
                null,
                blockFactory,
                "col",
                capturing(warnings)
            )
        ) {
            assertTrue("negative -> unsigned_long nulls the cell", cast.isNull(0));
            assertThat(warnings, hasSize(1));
        }
    }

    /**
     * An {@code unsigned_long} source (values held sign-flip-encoded in a LongBlock, including magnitudes above
     * {@code 2^63}) coerces to double and to keyword, rendering the true unsigned magnitude. This drives the
     * {@code converterFor(UNSIGNED_LONG, …)} arms on the BigInteger the decode produces.
     */
    public void testCastUnsignedLongSourcesToDoubleAndKeyword() {
        long big = NumericUtils.asLongUnsigned(new BigInteger("18446744073709551610")); // 2^64 - 6, above 2^63
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { big }, 1).asBlock();
            Block cast = castStrict(src, DataType.UNSIGNED_LONG, DataType.KEYWORD)
        ) {
            assertThat(((BytesRefBlock) cast).getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("18446744073709551610"));
        }
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { big }, 1).asBlock();
            Block cast = castStrict(src, DataType.UNSIGNED_LONG, DataType.DOUBLE)
        ) {
            assertThat(((DoubleBlock) cast).getDouble(0), equalTo(new BigInteger("18446744073709551610").doubleValue()));
        }
    }

    /**
     * A whole-number source declared {@code datetime} is reinterpreted as epoch-millis (the non-string DATETIME
     * arm), value-identical to a {@code long} read of the same token. Only the string→datetime arm was covered.
     */
    public void testCastIntegerAndLongToDatetimeUsesEpochMillis() {
        try (
            Block src = blockFactory.newIntArrayVector(new int[] { 42 }, 1).asBlock();
            Block cast = castStrict(src, DataType.INTEGER, DataType.DATETIME)
        ) {
            assertEquals(42L, ((LongBlock) cast).getLong(0)); // epoch millis
        }
        long epochMillis = 1704067200000L;
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { epochMillis }, 1).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.DATETIME)
        ) {
            assertEquals(epochMillis, ((LongBlock) cast).getLong(0));
        }
    }

    /**
     * A whole-number source declared {@code datetime} WITH a declared {@code format} parses the token THROUGH the
     * format as the epoch unit / parse dialect, overriding the epoch-millis reinterpret — the same semantic the
     * CSV/NDJSON readers apply to a numeric token. Covers INTEGER, LONG and UNSIGNED_LONG sources with
     * {@code epoch_second}.
     */
    public void testCastWholeNumberToDatetimeHonorsEpochSecondFormat() {
        DateFormatter epochSecond = DateFormatter.forPattern("epoch_second");
        long token = 1704067200L; // 2024-01-01T00:00:00Z, in seconds (fits an int as well as a long)
        long expectedMillis = 1704067200000L;
        try (
            Block src = blockFactory.newIntArrayVector(new int[] { (int) token }, 1).asBlock();
            Block cast = castStrict(src, DataType.INTEGER, DataType.DATETIME, epochSecond)
        ) {
            assertEquals(expectedMillis, ((LongBlock) cast).getLong(0));
        }
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { token }, 1).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.DATETIME, epochSecond)
        ) {
            assertEquals(expectedMillis, ((LongBlock) cast).getLong(0));
        }
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { NumericUtils.asLongUnsigned(BigInteger.valueOf(token)) }, 1).asBlock();
            Block cast = castStrict(src, DataType.UNSIGNED_LONG, DataType.DATETIME, epochSecond)
        ) {
            assertEquals(expectedMillis, ((LongBlock) cast).getLong(0));
        }
    }

    /**
     * A COMPOSITE format ({@code a||b}, the ES multi-format syntax — the default date format is itself
     * {@code strict_date_optional_time||epoch_millis}) serves a column whose carrier differs per file format: the same
     * declaration reads a calendar STRING token and a numeric EPOCH-SECOND token to the same instant. Alternatives are
     * tried left-to-right, and string-vs-number is unambiguous, so neither can shadow the other. This is the ClickBench
     * shape: {@code EventTime} is an int64 of Unix seconds in parquet and the string "2013-07-14 20:38:47" in NDJSON.
     */
    public void testCompositeFormatServesBothStringAndNumericCarriers() {
        DateFormatter composite = DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss||epoch_second");
        long expected = 1704067200000L; // 2024-01-01T00:00:00Z
        try (Block src = bytesBlock("2024-01-01 00:00:00"); Block cast = castStrict(src, DataType.KEYWORD, DataType.DATETIME, composite)) {
            assertEquals("the calendar alternative parses the string carrier", expected, ((LongBlock) cast).getLong(0));
        }
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { 1704067200L }, 1).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.DATETIME, composite)
        ) {
            assertEquals("the epoch_second alternative parses the numeric carrier", expected, ((LongBlock) cast).getLong(0));
        }
    }

    /** {@code epoch_millis} on a whole-number source is the identity reinterpret — the same value as no format. */
    public void testCastWholeNumberToDatetimeEpochMillisFormatIsIdentity() {
        DateFormatter epochMillis = DateFormatter.forPattern("epoch_millis");
        long token = 1704067200000L;
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { token }, 1).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.DATETIME, epochMillis)
        ) {
            assertEquals(token, ((LongBlock) cast).getLong(0));
        }
    }

    /** A calendar pattern reads a numeric token as its DIGITS, not as an epoch value: 20260101 -> 2026-01-01. */
    public void testCastWholeNumberToDatetimeCalendarPattern() {
        DateFormatter yyyyMMdd = DateFormatter.forPattern("yyyyMMdd");
        long expected = DeclaredTypeCoercions.parseDatetimeMillis("20260101", yyyyMMdd);
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { 20260101L }, 1).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.DATETIME, yyyyMMdd)
        ) {
            assertEquals(expected, ((LongBlock) cast).getLong(0));
        }
    }

    /** A numeric token the declared format cannot parse fails per-value: throw when strict, warn+null when lenient. */
    public void testCastWholeNumberToDatetimeUnparseableTokenFollowsErrorPolicy() {
        DateFormatter yyyyMMdd = DateFormatter.forPattern("yyyyMMdd");
        try (Block src = blockFactory.newLongArrayVector(new long[] { 7L }, 1).asBlock()) {
            expectThrows(InvalidArgumentException.class, () -> castStrict(src, DataType.LONG, DataType.DATETIME, yyyyMMdd).close());
        }
        List<String> warnings = new ArrayList<>();
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { 7L }, 1).asBlock();
            Block cast = DeclaredTypeCoercions.castBlock(
                src,
                DataType.LONG,
                DataType.DATETIME,
                yyyyMMdd,
                blockFactory,
                "ts",
                capturing(warnings)
            )
        ) {
            assertTrue(cast.isNull(0));
            assertThat(warnings, hasSize(1));
        }
    }

    /**
     * A {@code double} source declared {@code datetime}: with no format the value is epoch millis and its fractional
     * part rounds (the {@code ::datetime} / {@code safeDoubleToLong} semantic); with {@code epoch_second} it parses
     * as fractional seconds. A magnitude &ge; 1e7 must render plain-decimal (not scientific), else the epoch parser
     * rejects it.
     */
    public void testCastDoubleToDatetime() {
        try (
            Block src = blockFactory.newDoubleArrayVector(new double[] { 1704067200000.6 }, 1).asBlock();
            Block cast = castStrict(src, DataType.DOUBLE, DataType.DATETIME)
        ) {
            assertEquals(1704067200001L, ((LongBlock) cast).getLong(0)); // fraction rounds to the nearest milli
        }
        DateFormatter epochSecond = DateFormatter.forPattern("epoch_second");
        try (
            Block src = blockFactory.newDoubleArrayVector(new double[] { 1704067200.5 }, 1).asBlock();
            Block cast = castStrict(src, DataType.DOUBLE, DataType.DATETIME, epochSecond)
        ) {
            assertEquals(1704067200500L, ((LongBlock) cast).getLong(0)); // 0.5s -> 500ms, >= 1e7 plain-decimal render
        }
    }

    /**
     * A {@code date_nanos} source declared {@code datetime} narrows nanos&rarr;millis, truncating sub-millisecond
     * precision. This is what lets an annotated {@code TIMESTAMP(MICROS|NANOS)} parquet column — which infers as
     * {@code date_nanos} — be declared as the conventional {@code date}. Pinned value-identical to {@code ::datetime},
     * which maps DATE_NANOS through the same {@link DateUtils#toMilliSeconds}, so a declared read and an explicit cast
     * cannot disagree.
     */
    public void testCastDateNanosToDatetimeNarrowsToMillis() {
        long nanos = 1704067200_123_456_789L; // 2024-01-01T00:00:00.123456789Z
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { nanos }, 1).asBlock();
            Block cast = castStrict(src, DataType.DATE_NANOS, DataType.DATETIME)
        ) {
            assertEquals("nanos truncate to millis", 1704067200_123L, ((LongBlock) cast).getLong(0));
            assertEquals("must equal the ::datetime primitive", DateUtils.toMilliSeconds(nanos), ((LongBlock) cast).getLong(0));
        }
        // a pre-epoch nanos instant has no millis representation here: it throws and follows the read's error policy
        try (Block src = blockFactory.newLongArrayVector(new long[] { -1L }, 1).asBlock()) {
            expectThrows(InvalidArgumentException.class, () -> castStrict(src, DataType.DATE_NANOS, DataType.DATETIME).close());
        }
    }

    /**
     * A non-finite double in a {@code datetime} column follows {@code safeDoubleToLong} exactly as {@code ::datetime}
     * does today ({@code ToDatetime} maps DOUBLE via {@code ToLong.fromDouble} == {@code safeDoubleToLong}), so a
     * declared read and an explicit cast can never disagree: NaN rounds to epoch 0 and an infinity throws out-of-range.
     * NaN&rarr;1970 is a recorded consequence of mirroring the cast engine, not an accident. With a declared format
     * there is no parse for a non-finite token, so it fails per value through the error policy instead.
     */
    public void testCastNonFiniteDoubleToDatetimeMirrorsCastEngine() {
        try (
            Block src = blockFactory.newDoubleArrayVector(new double[] { Double.NaN }, 1).asBlock();
            Block cast = castStrict(src, DataType.DOUBLE, DataType.DATETIME)
        ) {
            assertEquals("NaN rounds to epoch 0, exactly as ::datetime does", 0L, ((LongBlock) cast).getLong(0));
        }
        try (Block src = blockFactory.newDoubleArrayVector(new double[] { Double.POSITIVE_INFINITY }, 1).asBlock()) {
            expectThrows(InvalidArgumentException.class, () -> castStrict(src, DataType.DOUBLE, DataType.DATETIME).close());
        }
        DateFormatter epochSecond = DateFormatter.forPattern("epoch_second");
        try (Block src = blockFactory.newDoubleArrayVector(new double[] { Double.NaN }, 1).asBlock()) {
            expectThrows(InvalidArgumentException.class, () -> castStrict(src, DataType.DOUBLE, DataType.DATETIME, epochSecond).close());
        }
    }

    /**
     * Temporal sources declared {@code double} yield the epoch value as a double — the untested temporal→double arm.
     */
    public void testCastTemporalToDouble() {
        long epochMillis = 1704067200000L;
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { epochMillis }, 1).asBlock();
            Block cast = castStrict(src, DataType.DATETIME, DataType.DOUBLE)
        ) {
            assertThat(((DoubleBlock) cast).getDouble(0), equalTo((double) epochMillis));
        }
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { epochMillis }, 1).asBlock();
            Block cast = castStrict(src, DataType.DATE_NANOS, DataType.DOUBLE)
        ) {
            assertThat(((DoubleBlock) cast).getDouble(0), equalTo((double) epochMillis));
        }
    }

    /**
     * The pairs {@link DeclaredTypeCoercions#fusedInDecode} claims as fusable are exactly the ones the columnar
     * readers decode without boxing; pin the boxed {@link DeclaredTypeCoercions#castBlock} reference value for
     * each so the fused fast path (exercised end-to-end in the per-format reader tests) has an authoritative
     * value to match. This is the SPI-level half; {@code ParquetFormatReaderTests} drives the fused path itself.
     */
    public void testFusedPairsCastBlockReferenceValues() {
        // integer -> long (lossless widen)
        try (
            Block src = blockFactory.newIntArrayVector(new int[] { 5, -7 }, 2).asBlock();
            Block cast = castStrict(src, DataType.INTEGER, DataType.LONG)
        ) {
            assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.INTEGER, DataType.LONG));
            assertEquals(5L, ((LongBlock) cast).getLong(0));
            assertEquals(-7L, ((LongBlock) cast).getLong(1));
        }
        // long -> datetime (epoch-millis reinterpret, same bits)
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { 1704067200000L }, 1).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.DATETIME)
        ) {
            assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.LONG, DataType.DATETIME));
            assertEquals(1704067200000L, ((LongBlock) cast).getLong(0));
        }
        // keyword <-> text (same bytes relabel)
        try (Block src = bytesBlock("hello"); Block cast = castStrict(src, DataType.KEYWORD, DataType.TEXT)) {
            assertTrue(DeclaredTypeCoercions.fusedInDecode(DataType.KEYWORD, DataType.TEXT));
            assertThat(((BytesRefBlock) cast).getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("hello"));
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
                InvalidArgumentException.class,
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

    /**
     * The date_nanos arm of {@link DeclaredTypeCoercions#scalarCoercer} must honor the declared format exactly as the
     * datetime arm above it does. It used to call the no-format overload, so a declared format on a date_nanos column
     * was silently ignored on every columnar reader (Parquet, ORC) and the cell fell back to ISO-8601 and failed.
     */
    public void testCastStringToDateNanosHonorsDeclaredFormat() {
        DateFormatter fmt = DateFormatter.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
        try (Block source = bytesBlock("10/Oct/2000:13:55:36 -0700")) {
            try (
                Block cast = DeclaredTypeCoercions.castBlock(source, DataType.KEYWORD, DataType.DATE_NANOS, fmt, blockFactory, null, null)
            ) {
                assertThat(((LongBlock) cast).getLong(0), equalTo(EsqlDataTypeConverter.dateNanosToLong("2000-10-10T20:55:36Z")));
            }
        }
    }

    /**
     * Sub-millisecond digits survive the declared-format date_nanos parse — the whole point of the type. A millisecond
     * conversion would silently truncate here.
     */
    public void testCastStringToDateNanosDeclaredFormatKeepsNanoPrecision() {
        DateFormatter fmt = DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        try (Block source = bytesBlock("2024-01-15 12:34:56.123456789")) {
            try (
                Block cast = DeclaredTypeCoercions.castBlock(source, DataType.KEYWORD, DataType.DATE_NANOS, fmt, blockFactory, null, null)
            ) {
                assertThat(((LongBlock) cast).getLong(0), equalTo(EsqlDataTypeConverter.dateNanosToLong("2024-01-15T12:34:56.123456789Z")));
            }
        }
    }

    /**
     * A null declared format means ISO-8601, not a NullPointerException. {@code dateNanosToLong(String, DateFormatter)}
     * dereferenced the formatter unguarded while its {@code dateTimeToLong} sibling defaulted; the two converters have
     * to agree on the null contract, because this call site threads an optional format into both.
     */
    public void testCastStringToDateNanosWithoutDeclaredFormatUsesIso() {
        try (Block source = bytesBlock("2024-01-15T12:34:56.123456789Z")) {
            try (
                Block cast = DeclaredTypeCoercions.castBlock(source, DataType.KEYWORD, DataType.DATE_NANOS, null, blockFactory, null, null)
            ) {
                assertThat(((LongBlock) cast).getLong(0), equalTo(EsqlDataTypeConverter.dateNanosToLong("2024-01-15T12:34:56.123456789Z")));
            }
        }
    }

    /**
     * A whole-number source declared {@code date_nanos} is an identity epoch-NANOS reinterpret — the declared
     * type names the numeric unit (datetime = millis, date_nanos = nanos), matching the shipped CSV
     * inline-schema numeric read and keeping footer stats, pushdown, and scan values bit-identical for a raw
     * column. NOT the mapper-ingest millis reading — see the class Javadoc's deliberate divergence.
     */
    public void testCastWholeNumberToDateNanosIsIdentityNanos() {
        long nanos = 1_700_000_000_123_456_789L;
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { 0L, nanos }, 2).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.DATE_NANOS)
        ) {
            LongBlock out = (LongBlock) cast;
            assertEquals(0L, out.getLong(out.getFirstValueIndex(0)));
            assertEquals("identity reinterpret, no scaling", nanos, out.getLong(out.getFirstValueIndex(1)));
        }
        try (
            Block src = blockFactory.newIntArrayVector(new int[] { 42 }, 1).asBlock();
            Block cast = castStrict(src, DataType.INTEGER, DataType.DATE_NANOS)
        ) {
            assertEquals(42L, ((LongBlock) cast).getLong(0));
        }
    }

    /**
     * A negative epoch has no {@code date_nanos} representation (the {@code TO_DATE_NANOS} range rule): the
     * lenient read nulls the cell and warns — never a negative nanos long — and the strict read fails.
     */
    /**
     * A whole-number source declared {@code date_nanos} WITH a declared {@code format} parses THROUGH the format —
     * the unit rule, identical to the DATETIME arm: the format names the unit, and only in its absence does the type
     * ({@code date_nanos} = nanos). Without this a column declared {@code {date_nanos, format: epoch_second}} would
     * silently reinterpret seconds as nanos: 1704067200 would read as 1970-01-01T00:00:01.37Z instead of 2024-01-01.
     * <p>
     * This is the pair that was jointly unreachable before the datetime and date_nanos work were reconciled — the
     * resolver rejected a format on a numeric physical, so the combination could not be expressed; once it could, the
     * numeric arm had to honor it.
     */
    public void testCastWholeNumberToDateNanosHonorsDeclaredFormat() {
        DateFormatter epochSecond = DateFormatter.forPattern("epoch_second");
        long token = 1704067200L;                    // 2024-01-01T00:00:00Z, in SECONDS
        long expectedNanos = 1704067200_000_000_000L; // the same instant, in nanos
        for (DataType from : List.of(DataType.INTEGER, DataType.LONG)) {
            Block src = from == DataType.INTEGER
                ? blockFactory.newIntArrayVector(new int[] { (int) token }, 1).asBlock()
                : blockFactory.newLongArrayVector(new long[] { token }, 1).asBlock();
            try (src; Block cast = castStrict(src, from, DataType.DATE_NANOS, epochSecond)) {
                assertEquals(
                    "[" + from.typeName() + "] declared date_nanos with epoch_second must read SECONDS, not nanos",
                    expectedNanos,
                    ((LongBlock) cast).getLong(0)
                );
            }
        }
        // unsigned_long rides the same arm (values held sign-flip-encoded in a LongBlock)
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { NumericUtils.asLongUnsigned(BigInteger.valueOf(token)) }, 1).asBlock();
            Block cast = castStrict(src, DataType.UNSIGNED_LONG, DataType.DATE_NANOS, epochSecond)
        ) {
            assertEquals(expectedNanos, ((LongBlock) cast).getLong(0));
        }
        // and a calendar dialect reads the digits, not an epoch
        DateFormatter yyyyMMdd = DateFormatter.forPattern("yyyyMMdd");
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { 20260101L }, 1).asBlock();
            Block cast = castStrict(src, DataType.LONG, DataType.DATE_NANOS, yyyyMMdd)
        ) {
            assertEquals(EsqlDataTypeConverter.dateNanosToLong("2026-01-01T00:00:00Z"), ((LongBlock) cast).getLong(0));
        }
    }

    public void testCastNegativeWholeNumberToDateNanosFailsPerValue() {
        List<String> warnings = new ArrayList<>();
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { -1L, 5L }, 2).asBlock();
            Block cast = DeclaredTypeCoercions.castBlock(
                src,
                DataType.LONG,
                DataType.DATE_NANOS,
                null,
                blockFactory,
                "ts",
                capturing(warnings)
            )
        ) {
            assertTrue("negative epoch nulls the cell", cast.isNull(0));
            LongBlock out = (LongBlock) cast;
            assertEquals("the good cell still decodes", 5L, out.getLong(out.getFirstValueIndex(1)));
        }
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0), containsString("declared type [date_nanos]"));
        try (Block src = blockFactory.newLongArrayVector(new long[] { -1L }, 1).asBlock()) {
            expectThrows(
                InvalidArgumentException.class,
                () -> DeclaredTypeCoercions.castBlock(src, DataType.LONG, DataType.DATE_NANOS, null, blockFactory, null, null).close()
            );
        }
    }

    /**
     * An {@code unsigned_long} source decodes through valueReader's sign-flip decode to the true Number, so a
     * magnitude &ge; 2^63 arrives as a BigInteger whose longValue() is negative and the non-negative domain
     * check rejects it per value — a wrapped positive cannot leak. In-domain magnitudes pass identically.
     */
    public void testCastUnsignedLongToDateNanosRejectsAboveSignedRangePerValue() {
        long inRange = NumericUtils.asLongUnsigned(BigInteger.valueOf(1_700_000_000_123_456_789L));
        long aboveSigned = NumericUtils.asLongUnsigned(new BigInteger("9223372036854775808")); // 2^63
        List<String> warnings = new ArrayList<>();
        try (
            Block src = blockFactory.newLongArrayVector(new long[] { inRange, aboveSigned }, 2).asBlock();
            Block cast = DeclaredTypeCoercions.castBlock(
                src,
                DataType.UNSIGNED_LONG,
                DataType.DATE_NANOS,
                null,
                blockFactory,
                "ts",
                capturing(warnings)
            )
        ) {
            LongBlock out = (LongBlock) cast;
            assertEquals(1_700_000_000_123_456_789L, out.getLong(out.getFirstValueIndex(0)));
            assertTrue("2^63 has no date_nanos representation — per-value failure, not a wrap", cast.isNull(1));
        }
        assertThat(warnings, hasSize(1));
    }

    /** Multi-value positions coerce element-by-element; a failing element nulls the whole position (bulk semantics). */
    public void testCastLongToDateNanosMultiValue() {
        List<String> warnings = new ArrayList<>();
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendLong(1L);
            builder.appendLong(2L);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendLong(3L);
            builder.appendLong(-1L);
            builder.endPositionEntry();
            try (Block source = builder.build()) {
                try (
                    Block cast = DeclaredTypeCoercions.castBlock(
                        source,
                        DataType.LONG,
                        DataType.DATE_NANOS,
                        null,
                        blockFactory,
                        "ts",
                        capturing(warnings)
                    )
                ) {
                    LongBlock out = (LongBlock) cast;
                    assertThat(out.getValueCount(0), equalTo(2));
                    int first = out.getFirstValueIndex(0);
                    assertEquals(1L, out.getLong(first));
                    assertEquals(2L, out.getLong(first + 1));
                    assertTrue("bulk semantics null the whole position on a negative element", cast.isNull(1));
                }
            }
        }
        assertThat(warnings, hasSize(1));
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

    /** The lenient branch degrades a null column name to {@code <unknown>} too, mirroring the strict path (shared detail). */
    public void testLenientCoercionNullColumnDegrades() {
        List<String> warnings = new ArrayList<>();
        SkipWarnings sink = capturing(warnings);
        try (Block source = bytesBlock("not-a-number")) {
            try (Block cast = DeclaredTypeCoercions.castBlock(source, DataType.KEYWORD, DataType.LONG, null, blockFactory, null, sink)) {
                assertTrue("the bad cell nulls under a live sink", cast.isNull(0));
            }
        }
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0), containsString("Column [<unknown>]"));
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

    /**
     * A strict coercion failure names the column, the declared type and the offending value, points at
     * {@code error_mode=null_field}, and is a client error (HTTP 400) — never the bare JDK parser exception
     * with no column/type context that regressed diagnosability. The original exception is chained as the cause.
     */
    public void testStrictCoercionFailureNamesColumnTypeAndValue() {
        try (Block src = bytesBlock("abc")) {
            InvalidArgumentException e = expectThrows(
                InvalidArgumentException.class,
                () -> DeclaredTypeCoercions.castBlock(src, DataType.KEYWORD, DataType.DOUBLE, null, blockFactory, "views", null).close()
            );
            assertThat(e.getMessage(), containsString("Column [views]"));
            assertThat(e.getMessage(), containsString("declared type [double]"));
            assertThat("the offending value survives on the message", e.getMessage(), containsString("abc"));
            assertThat(e.getMessage(), containsString("error_mode=null_field"));
            assertThat("a coercion failure is a client error, not a 500", e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertNotNull("the low-level parser exception is preserved as the cause", e.getCause());
        }
    }

    /**
     * The reported incident: a column of empty strings declared {@code double} failed with a bare
     * {@code NumberFormatException: empty String} naming neither the column nor the type. The retained
     * message now leads with the column and declared type while still carrying the raw detail.
     */
    public void testStrictCoercionFailureOnEmptyStringNamesColumn() {
        try (Block src = bytesBlock("")) {
            InvalidArgumentException e = expectThrows(
                InvalidArgumentException.class,
                () -> DeclaredTypeCoercions.castBlock(src, DataType.KEYWORD, DataType.DOUBLE, null, blockFactory, "FlashMinor2", null)
                    .close()
            );
            assertThat(e.getMessage(), containsString("Column [FlashMinor2]"));
            assertThat(e.getMessage(), containsString("declared type [double]"));
            assertThat("the raw JDK detail is retained, not discarded", e.getMessage(), containsString("empty String"));
        }
    }

    /**
     * The reused {@code ::} cast engine already throws {@link InvalidArgumentException} on an unparseable numeric
     * token; the chokepoint re-wraps it with column + declared-type context. The outer type stays
     * {@code InvalidArgumentException} (a client 400), the cast-engine exception is preserved as the cause, and the
     * message reads cleanly without a duplicated clause.
     */
    public void testStrictCoercionFailureWrapsCastEngineException() {
        try (Block src = bytesBlock("abc")) {
            InvalidArgumentException e = expectThrows(
                InvalidArgumentException.class,
                () -> DeclaredTypeCoercions.castBlock(src, DataType.KEYWORD, DataType.LONG, null, blockFactory, "views", null).close()
            );
            assertThat(e.getMessage(), containsString("Column [views]"));
            assertThat(e.getMessage(), containsString("declared type [long]"));
            assertTrue("the cast-engine exception is preserved as the cause", e.getCause() instanceof InvalidArgumentException);
        }
    }

    /** A reader that does not track a column name passes {@code null}; the message degrades to {@code <unknown>}, no NPE. */
    public void testStrictCoercionFailureNullColumnDegrades() {
        try (Block src = bytesBlock("abc")) {
            InvalidArgumentException e = expectThrows(
                InvalidArgumentException.class,
                () -> DeclaredTypeCoercions.castBlock(src, DataType.KEYWORD, DataType.DOUBLE, null, blockFactory, null, null).close()
            );
            assertThat(e.getMessage(), containsString("Column [<unknown>]"));
            assertThat(e.getMessage(), containsString("declared type [double]"));
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

    private Block castStrict(Block source, DataType from, DataType to, DateFormatter format) {
        return DeclaredTypeCoercions.castBlock(source, from, to, format, blockFactory, null, null);
    }

    private Block doubleBlock(double... values) {
        try (var builder = blockFactory.newDoubleBlockBuilder(values.length)) {
            for (double v : values) {
                builder.appendDouble(v);
            }
            return builder.build();
        }
    }

    /**
     * {@code epoch_millis} means "the number is already epoch-millis" — the identity. On a double it must therefore
     * ROUND like the format-free path (safeDoubleToLong), not truncate the fraction. 1.5 -> 2, matching {@code {date}}.
     */
    public void testDoubleToDatetimeEpochMillisRoundsLikeNoFormat() {
        DateFormatter epochMillis = DateFormatter.forPattern("epoch_millis");
        try (Block src = doubleBlock(1.5)) {
            try (
                Block cast = DeclaredTypeCoercions.castBlock(src, DataType.DOUBLE, DataType.DATETIME, epochMillis, blockFactory, null, null)
            ) {
                assertEquals("epoch_millis on a double is the identity: round, do not truncate", 2L, ((LongBlock) cast).getLong(0));
            }
        }
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

    /**
     * BigDecimal.toBigInteger() throws ArithmeticException -- not an IllegalArgumentException -- when the decimal
     * exponent is large enough that expanding it would overflow BigInteger. Escaping this method, it would bypass
     * every reader's per-cell error policy and hard-fail the whole read. It is remapped to the ordinary
     * out-of-range failure at the one place that parses.
     */
    public void testCoerceToUnsignedLongExoticExponentIsOutOfRangeNotArithmetic() {
        for (String token : List.of("1e999999999", "1e-999999999")) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> DeclaredTypeCoercions.coerceToUnsignedLong(token)
            );
            assertThat(e.getMessage(), containsString("out of range for an unsigned_long"));
        }
    }

    /** Every declarable type resolves to the shape PlannerUtils prescribes -- one enumeration, not two. */
    public void testElementTypeForAgreesWithPlannerUtils() {
        for (DataType type : DeclaredSchemaValidator.declarableTypes()) {
            assertThat(DeclaredTypeCoercions.elementTypeFor(type), equalTo(PlannerUtils.toElementType(type)));
        }
    }

    /**
     * PlannerUtils.toElementType signals an unmappable type with EsqlIllegalArgumentException, which is NOT an
     * IllegalArgumentException. elementTypeFor remaps it, so callers -- including NdJsonPageDecoder.setupBuilders,
     * which wraps this in a catch(IllegalArgumentException) -- need exactly one catch clause.
     */
    public void testElementTypeForThrowsIllegalArgumentForUnmappableType() {
        // SHORT is in toElementType's throw-set; DOC_DATA_TYPE maps to a shape this SPI cannot write.
        for (DataType type : List.of(DataType.SHORT, DataType.DOC_DATA_TYPE)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DeclaredTypeCoercions.elementTypeFor(type));
            assertThat(e.getMessage(), containsString("is not readable as a declared column"));
        }
    }

    /**
     * The convert-the-bound pushdown rests entirely on {@code decode(v) == v * declaredEpochFormatScale(...)} being
     * EXACT — a bound converted with a scale the decode does not actually follow would prune matching row groups.
     * Pin it over randomized values (including negatives, which pre-date the epoch) rather than assume it.
     */
    public void testDeclaredEpochFormatScaleMatchesTheActualDecode() {
        DateFormatter epochSecond = DateFormatter.forPattern("epoch_second");
        DateFormatter epochMillis = DateFormatter.forPattern("epoch_millis");
        for (int i = 0; i < 500; i++) {
            long seconds = randomLongBetween(-62_135_596_800L, 253_402_300_799L); // year 0001..9999
            assertEquals(
                "epoch_second -> datetime must be exactly x1000",
                seconds * 1_000L,
                DeclaredTypeCoercions.parseDatetimeMillis(String.valueOf(seconds), epochSecond)
            );
            assertEquals(
                "epoch_second scale must describe that decode",
                Long.valueOf(1_000L),
                DeclaredTypeCoercions.declaredEpochFormatScale("epoch_second", DataType.DATETIME)
            );

            long millis = randomLongBetween(-62_135_596_800_000L, 253_402_300_799_000L);
            assertEquals(
                "epoch_millis -> datetime must be the exact identity",
                millis,
                DeclaredTypeCoercions.parseDatetimeMillis(String.valueOf(millis), epochMillis)
            );
            assertEquals(
                "epoch_millis on datetime is the identity scale",
                Long.valueOf(1L),
                DeclaredTypeCoercions.declaredEpochFormatScale("epoch_millis", DataType.DATETIME)
            );
        }
    }

    /** The date_nanos half of the same pin: nanos have no identity format, so every format must rescale exactly. */
    public void testDeclaredEpochFormatScaleMatchesTheActualDateNanosDecode() {
        for (int i = 0; i < 500; i++) {
            // date_nanos spans ~1677..2262; stay inside it so the decode does not legitimately fail.
            long seconds = randomLongBetween(0L, 9_000_000_000L);
            assertEquals(
                "epoch_second -> date_nanos must be exactly x1e9",
                seconds * 1_000_000_000L,
                EsqlDataTypeConverter.dateNanosToLong(String.valueOf(seconds), DateFormatter.forPattern("epoch_second"))
            );
            assertEquals(Long.valueOf(1_000_000_000L), DeclaredTypeCoercions.declaredEpochFormatScale("epoch_second", DataType.DATE_NANOS));

            long millis = randomLongBetween(0L, 9_000_000_000_000L);
            assertEquals(
                "epoch_millis -> date_nanos must be exactly x1e6, NOT the identity",
                millis * 1_000_000L,
                EsqlDataTypeConverter.dateNanosToLong(String.valueOf(millis), DateFormatter.forPattern("epoch_millis"))
            );
            assertEquals(Long.valueOf(1_000_000L), DeclaredTypeCoercions.declaredEpochFormatScale("epoch_millis", DataType.DATE_NANOS));
        }
    }

    /**
     * A calendar format parses the digit string; a compound format resolves first-match-wins per value. Neither is a
     * single scale, so both must decline — and the compound case must not be mistaken for the identity by a
     * substring test, which would push a bound scaled 1000x wrong.
     */
    public void testNonEpochFormatsAdmitNoScale() {
        assertNull(DeclaredTypeCoercions.declaredEpochFormatScale("yyyyMMdd", DataType.DATETIME));
        assertNull(DeclaredTypeCoercions.declaredEpochFormatScale("strict_date_optional_time", DataType.DATETIME));
        assertNull(
            "a compound format containing epoch_millis is NOT the identity",
            DeclaredTypeCoercions.declaredEpochFormatScale("epoch_second||epoch_millis", DataType.DATETIME)
        );
        assertNull(DeclaredTypeCoercions.declaredEpochFormatScale("epoch_second||epoch_millis", DataType.DATE_NANOS));
        assertNull(DeclaredTypeCoercions.declaredEpochFormatScale(null, DataType.DATETIME));
        assertNull(
            "a non-temporal declared type never carries a format",
            DeclaredTypeCoercions.declaredEpochFormatScale("epoch_second", DataType.LONG)
        );
    }

    /**
     * The property every pruning decision depends on: a pushed bound must NEVER be stricter than the true predicate.
     * Too loose is harmless (the reader over-includes, FilterExec re-checks); too strict prunes a row group the query
     * matches, and a pruned group is never decoded. Brute-force it over the raw domain rather than restating the
     * algebra the implementation already claims.
     */
    public void testRawBoundIsNeverStricterThanTheTruth() {
        List<DeclaredTypeCoercions.RawDecodeRelation> relations = List.of(
            new DeclaredTypeCoercions.RawDecodeRelation.Identity(),
            new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(1000L),
            new DeclaredTypeCoercions.RawDecodeRelation.ScaleDown(1000L)
        );
        for (DeclaredTypeCoercions.RawDecodeRelation relation : relations) {
            for (DeclaredTypeCoercions.BoundOp op : DeclaredTypeCoercions.BoundOp.values()) {
                if (op == DeclaredTypeCoercions.BoundOp.EQ) {
                    continue; // EQ has no single-bound raw form; it routes through rawEqualityBand, tested separately
                }
                for (int i = 0; i < 200; i++) {
                    long raw = randomLongBetween(-5_000_000L, 5_000_000L);
                    long decoded = decodeFor(relation, raw);
                    long bound = randomLongBetween(-6_000L, 6_000L);
                    boolean trulyMatches = switch (op) {
                        case EQ -> decoded == bound;
                        case NOT_EQ -> decoded != bound;
                        case GT -> decoded > bound;
                        case GTE -> decoded >= bound;
                        case LT -> decoded < bound;
                        case LTE -> decoded <= bound;
                    };
                    Long rawBound = DeclaredTypeCoercions.rawBoundFor(relation, bound, op);
                    if (rawBound == null) {
                        continue; // declined: no pruning, never a lost row
                    }
                    boolean pushedKeeps = switch (op) {
                        case EQ -> raw == rawBound;
                        case NOT_EQ -> raw != rawBound;
                        case GT -> raw > rawBound;
                        case GTE -> raw >= rawBound;
                        case LT -> raw < rawBound;
                        case LTE -> raw <= rawBound;
                    };
                    // Ordered ops must be EXACT (never looser either): a loose bound is safe DIRECTLY but becomes
                    // stricter-than-truth once wrapped in NOT, which the translator admits. Only == is allowed to be
                    // a decline (null), never loose.
                    if (op != DeclaredTypeCoercions.BoundOp.EQ
                        && op != DeclaredTypeCoercions.BoundOp.NOT_EQ
                        && trulyMatches == false
                        && pushedKeeps) {
                        fail(
                            "LOOSER THAN TRUTH — unsafe under NOT: relation="
                                + relation
                                + " op="
                                + op
                                + " raw="
                                + raw
                                + " decodes to "
                                + decoded
                                + ", bound="
                                + bound
                                + ", pushed raw bound="
                                + rawBound
                        );
                    }
                    if (trulyMatches && pushedKeeps == false) {
                        fail(
                            "STRICTER THAN TRUTH — this prunes a matching row: relation="
                                + relation
                                + " op="
                                + op
                                + " raw="
                                + raw
                                + " decodes to "
                                + decoded
                                + ", bound="
                                + bound
                                + ", pushed raw bound="
                                + rawBound
                        );
                    }
                }
            }
        }
    }

    /** Mirrors what the readers actually do, written independently of the implementation under test. */
    private static long decodeFor(DeclaredTypeCoercions.RawDecodeRelation relation, long raw) {
        return switch (relation) {
            case DeclaredTypeCoercions.RawDecodeRelation.Identity ignored -> raw;
            case DeclaredTypeCoercions.RawDecodeRelation.ScaleUp up -> raw * up.factor();
            case DeclaredTypeCoercions.RawDecodeRelation.ScaleDown down -> Math.floorDiv(raw, down.divisor());
        };
    }

    /**
     * The equality band must be EXACT in both directions: every raw value in it decodes to the bound (so pushing it
     * keeps no junk the reader must re-filter), and every raw value outside it does not (so pushing it prunes no
     * matching row). A one-sided property would let a band that is too wide pass.
     */
    public void testEqualityBandIsExactlyTheRawValuesThatDecodeToTheBound() {
        List<DeclaredTypeCoercions.RawDecodeRelation> relations = List.of(
            new DeclaredTypeCoercions.RawDecodeRelation.Identity(),
            new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(1000L),
            new DeclaredTypeCoercions.RawDecodeRelation.ScaleDown(1000L)
        );
        for (DeclaredTypeCoercions.RawDecodeRelation relation : relations) {
            for (int i = 0; i < 300; i++) {
                long bound = randomLongBetween(-6_000L, 6_000L);
                DeclaredTypeCoercions.RawBand band = DeclaredTypeCoercions.rawEqualityBand(relation, bound);
                if (band == null) {
                    // Declined: only legal when NO raw value decodes to the bound at all.
                    for (long raw = -10_000; raw <= 10_000; raw++) {
                        assertNotEquals(
                            "declined an equality that a stored value could have matched: relation=" + relation + " bound=" + bound,
                            bound,
                            decodeFor(relation, raw)
                        );
                    }
                    continue;
                }
                assertTrue("band must not be inverted: " + band, band.lo() <= band.hi());
                for (long raw = band.lo(); raw <= band.hi(); raw++) {
                    assertEquals(
                        "a raw value inside the band must decode to the bound: relation=" + relation + " raw=" + raw,
                        bound,
                        decodeFor(relation, raw)
                    );
                }
                // just outside, both sides, must NOT decode to the bound
                assertNotEquals("band is too narrow on the low side", bound, decodeFor(relation, band.lo() - 1));
                assertNotEquals("band is too narrow on the high side", bound, decodeFor(relation, band.hi() + 1));
            }
        }
    }

    /** The forward map must be monotone, so a min stays a min and a max stays a max through it. */
    public void testForwardStatMapIsMonotone() {
        List<DeclaredTypeCoercions.RawDecodeRelation> relations = List.of(
            new DeclaredTypeCoercions.RawDecodeRelation.Identity(),
            new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(1000L),
            new DeclaredTypeCoercions.RawDecodeRelation.ScaleDown(1000L)
        );
        for (DeclaredTypeCoercions.RawDecodeRelation relation : relations) {
            for (int i = 0; i < 300; i++) {
                long a = randomLongBetween(-5_000_000L, 5_000_000L);
                long b = randomLongBetween(-5_000_000L, 5_000_000L);
                Long da = DeclaredTypeCoercions.rawStatToDecoded(relation, Math.min(a, b));
                Long db = DeclaredTypeCoercions.rawStatToDecoded(relation, Math.max(a, b));
                if (da != null && db != null) {
                    assertTrue("forward map inverted order: " + relation + " " + da + " > " + db, da <= db);
                }
                assertEquals(
                    "forward map must agree with the reader decode",
                    decodeFor(relation, a),
                    (long) DeclaredTypeCoercions.rawStatToDecoded(relation, a)
                );
            }
        }
    }

    /**
     * The forward stat map must DECLINE (return null) rather than wrap when a raw stat scaled up has no {@code long}
     * representation — a huge garbage {@code epoch_second} value under a {@code date_nanos} declaration, say. A wrapped
     * negative would read as a bogus extremum and skip the wrong TopN groups. The monotone property test above never
     * hits this because it stays within +/-5M; this pins the edge. Relaxing {@code multiplyExact} to {@code *} reds it.
     */
    public void testForwardStatMapDeclinesOnScaleUpOverflow() {
        var scaleUp = new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(1_000_000_000L); // epoch_second -> nanos
        long lastSafe = Long.MAX_VALUE / 1_000_000_000L;
        assertNotNull("the largest raw that still fits must map", DeclaredTypeCoercions.rawStatToDecoded(scaleUp, lastSafe));
        assertNull(
            "a raw whose x1e9 overflows Long.MAX must decline, not wrap",
            DeclaredTypeCoercions.rawStatToDecoded(scaleUp, lastSafe + 1)
        );
        assertNull(
            "negative overflow must decline too",
            DeclaredTypeCoercions.rawStatToDecoded(scaleUp, Long.MIN_VALUE / 1_000_000_000L - 1)
        );
    }

    /**
     * A huge epoch-second literal parses to a valid Instant, then x1000 to millis overflows a long. That overflow
     * must surface as an IllegalArgumentException the reader per-cell error policy can catch and null — not an
     * ArithmeticException that escapes it and hard-fails the whole read. Same contract coerceToUnsignedLong keeps.
     */
    public void testEpochSecondOverflowFailsPerCellNotAsArithmeticException() {
        DateFormatter epochSecond = DateFormatter.forPattern("epoch_second");
        long hugeSeconds = 10_000_000_000_000_000L; // parses fine as an Instant; x1000 overflows
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredTypeCoercions.parseDatetimeMillis(String.valueOf(hugeSeconds), epochSecond)
        );
        assertFalse("must not leak ArithmeticException past the per-cell policy", e instanceof ArithmeticException);
    }

    /**
     * Boundary-exact version of the never-stricter/never-looser property: random sampling almost never lands in the
     * 999-wide band where a ScaleDown bound can be loose, so probe the band edges DETERMINISTICALLY. A loose ordered
     * bound is safe directly but stricter-than-truth once wrapped in NOT, which the translator admits.
     */
    public void testOrderedBoundsAreExactAtBandEdges() {
        long d = 1000L;
        var rel = new DeclaredTypeCoercions.RawDecodeRelation.ScaleDown(d);
        for (long bound : new long[] { -3, 0, 1, 7 }) {
            long base = bound * d;
            // raw values straddling both the low edge (base) and the high edge (base + d - 1) of the band.
            for (long raw : new long[] { base - 1, base, base + 1, base + d - 1, base + d, base + d + 1 }) {
                long decoded = Math.floorDiv(raw, d);
                for (DeclaredTypeCoercions.BoundOp op : DeclaredTypeCoercions.BoundOp.values()) {
                    if (op == DeclaredTypeCoercions.BoundOp.EQ || op == DeclaredTypeCoercions.BoundOp.NOT_EQ) {
                        continue; // EQ/NOT_EQ are set-membership, not ordered — tested separately
                    }
                    Long rawBound = DeclaredTypeCoercions.rawBoundFor(rel, bound, op);
                    if (rawBound == null) {
                        continue;
                    }
                    boolean truth = switch (op) {
                        case GT -> decoded > bound;
                        case GTE -> decoded >= bound;
                        case LT -> decoded < bound;
                        case LTE -> decoded <= bound;
                        case EQ, NOT_EQ -> throw new AssertionError("skipped above");
                    };
                    boolean kept = switch (op) {
                        case GT -> raw > rawBound;
                        case GTE -> raw >= rawBound;
                        case LT -> raw < rawBound;
                        case LTE -> raw <= rawBound;
                        case EQ, NOT_EQ -> throw new AssertionError("skipped above");
                    };
                    assertEquals(
                        "ordered bound must be EXACT (safe under NOT): op="
                            + op
                            + " bound="
                            + bound
                            + " raw="
                            + raw
                            + " decodes "
                            + decoded
                            + " pushed="
                            + rawBound,
                        truth,
                        kept
                    );
                }
            }
        }
    }

    /** NOT_EQ restores the != pruning: exact for Identity and an exact-multiple ScaleUp, declined for a band. */
    public void testNotEqBoundExactOrDeclined() {
        var identity = new DeclaredTypeCoercions.RawDecodeRelation.Identity();
        var up = new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(1000L);
        var down = new DeclaredTypeCoercions.RawDecodeRelation.ScaleDown(1000L);
        var NE = DeclaredTypeCoercions.BoundOp.NOT_EQ;
        assertEquals("identity passes the bound through", Long.valueOf(5L), DeclaredTypeCoercions.rawBoundFor(identity, 5L, NE));
        assertEquals("scale-up exact multiple inverts by division", Long.valueOf(5L), DeclaredTypeCoercions.rawBoundFor(up, 5000L, NE));
        assertNull("scale-up non-multiple matches everything -> decline", DeclaredTypeCoercions.rawBoundFor(up, 5001L, NE));
        assertNull("scale-down != is a band complement -> decline", DeclaredTypeCoercions.rawBoundFor(down, 5L, NE));
    }
}
