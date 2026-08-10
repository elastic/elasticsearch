/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Utf8Sanitizer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.Arrays;

/**
 * Shared Parquet decode helpers used by both the baseline {@code ParquetColumnIterator}
 * and {@link OptimizedParquetColumnIterator}. Centralises list-column decoding,
 * timestamp conversion, UUID formatting, {@code unsigned_long} sign-flip encoding, and other utilities so that bug
 * fixes in one decode path are automatically reflected in the other.
 */
final class ParquetColumnDecoding {

    private ParquetColumnDecoding() {}

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    // ---- Temporal constants ----

    static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    static final long NANOS_PER_MILLI = 1_000_000L;
    /** Julian day number for Unix epoch (1970-01-01). */
    static final int JULIAN_EPOCH_OFFSET = 2_440_588;

    // ---- Temporal helpers ----

    static long convertTimestampToMillis(long raw, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MILLIS -> raw;
                case MICROS -> raw / 1_000;
                case NANOS -> raw / 1_000_000;
            };
        }
        return raw;
    }

    // ---- date_nanos (epoch-nanoseconds) helpers ----

    static final long NANOS_PER_MICRO = 1_000L;

    /**
     * Largest / smallest epoch-microsecond value that can be scaled to epoch-nanoseconds without
     * overflowing a {@code long}. These bounds coincide with the representable {@code date_nanos}
     * instant range (~1677-09-21 .. 2262-04-11): {@code Long.MAX_VALUE} nanoseconds is ~year 2262,
     * so a microsecond value beyond {@code Long.MAX_VALUE / 1_000} has no nanosecond representation.
     */
    static final long MAX_MICROS_AS_NANOS = Long.MAX_VALUE / NANOS_PER_MICRO;
    static final long MIN_MICROS_AS_NANOS = Long.MIN_VALUE / NANOS_PER_MICRO;

    /** Whether {@code logicalType} is a {@code TIMESTAMP(MICROS)} annotation (the only unit whose nanos scaling can overflow). */
    static boolean isMicrosTimestamp(LogicalTypeAnnotation logicalType) {
        return logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts
            && ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS;
    }

    /**
     * Whether decoding this column can turn a physically-present value into {@code null}. Temporal coercion is
     * partial: a negative epoch under {@code date_nanos}, a range overflow, or a format-parse failure all produce a
     * {@code null} for a value the row-group statistics counted as present. When that is possible, {@code IS NULL}
     * pushdown must decline — the decoded null set is larger than the physical one, so pushing {@code eq(col, null)}
     * would prune a group whose {@code nullCount == 0} yet holds rows that decode to null.
     *
     * <p>Precise on purpose: returning {@code true} for the identity cases would needlessly forfeit {@code IS NULL}
     * pruning on the common inferred reads (bare {@code INT64}, {@code TIMESTAMP(MILLIS)} datetime, and
     * {@code TIMESTAMP(NANOS)} date_nanos), which never null.
     */
    static boolean decodeCanNull(PrimitiveType primitiveType, DataType declaredType, @Nullable String declaredFormat) {
        if (declaredFormat != null) {
            return true; // a format parse can fail per value
        }
        if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
            // The only INT32 datetime is an inferred DATE (days -> millis), which never nulls; a format arm is
            // already handled above. So a no-format INT32 keeps its IS NULL pushdown.
            return false;
        }
        if (primitiveType.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64) {
            return true; // conservative for shapes this method does not model
        }
        LogicalTypeAnnotation logical = primitiveType.getLogicalTypeAnnotation();
        boolean millisAnnotated = logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts
            && ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS;
        boolean nanosAnnotated = logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts2
            && ts2.getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS;
        return switch (declaredType) {
            // Identity reads never null; every rescaling read can overflow at its edge.
            case DATETIME -> (logical == null || millisAnnotated) == false;
            // Bare INT64 rejects negatives via castBlock; NANOS is the identity; MICROS/MILLIS scale and can overflow.
            case DATE_NANOS -> nanosAnnotated == false;
            default -> true;
        };
    }

    @Nullable
    /**
     * How this column's decoded value relates to the raw value its parquet statistics hold, or {@code null} when no
     * exact relation exists and every stats-based decision must decline.
     *
     * <p>The derivation is parquet-local because annotation semantics are; the relation and the bound math it feeds
     * are shared ({@link DeclaredTypeCoercions}) because format semantics are reader-agnostic. This is the single
     * place the question is answered for predicate translation, the TopN threshold rail, and the page index — they
     * previously each re-derived it, which is how one of them was always wrong.
     *
     * <p>Note the direction flips on the DECLARED type, not on the file: a {@code TIMESTAMP(MICROS)} column decodes
     * to nanos ({@code raw x 1000}) when it infers {@code date_nanos}, and to millis ({@code raw / 1000}, truncating)
     * when it is declared {@code date}. An explicit declaration is allowed to be lossy — that is the point of
     * declaring it — but the pruning must know which way the map goes.
     *
     * @param declaredType the ES|QL type the column reads as ({@code DATETIME} or {@code DATE_NANOS})
     * @param declaredFormat the column's declared date format, or {@code null}
     */
    static DeclaredTypeCoercions.RawDecodeRelation rawDecodeRelation(
        PrimitiveType primitiveType,
        DataType declaredType,
        @Nullable String declaredFormat
    ) {
        if (primitiveType.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64) {
            return null;
        }
        LogicalTypeAnnotation logical = primitiveType.getLogicalTypeAnnotation();
        if (declaredFormat != null) {
            // A format composed on top of an annotation's own transform is not a single scale: decline.
            if (logical != null) {
                return null;
            }
            Long scale = DeclaredTypeCoercions.declaredEpochFormatScale(declaredFormat, declaredType);
            return scale == null ? null : new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(scale);
        }
        if (logical == null) {
            // A bare INT64 reads as the declared type's own unit — the fused identity.
            return new DeclaredTypeCoercions.RawDecodeRelation.Identity();
        }
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (declaredType) {
                case DATETIME -> switch (ts.getUnit()) {
                    case MILLIS -> new DeclaredTypeCoercions.RawDecodeRelation.Identity();
                    // narrowed through DateUtils.toMilliSeconds: one decoded milli covers a band of raw ticks
                    case MICROS -> new DeclaredTypeCoercions.RawDecodeRelation.ScaleDown(1_000L);
                    case NANOS -> new DeclaredTypeCoercions.RawDecodeRelation.ScaleDown(1_000_000L);
                };
                case DATE_NANOS -> switch (ts.getUnit()) {
                    case NANOS -> new DeclaredTypeCoercions.RawDecodeRelation.Identity();
                    case MICROS -> new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(NANOS_PER_MICRO);
                    case MILLIS -> new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(NANOS_PER_MILLI);
                };
                default -> null;
            };
        }
        // TIME (nanos-of-day, not an epoch), DECIMAL, unsigned INT64 (sign-wrap ordering), and anything this method
        // has never heard of: the scan applies a transform not modeled here, so no stats decision is safe.
        return null;
    }

    /**
     * Whether decoding a physical column with logical annotation {@code annotation} into an ESQL integral value
     * ({@code long}/{@code integer}) applies a scaling factor that the raw parquet footer statistics do NOT carry, so
     * a raw integral predicate pushed against those stats would mis-prune. This is the ground truth for the pushdown
     * decline guard ({@code ParquetPushedExpressions.pushDeclinedForUnitMismatch}); it lives here, next to the decode
     * transforms it summarizes, so the two cannot drift when a logical type is added:
     * <ul>
     *   <li>{@code DATE} — {@link #dateDaysToMillis} scales days ×86_400_000 to epoch-millis;</li>
     *   <li>{@code TIMESTAMP(MICROS)} / {@code TIME(MICROS)} — {@link #convertTimestampToNanos} / the time nanos
     *       multiplier scale ×1_000 to nanos ({@code MILLIS}/{@code NANOS} are identity);</li>
     *   <li>{@code DECIMAL(scale>0)} — the value reader divides the unscaled integer by 10^scale.</li>
     * </ul>
     */
    static boolean integralDecodeScalesRelativeToRawStats(LogicalTypeAnnotation annotation) {
        if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return true;
        }
        if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS;
        }
        if (annotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            return time.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS;
        }
        if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation dec) {
            return dec.getScale() != 0;
        }
        return false;
    }

    /** Whether scaling a {@code TIMESTAMP(MICROS)} value to epoch-nanoseconds would overflow a {@code long}. */
    static boolean microsOverflowsNanos(long micros) {
        return micros > MAX_MICROS_AS_NANOS || micros < MIN_MICROS_AS_NANOS;
    }

    /**
     * Converts a Parquet INT64 timestamp value to epoch-nanoseconds for a {@code DATE_NANOS} column:
     * {@code MICROS} is scaled by 1_000, {@code NANOS} passes through. {@code MILLIS} is included for
     * completeness (×1_000_000) although the type mapper only routes {@code MICROS}/{@code NANOS}
     * timestamps to {@code DATE_NANOS}. Callers must guard {@code MICROS} inputs against overflow via
     * {@link #microsOverflowsNanos(long)}; this method does not check.
     */
    static long convertTimestampToNanos(long raw, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MILLIS -> raw * 1_000_000L;
                case MICROS -> raw * NANOS_PER_MICRO;
                case NANOS -> raw;
            };
        }
        return raw;
    }

    /**
     * Emits a response {@code Warning} header noting that a Parquet timestamp column carried instants
     * outside the representable {@code date_nanos} range and those values were returned as null. The
     * message is column-scoped and constant, so the response-header machinery deduplicates it to a
     * single entry per affected column regardless of how many values or batches triggered it.
     */
    static void warnTimestampOutOfRange(ColumnInfo info) {
        ColumnDescriptor descriptor = info.descriptor();
        String column = descriptor == null ? "<unknown>" : String.join(".", descriptor.getPath());
        HeaderWarning.addWarning(
            "Parquet timestamp column ["
                + column
                + "] contains values outside the representable date_nanos range (~1677-09-21 to 2262-04-11); "
                + "such values are returned as null"
        );
    }

    /** Converts a date32 value (days since epoch) to epoch milliseconds. */
    static long dateDaysToMillis(long days) {
        return days * MILLIS_PER_DAY;
    }

    /**
     * Declared string&rarr;datetime coercion over an already-decoded bytes block: parses every value with the
     * column's declared format (ISO default when {@code null}) via the shared
     * {@link DeclaredTypeCoercions#parseDatetimeMillis} scalar — the same conversion the text readers apply at
     * parse time, so identical bytes with an identical declared format yield the identical instant. Preserves
     * nulls and multi-value positions. Does NOT take ownership of {@code source}; the caller closes it.
     * An unparseable value routes through {@link DeclaredTypeCoercions#onCoercionFailure} with the same
     * per-position semantics as {@code castBlock}: a live {@code warnings} sink nulls the whole position and
     * records one Warning, a {@code null} sink (strict, {@code fail_fast}) propagates the failure.
     */
    static Block bytesBlockToDatetimeMillis(
        Block source,
        @Nullable DateFormatter dateFormatter,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings
    ) {
        int positions = source.getPositionCount();
        if (source.areAllValuesNull()) {
            return blockFactory.newConstantNullBlock(positions);
        }
        BytesRefBlock bytes = (BytesRefBlock) source;
        BytesRef scratch = new BytesRef();
        long[] parsed = null;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = bytes.getValueCount(pos);
                if (bytes.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    BytesRef value = bytes.getBytesRef(bytes.getFirstValueIndex(pos), scratch);
                    try {
                        builder.appendLong(DeclaredTypeCoercions.parseDatetimeMillis(value.utf8ToString(), dateFormatter));
                    } catch (IllegalArgumentException | DateTimeException e) {
                        DeclaredTypeCoercions.onCoercionFailure(columnName, DataType.KEYWORD, DataType.DATETIME, e, warnings);
                        builder.appendNull();
                    }
                } else {
                    // Parse the whole position before appending: a failure mid-entry cannot be
                    // rolled back on the builder, and the bulk model nulls the whole position.
                    if (parsed == null || parsed.length < count) {
                        parsed = new long[count];
                    }
                    int firstIdx = bytes.getFirstValueIndex(pos);
                    boolean failed = false;
                    for (int v = 0; v < count && failed == false; v++) {
                        BytesRef value = bytes.getBytesRef(firstIdx + v, scratch);
                        try {
                            parsed[v] = DeclaredTypeCoercions.parseDatetimeMillis(value.utf8ToString(), dateFormatter);
                        } catch (IllegalArgumentException | DateTimeException e) {
                            DeclaredTypeCoercions.onCoercionFailure(columnName, DataType.KEYWORD, DataType.DATETIME, e, warnings);
                            failed = true;
                        }
                    }
                    if (failed) {
                        builder.appendNull();
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendLong(parsed[v]);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    /**
     * Converts a Parquet INT96 value (12 bytes LE: 8 bytes nanos-of-day + 4 bytes Julian day)
     * to epoch milliseconds. The bytes are read starting at {@code offset} for {@code length}
     * bytes (must be 12).
     */
    static long int96ToEpochMillis(byte[] bytes, int offset, int length) {
        if (length != 12) {
            throw new IllegalArgumentException("INT96 requires exactly 12 bytes, got " + length);
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buf.getLong();
        int julianDay = buf.getInt();
        long epochDay = julianDay - JULIAN_EPOCH_OFFSET;
        return epochDay * MILLIS_PER_DAY + nanosOfDay / NANOS_PER_MILLI;
    }

    /**
     * Decodes a Parquet footer stat value into the same representation the scan path produces, so
     * pushed-down MIN/MAX match a scan:
     * <ul>
     *   <li>date32 -&gt; epoch-millis (DATETIME)</li>
     *   <li>timestamp[millis] -&gt; epoch-millis (DATETIME)</li>
     *   <li>timestamp[micros|nanos] -&gt; epoch-nanos (DATE_NANOS), matching the scan path</li>
     *   <li>time -&gt; raw milliseconds for TIME_MILLIS (physical INT32), nanoseconds otherwise</li>
     * </ul>
     * Returns {@code null} when the type is not one of the above (caller falls through to other
     * normalization). INT96 is deliberately excluded: its footer min/max are compared by parquet-mr
     * as unsigned little-endian bytes (nanos-of-day in the low bytes), so they are not chronological
     * and cannot be trusted for MIN/MAX pushdown — returning {@code null} forces a scan instead. A
     * {@code timestamp[micros]} stat whose scaled value would overflow the {@code date_nanos} range
     * also returns {@code null} (forcing a scan) rather than publishing a wrapped-around bound.
     */
    static Long decodeTemporalStat(Object value, PrimitiveType type) {
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return dateDaysToMillis(((Number) value).longValue());
        }
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            long raw = ((Number) value).longValue();
            return switch (ts.getUnit()) {
                // MILLIS stays DATETIME (epoch-millis); MICROS/NANOS become DATE_NANOS (epoch-nanos) so the
                // published bound matches what the DATE_NANOS scan produces.
                case MILLIS -> raw;
                case NANOS -> raw;
                case MICROS -> microsOverflowsNanos(raw) ? null : raw * NANOS_PER_MICRO;
            };
        }
        if (logical instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            long raw = ((Number) value).longValue();
            // Mirror the scan path: TIME_MILLIS (physical INT32) stays raw ms; TIME_MICROS/NANOS
            // scale to nanoseconds via timeNanoMultiplier. Signed comparison of these physical
            // values is chronological, so footer min/max ordering is preserved.
            return type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32 ? raw : raw * timeNanoMultiplier(time);
        }
        return null;
    }

    /**
     * Whether the column carries a temporal logical-type annotation (date32, timestamp, or time) that
     * {@link #decodeTemporalStat} scales into the scan's output unit. When this is {@code true} but
     * {@code decodeTemporalStat} returns {@code null}, the footer statistic is temporal yet unusable
     * (a {@code timestamp[us]} value outside the representable {@code date_nanos} range, which the scan
     * nulls out) and the caller must drop the column's min/max <em>and</em> null count rather than
     * publishing a raw physical value. INT96 timestamps are deliberately excluded here: they have no
     * logical annotation and their footer min/max are not chronological, so they fall through to the
     * caller's generic fallback (unchanged behavior).
     */
    static boolean hasTemporalStatEncoding(PrimitiveType type) {
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        return logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation
            || logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
            || logical instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
    }

    /**
     * Returns the multiplier needed to convert a Parquet TIME_* value to nanoseconds.
     * TIME_MICROS values are stored as microseconds and must be multiplied by 1_000;
     * TIME_MILLIS and TIME_NANOS are stored in their final unit (ms handled as INTEGER, ns as-is).
     */
    static long timeNanoMultiplier(LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
        return time.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS ? 1_000L : 1L;
    }

    // ---- Unsigned integer logical-type checks ----

    /**
     * Returns {@code true} when {@code primitiveType} carries the Parquet {@code UINT_32} logical
     * annotation (physical {@code INT32}, {@code intType(32, false)}) — the shape that widens to
     * ESQL {@code LONG} because unsigned 32-bit values can exceed signed {@code int} range. Shared
     * by the predicate pushdown and stats-normalization code paths so the two cannot drift.
     */
    static boolean isUnsignedInt32(PrimitiveType primitiveType) {
        return isUnsignedInt(primitiveType, 32);
    }

    /**
     * Returns {@code true} when {@code primitiveType} carries the Parquet {@code UINT_64} logical
     * annotation (physical {@code INT64}, {@code intType(64, false)}) — the shape that maps to ESQL
     * {@code UNSIGNED_LONG}, which ESQL stores sign-flip-encoded via {@link #encodeUnsignedLong}.
     */
    static boolean isUnsignedInt64(PrimitiveType primitiveType) {
        return isUnsignedInt(primitiveType, 64);
    }

    private static boolean isUnsignedInt(PrimitiveType primitiveType, int bitWidth) {
        return primitiveType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogical
            && intLogical.isSigned() == false
            && intLogical.getBitWidth() == bitWidth;
    }

    /**
     * Decodes a FLOAT16 or DECIMAL footer stat value into the same {@code double} the scan path
     * produces (mirrors {@code readFloat16Column} / {@code readDecimalAsDoubleColumn}), so
     * pushed-down MIN/MAX match a scan. FLOAT16 is always Binary-backed (physical
     * {@code FIXED_LEN_BYTE_ARRAY(2)}), so its footer stat is a Parquet {@link Binary}. DECIMAL can
     * be backed by INT32, INT64, BINARY, or FIXED_LEN_BYTE_ARRAY — its stat arrives as an
     * {@link Integer}/{@link Long} unscaled value for the former two, and as {@link Binary} bytes
     * (big-endian two's complement) for the latter two.
     *
     * <p>Returns {@code null} when the type is not FLOAT16/DECIMAL, or the value's runtime type
     * doesn't match what the physical type implies (caller falls through to other normalization).
     */
    static Double decodeBinaryNumericStat(Object value, PrimitiveType type) {
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
            if (value instanceof Binary binary) {
                byte[] bytes = binary.getBytes();
                short float16Bits = (short) ((bytes[1] & 0xFF) << 8 | (bytes[0] & 0xFF));
                return (double) Float.float16ToFloat(float16Bits);
            }
            return null;
        }
        if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            BigInteger unscaled;
            if (value instanceof Binary binary) {
                unscaled = new BigInteger(binary.getBytes());
            } else if (value instanceof Number number) {
                unscaled = BigInteger.valueOf(number.longValue());
            } else {
                return null;
            }
            return new BigDecimal(unscaled, decimal.getScale()).doubleValue();
        }
        return null;
    }

    // ---- Unsigned long encoding ----

    /**
     * Sign-flip-encodes a raw {@code unsigned_long} value ({@code value ^ 2^63}) into ESQL's sortable signed
     * representation, mirroring the indexing path. ESQL stores {@code unsigned_long} inside a signed {@code LongBlock}
     * in this form so signed-long ordering matches unsigned ordering, and every value-output surface decodes it back on
     * the way out. Every Parquet read producer of an {@code unsigned_long} block must therefore route its INT64 values
     * through this method. Shared so the baseline, optimized, and list read paths cannot drift.
     */
    static long encodeUnsignedLong(long value) {
        return NumericUtils.asLongUnsigned(value);
    }

    /**
     * Applies {@link #encodeUnsignedLong(long)} to the first {@code count} values in place. Null slots within the range
     * hold undefined bits but are masked out by the caller, so encoding them is harmless.
     */
    static void encodeUnsignedLongInPlace(long[] values, int count) {
        for (int i = 0; i < count; i++) {
            values[i] = encodeUnsignedLong(values[i]);
        }
    }

    // ---- UUID formatting ----

    /**
     * Formats a 16-byte UUID in big-endian layout as the standard 8-4-4-4-12 hex string.
     * Requires exactly 16 bytes; shorter or longer arrays indicate an upstream bug since
     * Parquet {@code FIXED_LEN_BYTE_ARRAY(16)} is always exactly 16 bytes.
     */
    static String formatUuid(byte[] bytes) {
        return formatUuid(bytes, 0, bytes == null ? 0 : bytes.length);
    }

    /**
     * Formats a 16-byte UUID in big-endian layout as the standard 8-4-4-4-12 hex string.
     * The UUID bytes start at {@code offset} in the given array.
     */
    static String formatUuid(byte[] bytes, int offset, int length) {
        if (bytes == null || length != 16) {
            throw new IllegalArgumentException("UUID requires exactly 16 bytes, got " + (bytes == null ? "null" : length));
        }
        if (offset < 0 || offset + 16 > bytes.length) {
            throw new IllegalArgumentException("UUID byte offset out of bounds: offset=" + offset + ", array length=" + bytes.length);
        }
        StringBuilder sb = new StringBuilder(36);
        for (int i = 0; i < 16; i++) {
            sb.append(HEX[(bytes[offset + i] >> 4) & 0xF]);
            sb.append(HEX[bytes[offset + i] & 0xF]);
            if (i == 3 || i == 5 || i == 7 || i == 9) {
                sb.append('-');
            }
        }
        return sb.toString();
    }

    // ---- Skip helpers ----

    static void skipValues(ColumnReader cr, int rows) {
        for (int i = 0; i < rows; i++) {
            cr.consume();
        }
    }

    /**
     * Skips all Parquet values for the given number of rows in a LIST column,
     * respecting repetition levels to consume entire lists per row.
     */
    static void skipListValues(ColumnReader cr, int rows) {
        for (int row = 0; row < rows; row++) {
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                cr.consume();
            }
        }
    }

    // ---- List column reading ----

    /**
     * Reads a LIST column using repetition levels to determine list boundaries,
     * producing multi-valued ESQL blocks. Dispatches to the appropriate typed reader
     * based on the ESQL element type. Handles null lists, empty lists, and null
     * elements within lists correctly. Unsupported types are skipped and returned
     * as a constant null block.
     */
    static Block readListColumn(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        return readListColumn(cr, info, rows, blockFactory, null, null);
    }

    /**
     * As {@link #readListColumn(ColumnReader, ColumnInfo, int, BlockFactory)}, coercing a
     * declared element type beyond the fused pairs: the list decodes at the file's own element
     * type, then {@link DeclaredTypeCoercions#castBlock} coerces each element to the declared
     * type ({@code warnings} carries the per-value failure sink; {@code null} = strict).
     */
    static Block readListColumn(
        ColumnReader cr,
        ColumnInfo info,
        int rows,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings
    ) {
        DataType declared = info.esqlType();
        DataType fileElementType = info.fileEsqlType();
        if (fileElementType != null
            && declared != fileElementType
            && DeclaredTypeCoercions.fusedInDecode(fileElementType, declared, info.dateFormatter() != null) == false
            && DeclaredTypeCoercions.supports(fileElementType, declared)) {
            Block physical = readListColumn(cr, info.fileTyped(), rows, blockFactory);
            try {
                return DeclaredTypeCoercions.castBlock(
                    physical,
                    fileElementType,
                    declared,
                    info.dateFormatter(),
                    blockFactory,
                    columnName,
                    warnings
                );
            } finally {
                physical.close();
            }
        }
        DataType elementType = info.esqlType();
        int maxDef = info.maxDefLevel();
        return switch (elementType) {
            case INTEGER -> readListIntColumn(cr, maxDef, rows, blockFactory);
            case LONG -> {
                if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                    // TIME_MILLIS: physical INT32 widened to long (raw ms value, no unit conversion)
                    yield readListInt32AsLongColumn(cr, maxDef, rows, blockFactory);
                }
                long multiplier = info.logicalType() instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time
                    ? timeNanoMultiplier(time)
                    : 1L;
                yield readListLongColumn(cr, maxDef, rows, blockFactory, multiplier);
            }
            case UNSIGNED_LONG -> readListUnsignedLongColumn(cr, maxDef, rows, blockFactory);
            case DOUBLE -> readListDoubleColumn(cr, maxDef, rows, blockFactory);
            case BOOLEAN -> readListBooleanColumn(cr, maxDef, rows, blockFactory);
            case KEYWORD, TEXT -> readListBytesRefColumn(cr, info, rows, blockFactory);
            case DATETIME -> readListDatetimeColumn(cr, info, rows, blockFactory, columnName, warnings);
            case DATE_NANOS -> readListDateNanosColumn(cr, info, rows, blockFactory);
            default -> {
                skipListValues(cr, rows);
                yield blockFactory.newConstantNullBlock(rows);
            }
        };
    }

    /**
     * Reads a single list row from the Parquet column reader, handling repetition and
     * definition levels to produce multi-valued ESQL block entries. The supplied
     * {@link Runnable} is invoked once per element to append the current value from
     * the column reader into the block builder.
     *
     * <p>The caller must have positioned the column reader at the start of the row.
     * After this method returns, the reader is positioned at the start of the next row
     * (repetition level == 0).
     */
    private static void readListRow(ColumnReader cr, int maxDef, Block.Builder builder, Runnable appender) {
        int def = cr.getCurrentDefinitionLevel();
        if (def >= maxDef) {
            builder.beginPositionEntry();
            appender.run();
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                if (cr.getCurrentDefinitionLevel() >= maxDef) {
                    appender.run();
                }
                cr.consume();
            }
            builder.endPositionEntry();
        } else {
            cr.consume();
            boolean hasValues = false;
            while (cr.getCurrentRepetitionLevel() > 0) {
                if (cr.getCurrentDefinitionLevel() >= maxDef) {
                    if (hasValues == false) {
                        builder.beginPositionEntry();
                        hasValues = true;
                    }
                    appender.run();
                }
                cr.consume();
            }
            if (hasValues) {
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }
    }

    private static Block readListIntColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newIntBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendInt(cr.getInteger());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListLongColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory, long multiplier) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = multiplier == 1
                ? () -> builder.appendLong(cr.getLong())
                : () -> builder.appendLong(cr.getLong() * multiplier);
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListInt32AsLongColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendLong(cr.getInteger());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    /**
     * Reads a LIST of {@code unsigned_long} (Parquet INT64 with {@code intType(64, false)}) into a {@code LongBlock}.
     * Each element is sign-flip-encoded ({@code value ^ 2^63}) on the way in, mirroring the scalar read path and the
     * indexing path, so the always-decoding output edge produces the true unsigned value.
     */
    private static Block readListUnsignedLongColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendLong(encodeUnsignedLong(cr.getLong()));
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListDoubleColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newDoubleBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendDouble(cr.getDouble());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListBooleanColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newBooleanBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendBoolean(cr.getBoolean());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListBytesRefColumn(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        int maxDef = info.maxDefLevel();
        // UUID-annotated bytes are raw 16-byte payloads: format them as hex (matching the scalar path)
        // rather than sanitizing, which would mangle valid UUID bytes into replacement characters.
        boolean isUuid = info.logicalType() instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
        try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
            Runnable appender = isUuid
                ? () -> builder.appendBytesRef(new BytesRef(formatUuid(cr.getBinary().getBytes())))
                : () -> builder.appendBytesRef(Utf8Sanitizer.sanitize(new BytesRef(cr.getBinary().getBytes())));
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListDatetimeColumn(
        ColumnReader cr,
        ColumnInfo info,
        int rows,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings
    ) {
        // Declared string->datetime coercion for LIST<string> columns: parse each element via the shared
        // scalar with the column's declared format (ISO default), mirroring the flat decode paths. A parse
        // failure follows castBlock's bulk semantics (whole position nulls, or propagates when strict), so
        // this arm gathers each row before appending.
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.BINARY) {
            return readListStringDatetimeColumn(cr, info, rows, blockFactory, columnName, warnings);
        }
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            int maxDef = info.maxDefLevel();
            Runnable appender = () -> builder.appendLong(convertTimestampToMillis(cr.getLong(), info.logicalType()));
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    /**
     * The string&rarr;datetime LIST decode: walks each row's repetition levels like
     * {@link #readListRow} (defined elements append, null elements are skipped, a row with no
     * defined element is a null position) but parses the row into a primitive scratch first so a
     * mid-row parse failure can null the WHOLE position — {@code castBlock}'s bulk semantics —
     * instead of leaving a half-built entry. On failure the remaining elements are still
     * materialised (not parsed) so the column reader's data cursor stays in lock-step with its
     * level cursor for the rows that follow. {@code warnings} carries the per-position failure
     * sink; {@code null} = strict, the failure propagates.
     */
    private static Block readListStringDatetimeColumn(
        ColumnReader cr,
        ColumnInfo info,
        int rows,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings
    ) {
        int maxDef = info.maxDefLevel();
        DateFormatter dateFormatter = info.dateFormatter();
        long[] parsed = new long[8];
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                int count = 0;
                boolean failed = false;
                boolean rowDone = false;
                while (rowDone == false) {
                    if (cr.getCurrentDefinitionLevel() >= maxDef) {
                        // Always read the value so the data cursor advances even after a failure.
                        String value = cr.getBinary().toStringUsingUTF8();
                        if (failed == false) {
                            if (count == parsed.length) {
                                parsed = Arrays.copyOf(parsed, count * 2);
                            }
                            try {
                                parsed[count++] = DeclaredTypeCoercions.parseDatetimeMillis(value, dateFormatter);
                            } catch (IllegalArgumentException | DateTimeException e) {
                                DeclaredTypeCoercions.onCoercionFailure(columnName, DataType.KEYWORD, DataType.DATETIME, e, warnings);
                                failed = true;
                            }
                        }
                    }
                    cr.consume();
                    rowDone = cr.getCurrentRepetitionLevel() == 0;
                }
                if (failed || count == 0) {
                    // failed: bulk semantics null the whole position (already warned above).
                    // count == 0: null list, empty list, or all-null elements — a null position,
                    // matching readListRow's no-defined-values branch.
                    builder.appendNull();
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendLong(parsed[v]);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    /**
     * Reads a LIST of {@code DATE_NANOS} timestamps (Parquet INT64 {@code TIMESTAMP(MICROS|NANOS)}) into a
     * {@code LongBlock} of epoch-nanoseconds. {@code MICROS} elements whose scaled value would overflow the
     * representable {@code date_nanos} range are dropped from their list (mirroring how the generic list reader
     * already skips null elements); a list whose elements <em>all</em> overflow becomes a null position. A single
     * deduplicated warning header is emitted when any element was dropped. This uses a lazy {@code beginPositionEntry}
     * so an all-overflow defined list never triggers the empty-position assertion in {@link Block.Builder}.
     */
    private static Block readListDateNanosColumn(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        int maxDef = info.maxDefLevel();
        LogicalTypeAnnotation logical = info.logicalType();
        boolean micros = isMicrosTimestamp(logical);
        boolean[] anyOverflow = { false };
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readDateNanosListRow(cr, maxDef, micros, logical, builder, anyOverflow);
            }
            if (anyOverflow[0]) {
                warnTimestampOutOfRange(info);
            }
            return builder.build();
        }
    }

    /**
     * Reads a single {@code DATE_NANOS} list row, mirroring {@link #readListRow} but scaling each element to
     * epoch-nanoseconds and dropping {@code MICROS} elements that overflow the representable range. The position
     * entry is opened lazily on the first retained element, so a list with no retained elements is emitted as null.
     */
    private static void readDateNanosListRow(
        ColumnReader cr,
        int maxDef,
        boolean micros,
        LogicalTypeAnnotation logical,
        LongBlock.Builder builder,
        boolean[] anyOverflow
    ) {
        boolean open = cr.getCurrentDefinitionLevel() >= maxDef && appendDateNanosElement(cr, logical, micros, builder, false, anyOverflow);
        cr.consume();
        while (cr.getCurrentRepetitionLevel() > 0) {
            if (cr.getCurrentDefinitionLevel() >= maxDef) {
                open = appendDateNanosElement(cr, logical, micros, builder, open, anyOverflow);
            }
            cr.consume();
        }
        if (open) {
            builder.endPositionEntry();
        } else {
            builder.appendNull();
        }
    }

    /**
     * Appends the current column value as an epoch-nanosecond element, opening the position entry lazily.
     * Returns whether the entry is open afterwards. An out-of-range {@code MICROS} value is skipped (not appended)
     * and flips {@code anyOverflow}.
     */
    private static boolean appendDateNanosElement(
        ColumnReader cr,
        LogicalTypeAnnotation logical,
        boolean micros,
        LongBlock.Builder builder,
        boolean open,
        boolean[] anyOverflow
    ) {
        long raw = cr.getLong();
        if (micros && microsOverflowsNanos(raw)) {
            anyOverflow[0] = true;
            return open;
        }
        if (open == false) {
            builder.beginPositionEntry();
            open = true;
        }
        builder.appendLong(convertTimestampToNanos(raw, logical));
        return open;
    }

    // ---- NoOp converters for ColumnReadStoreImpl ----

    /**
     * Minimal GroupConverter that satisfies
     * {@link org.apache.parquet.column.impl.ColumnReadStoreImpl}'s constructor.
     * We never call {@code writeCurrentValueToConverter()}, so all converters are no-ops.
     */
    static final class NoOpGroupConverter extends GroupConverter {
        private final GroupType schema;

        NoOpGroupConverter(GroupType schema) {
            this.schema = schema;
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            Type field = schema.getType(fieldIndex);
            return field.isPrimitive() ? new NoOpPrimitiveConverter() : new NoOpGroupConverter(field.asGroupType());
        }

        @Override
        public void start() {}

        @Override
        public void end() {}
    }

    private static final class NoOpPrimitiveConverter extends PrimitiveConverter {}
}
