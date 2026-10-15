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
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SharedErrorBudget;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Shared Parquet decode helpers used by both the baseline {@code ParquetColumnIterator}
 * and {@link OptimizedParquetColumnIterator}. Centralises list-column decoding,
 * timestamp conversion, UUID formatting, {@code unsigned_long} sign-flip encoding, and other utilities so that bug
 * fixes in one decode path are automatically reflected in the other.
 */
final class ParquetColumnDecoding {

    private static final Logger logger = LogManager.getLogger(ParquetColumnDecoding.class);

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
            // Only the MILLIS arm is reachable today: a TIMESTAMP(MICROS|NANOS) column renders date_nanos, and a
            // declared `date` over it is not a fused pair, so it narrows through castBlock rather than arriving
            // here. The sub-millisecond arms are kept, and floor rather than truncate with `/`, so that they agree
            // with that narrowing -- which floors -- if a route ever does reach them. Java's `/` rounds toward
            // zero, so the two would disagree by a millisecond below the epoch, which is the range that route
            // would exist to open.
            return switch (ts.getUnit()) {
                case MILLIS -> raw;
                case MICROS -> Math.floorDiv(raw, 1_000L);
                case NANOS -> Math.floorDiv(raw, 1_000_000L);
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

    /**
     * Whether {@code logicalType} states that an INT64 temporal column's raw value is microseconds, and so needs the
     * ×1_000 scale to reach the epoch-nanoseconds a {@code DATE_NANOS} block holds: {@code TIMESTAMP(MICROS)} or
     * {@code TIME(MICROS)}.
     *
     * <p>This predicate decides the scale itself, not merely whether to range-check it: {@link PageColumnReader}
     * reads it into a boolean and multiplies by {@link #NANOS_PER_MICRO} only when it is true, so narrowing it back
     * to {@code TIMESTAMP} would silently stop scaling every {@code TIME(MICROS)} column.
     *
     * <p>Only the timestamp can overflow the scale. A well-formed {@code TIME} value is a microsecond-of-day, so it
     * is bounded by 86_400_000_000 -- {@link #MAX_MICROS_AS_NANOS} divided by about 107_000 -- which makes the
     * overflow guard paired with this predicate dead for such a column rather than wrong for it. A file that
     * violates its own annotation is still nulled by that guard; see {@link #decodeCanNull}.
     */
    static boolean isMicrosTemporal(LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS;
        }
        return logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time
            && time.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS;
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
            // Every route an INT32 takes to a temporal block is total. DatetimeDecode gives it two arms over three
            // annotation cases (DATE_DAYS scales days to millis; INT32_EPOCH_MILLIS covers both a TIME(MILLIS),
            // already the millisecond of day, and a uint32 declared `date`, whose widened value is reinterpreted).
            // The castBlock routes are total too: the INTEGER arm is a widen, and the DOUBLE arm of an INT32 DECIMAL
            // divides by a power of ten, whose quotient can be neither non-finite nor outside long range. A format
            // arm is handled above, so a no-format INT32 keeps its IS NULL pushdown.
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
        // A TIME value is a time of day, so a well-formed one is at most 86_400_000_000 microseconds -- which is
        // MAX_MICROS_AS_NANOS divided by about 107_000 -- and neither its scale nor its narrowing can null. But
        // nothing enforces that a file obeys its own annotation, and a MICROS value past that bound is nulled by
        // microsOverflowsNanos like any other, so only the units with no guard on their path relax here. Trusting
        // the annotation instead would push IS NULL over a column whose decode can null, which drops rows.
        boolean timeAnnotated = logical instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time2
            && time2.getUnit() != LogicalTypeAnnotation.TimeUnit.MICROS;
        return switch (declaredType) {
            // An identity read never nulls, and neither does a narrowing: floorDiv is total over the whole long
            // domain. MICROS declines because its decode genuinely nulls: a column rendering date_nanos read as
            // `date` is NOT a fusedInDecode pair, so it decodes to nanoseconds first, where microsOverflowsNanos
            // nulls anything outside the nanosecond domain, and only then narrows. The decline is necessary, not
            // conservative, and must not be relaxed while that two-step route stands -- this predicate gates
            // row-group pruning, so a wrong `false` here drops rows. The MICROS arm of the TIME units is the same
            // case. Relaxing it also needs the arm decided by FILE type rather than declared: a uint64 column
            // reaches `date` through Numbers.toLong, which throws at or above 2^63, and a scale-0 DECIMAL through
            // safeDoubleToLong.
            case DATETIME -> (logical == null || millisAnnotated || nanosAnnotated || timeAnnotated) == false;
            // A bare INT64 widens millis->nanos via castBlock and nulls pre-epoch/post-2262 values; NANOS is the
            // identity; MICROS/MILLIS scale and can overflow.
            case DATE_NANOS -> (nanosAnnotated || timeAnnotated) == false;
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
     * to nanos ({@code raw x 1000}) when it infers {@code date_nanos}, and to millis ({@code floorDiv(raw, 1000)})
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
            // A bare INT64 is epoch millis (DeclaredTypeCoercions' unit rule): the fused identity under datetime, the
            // castBlock millis->nanos widen under date_nanos — the same relation a declared epoch_millis format has.
            return switch (declaredType) {
                case DATETIME -> new DeclaredTypeCoercions.RawDecodeRelation.Identity();
                case DATE_NANOS -> new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(NANOS_PER_MILLI);
                default -> null;
            };
        }
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (declaredType) {
                case DATETIME -> switch (ts.getUnit()) {
                    case MILLIS -> new DeclaredTypeCoercions.RawDecodeRelation.Identity();
                    // narrowed by floor division (DeclaredTypeCoercions): one decoded milli covers a band of raw ticks
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
        if (logical instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            // A TIME column reads as DATE_NANOS holding a nanosecond-of-day, so the relation is the same unit scale
            // the TIMESTAMP arm above applies for the same two units -- what the value counts from does not enter the
            // bound arithmetic. Sound against the footer because a TIME column's raw values compare chronologically
            // under signed comparison, the precondition decodeTemporalStat states for publishing their min/max.
            // Only a DATE_NANOS target gets a relation. A declared `date` over a TIME column composes to
            // floorDiv(raw x 1000, 1e6) == floorDiv(raw, 1000), i.e. the ScaleDown(1000) the TIMESTAMP(MICROS) arm
            // above already uses, so an arm for it is expressible -- it is simply not written, and declining costs
            // pruning rather than rows.
            //
            // MILLIS declines rather than publishing a relation. The Parquet spec puts TIME(MILLIS) on an INT32 and
            // parquet-mr enforces it -- Types rejects the pair with "TIME(MILLIS,true) can only annotate INT32" -- so
            // the arm is unreachable through that API. It declines anyway because the two sides would disagree if it
            // ever were reached: timeNanoMultiplier scales MILLIS by 1, while a nanosecond target wants 1_000_000.
            // Declining loses pruning; publishing the mismatched relation would lose rows.
            return declaredType == DataType.DATE_NANOS ? switch (time.getUnit()) {
                case NANOS -> new DeclaredTypeCoercions.RawDecodeRelation.Identity();
                case MICROS -> new DeclaredTypeCoercions.RawDecodeRelation.ScaleUp(NANOS_PER_MICRO);
                case MILLIS -> null;
            } : null;
        }
        // DECIMAL, unsigned INT64 (sign-wrap ordering), and anything this method has never heard of: the scan
        // applies a transform not modeled here, so no stats decision is safe.
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
     *   <li>{@code TIMESTAMP(MICROS)} / {@code TIME(MICROS)} — {@link #convertTemporalToNanos} / the time nanos
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
     * Converts a Parquet INT64 temporal value to the nanoseconds a {@code DATE_NANOS} column holds:
     * {@code MICROS} is scaled by 1_000, {@code NANOS} passes through. {@code TIMESTAMP(MILLIS)} is included for
     * completeness (×1_000_000) although the type mapper routes it to {@code DATETIME}. Callers must guard
     * {@code MICROS} inputs against overflow via {@link #microsOverflowsNanos(long)}; this method does not check.
     *
     * <p>A {@code TIME} annotation takes the same unit arithmetic, because the unit question is the same one: the
     * file states a unit and the block holds nanoseconds. What the result counts from differs -- a {@code TIMESTAMP}
     * is an offset from the epoch, a {@code TIME} a nanosecond-of-day, which {@code DATE_NANOS} renders as an
     * instant on 1970-01-01, at the annotation's own precision.
     *
     * <p>This method is the row-at-a-time and list path. The vectorized reader applies the same factor inline over
     * its batch array, gated on {@link #isMicrosTemporal}, and the footer-statistic decode
     * ({@link #decodeTemporalStat}) applies {@link #timeNanoMultiplier}; all three agree by construction, since for
     * a {@code TIME} column every one of them is x1_000 for {@code MICROS} and the identity otherwise.
     */
    static long convertTemporalToNanos(long raw, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MILLIS -> raw * 1_000_000L;
                case MICROS -> raw * NANOS_PER_MICRO;
                case NANOS -> raw;
            };
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            return raw * timeNanoMultiplier(time);
        }
        return raw;
    }

    /**
     * Reports that a Parquet timestamp column carried instants outside the representable {@code date_nanos} range and
     * those values were returned as null. When supplied, {@code warningSink} relays the message through the external
     * driver's warning channel so it survives a scan on another node; the {@code null} fallback writes directly to
     * the current thread's response headers for tests and synchronous callers.
     *
     * <p>The message is column-scoped and constant, so both the production informational-warning budget and response
     * headers deduplicate it regardless of how many values or batches triggered it.
     */
    static void warnTimestampOutOfRange(ColumnInfo info, @Nullable Consumer<String> warningSink) {
        ColumnDescriptor descriptor = info.descriptor();
        String column = descriptor == null ? "<unknown>" : String.join(".", descriptor.getPath());
        // The outcome is the same in every error_mode: no caller charges the budget or drops the row. A scalar is
        // nulled; a LIST element is dropped, and the list is null only when every element is out of range.
        String warning = "column [" + column + "]: timestamps outside the [date_nanos] range (1677-09-21 to 2262-04-11); returning null";
        if (warningSink != null) {
            warningSink.accept(warning);
        } else {
            HeaderWarning.addWarning(warning);
        }
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
     * semantics as {@code castBlock}: a live {@code warnings} sink records one Warning per value and nulls a
     * single-valued cell or removes the value from a multi-valued one (nulling it only when none parses), a
     * {@code null} sink (strict, {@code fail_fast}) propagates the failure.
     */
    static Block bytesBlockToDatetimeMillis(
        Block source,
        @Nullable DateFormatter dateFormatter,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings
    ) {
        return bytesBlockToDatetimeMillis(source, dateFormatter, blockFactory, columnName, warnings, null);
    }

    /**
     * Overload of {@link #bytesBlockToDatetimeMillis} that additionally reports failed positions to
     * {@code failedPositionSink} for {@code skip_row} callers. When non-null, a parse failure at position
     * {@code p} still nulls (or truncates) the cell in the returned block and calls {@code failedPositionSink.accept(p)}
     * so the caller's {@link org.elasticsearch.xpack.esql.datasources.spi.ColumnarRowDropHelper} can drop
     * the whole row at the page emit point.
     */
    static Block bytesBlockToDatetimeMillis(
        Block source,
        @Nullable DateFormatter dateFormatter,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings,
        @Nullable IntConsumer failedPositionSink
    ) {
        int positions = source.getPositionCount();
        if (source.areAllValuesNull()) {
            return blockFactory.newConstantNullBlock(positions);
        }
        BytesRefBlock bytes = (BytesRefBlock) source;
        BytesRef scratch = new BytesRef();
        boolean skipRow = failedPositionSink != null;
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
                        if (skipRow) failedPositionSink.accept(pos);
                        builder.appendNull();
                    }
                } else {
                    // castBlock's multi-value rule: an unparseable value is removed and the parseable ones kept,
                    // the entry opening on the first of them; under skip_row the first failure drops the row.
                    int firstIdx = bytes.getFirstValueIndex(pos);
                    boolean open = false;
                    for (int v = 0; v < count; v++) {
                        BytesRef value = bytes.getBytesRef(firstIdx + v, scratch);
                        long millis;
                        try {
                            millis = DeclaredTypeCoercions.parseDatetimeMillis(value.utf8ToString(), dateFormatter);
                        } catch (IllegalArgumentException | DateTimeException e) {
                            if (skipRow) {
                                DeclaredTypeCoercions.onCoercionFailure(columnName, DataType.KEYWORD, DataType.DATETIME, e, warnings);
                                failedPositionSink.accept(pos);
                                break;
                            }
                            DeclaredTypeCoercions.onCoercionFailure(columnName, DataType.KEYWORD, DataType.DATETIME, e, warnings, true);
                            continue;
                        }
                        if (open == false) {
                            builder.beginPositionEntry();
                            open = true;
                        }
                        builder.appendLong(millis);
                    }
                    if (open) {
                        builder.endPositionEntry();
                    } else {
                        builder.appendNull();
                    }
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
            //
            // The micros overflow guard is the same one the TIMESTAMP(MICROS) arm above carries, and it has to be
            // here for the same reason: the scan NULLS a microsecond value with no nanosecond representation, so a
            // stat that wrapped instead would publish a bound the scan never produces and serve it to MIN/MAX. Only
            // a file violating its own annotation reaches it -- a microsecond-of-day cannot exceed 86_400_000_000 --
            // but nothing enforces that a file obeys its annotation, which is the same standard decodeCanNull is
            // conservative under. NANOS multiplies by one and cannot overflow.
            if (type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
                return raw;
            }
            return time.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS && microsOverflowsNanos(raw)
                ? null
                : raw * timeNanoMultiplier(time);
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
     * TIME_MICROS values are stored as microseconds and must be multiplied by 1_000; TIME_NANOS is already in its
     * final unit. TIME_MILLIS does not reach this method: it is physically an INT32 and renders {@code datetime},
     * whose millisecond-of-day is the value itself.
     */
    static long timeNanoMultiplier(LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
        return time.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS ? 1_000L : 1L;
    }

    // ---- Datetime decode classification ----

    /**
     * Which arm decodes a column into a {@code datetime} block.
     *
     * <p>A {@code datetime} column reaches a decode loop either at the file's own type or through a coercion that
     * {@link DeclaredTypeCoercions#fusedInDecode} folded into the loop, and the second case is why the arm cannot be
     * read off the physical primitive name: the pair being coerced is stated in ESQL types
     * ({@code long -> datetime}, {@code keyword -> datetime}) and several Parquet shapes reach the same pair through
     * different physical types. An {@code INT32} is a day count only when the file says {@code DATE}; carrying
     * {@code UINT_32} it renders ESQL {@code long}, and carrying {@code TIME(MILLIS)} it renders {@code datetime},
     * whose millisecond-of-day needs no scaling. A text column is {@code BINARY} or
     * {@code FIXED_LEN_BYTE_ARRAY}, both of which render ESQL {@code keyword}.
     *
     * <p>Both decode engines classify here, so neither can key the choice on a different thing, and the resulting
     * value equals what {@link DeclaredTypeCoercions#castBlock} produces over the column's inferred decode — the
     * invariant the deferred late-materialization route relies on.
     */
    enum DatetimeDecode {
        /** 12-byte {@code INT96}: Julian day plus nanos-of-day. */
        INT96,
        /** A text column: parse each value with the column's declared format. */
        STRING_PARSE,
        /** {@code INT32} carrying {@code DATE}: the value is a day count. */
        DATE_DAYS,
        /**
         * An {@code INT32} that is not {@code DATE}: the value is epoch millis. Reached by a column rendering ESQL
         * {@code long} and, since TIME became a temporal type, by an inferred {@code TIME(MILLIS)} rendering
         * {@code datetime} — whose millisecond-of-day IS the epoch-millis instant on 1970-01-01, so the same
         * pass-through is right for both.
         */
        INT32_EPOCH_MILLIS,
        /** An {@code INT64}: the annotation's own unit normalization, then epoch millis. */
        INT64_EPOCH_MILLIS
    }

    /** Classifies {@code info}'s {@code datetime} decode; see {@link DatetimeDecode}. */
    static DatetimeDecode datetimeDecodeFor(ColumnInfo info) {
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT96) {
            return DatetimeDecode.INT96;
        }
        DataType fileType = info.fileEsqlType() == null ? info.esqlType() : info.fileEsqlType();
        if (DataType.isString(fileType)) {
            return DatetimeDecode.STRING_PARSE;
        }
        boolean int32 = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
        // The physical half of the test is load-bearing, not belt-and-braces: DATE_DAYS reads with the 32-bit
        // accessor, and DATE is only a legal annotation on an INT32. An illegal DATE on an INT64 stays on the
        // 64-bit arm rather than being read at the wrong width.
        if (int32 && info.logicalType() instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return DatetimeDecode.DATE_DAYS;
        }
        return int32 ? DatetimeDecode.INT32_EPOCH_MILLIS : DatetimeDecode.INT64_EPOCH_MILLIS;
    }

    /**
     * Whether an {@code INT32} column's value widens to {@code long} as unsigned: the annotation overload of
     * {@link #isUnsignedInt32(PrimitiveType)}, for the arms that hold the annotation rather than the primitive. Only
     * {@code uint32} does. A bare {@code INT32}, a {@code TIME}, a narrower unsigned annotation and every other
     * annotation sign-extend, which for {@code uint8} and {@code uint16} is the same value either way, since those
     * fit the positive int range.
     */
    static boolean isUnsignedInt32(@Nullable LogicalTypeAnnotation logical) {
        return logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogical
            && intLogical.isSigned() == false
            && intLogical.getBitWidth() == 32;
    }

    /**
     * The value the inferred ESQL {@code long} read of an {@code INT32} column produces, which widens {@code uint32}
     * rather than sign-extending it. A declared {@code datetime} reinterprets that number as epoch millis, but the
     * reinterpretation belongs to the caller.
     *
     * <p>No bit-width check is needed. {@code intType(32, false)} is the only annotation that renders {@code long}
     * from an {@code INT32} at all: a {@code uint8} or {@code uint16} renders {@code integer}, which is not a fused
     * {@code datetime} pair and never reaches this method -- and would widen identically anyway, both maxima fitting
     * the positive int range.
     */
    static long int32AsInferredLong(int raw, @Nullable LogicalTypeAnnotation logical) {
        return isUnsignedInt32(logical) ? Integer.toUnsignedLong(raw) : raw;
    }

    /**
     * The value the inferred read of an {@code INT64} column produces: a {@code TIMESTAMP} annotation's unit narrowed
     * to millis, and a bare {@code INT64} unchanged. As with {@link #int32AsInferredLong}, reinterpreting the result
     * as an epoch is the caller's step. No {@code TIME} case: an INT64 {@code TIME} column renders
     * {@code DATE_NANOS}, so it never reaches a fused {@code long -> datetime} decode.
     */
    static long int64AsInferredLong(long raw, @Nullable LogicalTypeAnnotation logical) {
        return convertTimestampToMillis(raw, logical);
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

    /**
     * The text an inferred {@code keyword} read of this column produces, which is what a declared
     * {@code keyword -> datetime} coercion must parse: a {@code UUID}-annotated column renders as the canonical
     * 8-4-4-4-12 hex string, every other binary as its own UTF-8 bytes. The list arm did not do this -- it parsed
     * the raw 16 bytes -- so a {@code LIST<FIXED_LEN_BYTE_ARRAY(16)>} annotated {@code UUID} and declared
     * {@code date} returned an instant decoded out of UUID bytes, while a scalar column of the identical shape is
     * refused and the inferred read of the same list renders hex. Routing the parse through this method makes the
     * list agree with both.
     */
    static String declaredTextOf(Binary binary, @Nullable LogicalTypeAnnotation logical) {
        return logical instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation
            ? formatUuid(binary.getBytes())
            : binary.toStringUsingUTF8();
    }

    // ---- Skip helpers ----

    /**
     * Shared policy, warning, and error-budget owner for malformed LIST repetition-level streams.
     * One instance is retained for the lifetime of an iterator or extractor so separate columns
     * and row groups contribute to the same warning cap. Iterators also charge their dropped rows
     * through {@link #completeBatch}; this keeps one error budget for every recoverable failure in
     * a Parquet read.
     */
    static final class ListCorruptionHandler {
        private final ErrorPolicy errorPolicy;
        private final String fileLocation;
        private final SkipWarnings warnings;
        /**
         * Highest row ordinal already charged, per column and row group, or {@code null} when every
         * event is charged. A high-water mark rather than the set of charged events: a decode pass
         * walks a row group's rows in ascending order, so an orphan at or below the mark was seen
         * (and charged) by the pass that reached the mark. That makes the mark exactly as precise as
         * the event set, while bounding this map by the number of columns and row groups rather than
         * by the number of malformed rows in the file.
         */
        @Nullable
        private final Map<RecoveryScope, Long> chargedRows;
        /** Shared budget for reader + adapter combined counting; {@code null} for the standalone path. */
        @Nullable
        private final SharedErrorBudget sharedBudget;
        private long errorCount;
        private long recoveredListErrorCount;
        private long droppedRowErrorCount;
        private long completedRows;
        private long rowsSeen;

        ListCorruptionHandler(ErrorPolicy errorPolicy, String fileLocation, @Nullable Consumer<String> warningSink) {
            this(errorPolicy, fileLocation, warningSink, false, null);
        }

        ListCorruptionHandler(
            ErrorPolicy errorPolicy,
            String fileLocation,
            @Nullable Consumer<String> warningSink,
            boolean deduplicateRecoveries
        ) {
            this(errorPolicy, fileLocation, warningSink, deduplicateRecoveries, null);
        }

        ListCorruptionHandler(
            ErrorPolicy errorPolicy,
            String fileLocation,
            @Nullable Consumer<String> warningSink,
            boolean deduplicateRecoveries,
            @Nullable SharedErrorBudget sharedBudget
        ) {
            this.errorPolicy = errorPolicy;
            this.fileLocation = fileLocation;
            this.chargedRows = deduplicateRecoveries ? new HashMap<>() : null;
            this.sharedBudget = sharedBudget;
            this.warnings = SkipWarnings.of(errorPolicy, "Malformed list data in [" + fileLocation + "]; skipping it", warningSink);
        }

        boolean isStrict() {
            return errorPolicy.isStrict();
        }

        void recoveredOrphan(String columnName, int rowGroupOrdinal, long rowOrdinal, long rowsSeen, long discardedValues) {
            if (chargedRows != null) {
                RecoveryScope scope = new RecoveryScope(columnName, rowGroupOrdinal);
                Long charged = chargedRows.get(scope);
                if (charged != null && rowOrdinal <= charged) {
                    return;
                }
                chargedRows.put(scope, rowOrdinal);
            }
            errorCount++;
            recoveredListErrorCount++;
            String detail = "column ["
                + columnName
                + "], row group ["
                + (rowGroupOrdinal + 1)
                + "], row ["
                + rowOrdinal
                + "]: ["
                + discardedValues
                + "] list values dropped";
            warnings.add(detail);
            // Structural corruption is discovered while streaming. As with the text readers, a
            // ratio can trip before later good rows have a chance to dilute it.
            this.rowsSeen = Math.max(this.rowsSeen, Math.max(1L, rowsSeen));
            if (sharedBudget != null) {
                sharedBudget.addErrors(1);
                sharedBudget.ensureRowsAtLeast(Math.max(1L, rowsSeen));
            }
            checkBudget();
            // The response detail omits the file (the summary names it); the log line has no summary, so it names it here.
            logger.log(errorPolicy.logErrors() ? Level.INFO : Level.DEBUG, "Malformed list data in [" + fileLocation + "]: " + detail);
        }

        /**
         * Completes one decoded batch and charges row drops to the same counter as recovered LIST fragments.
         */
        void completeBatch(int sourceRows, int droppedRows) {
            completedRows += sourceRows;
            rowsSeen = Math.max(rowsSeen, completedRows);
            errorCount += droppedRows;
            droppedRowErrorCount += droppedRows;
            if (sharedBudget != null) {
                // Use ensureRowsAtLeast rather than addReaderBatch so that a prior ensureRowsAtLeast
                // call from recoveredOrphan (which uses a file-global ordinal) does not cause
                // double-counting when completedRows reaches the same value additively.
                sharedBudget.ensureRowsAtLeast(completedRows);
                sharedBudget.addErrors(droppedRows);
            }
            checkBudget();
        }

        private void checkBudget() {
            String errorKind;
            if (droppedRowErrorCount == 0) {
                errorKind = "structural errors";
            } else if (recoveredListErrorCount == 0) {
                errorKind = "dropped rows";
            } else {
                errorKind = "errors";
            }
            if (sharedBudget != null) {
                sharedBudget.checkBudget(errorKind);
                return;
            }
            if (errorPolicy.isBudgetExceeded(errorCount, rowsSeen) == false) {
                return;
            }
            throw new ParsingException(
                Source.EMPTY,
                "[{}] {} in [{}] rows of [{}]; {}",
                errorCount,
                errorKind,
                rowsSeen,
                fileLocation,
                errorPolicy.trippedLimit(errorCount)
            );
        }

        private record RecoveryScope(String columnName, int rowGroupOrdinal) {}
    }

    /**
     * Stateful view of one repeated leaf in one row group. It keeps parquet-mr's level cursor and
     * physical-value cursor in lock-step, validates every record boundary, and makes end-of-stream
     * checks independent of parquet-mr's post-exhaustion sentinel levels.
     */
    static final class ListColumnReader {
        private final ColumnReader reader;
        private final int maxDefLevel;
        private final int maxRepLevel;
        private final ListCorruptionHandler corruptionHandler;
        private final String columnName;
        private final String fileLocation;
        private final int rowGroupOrdinal;
        private final long rowsBeforeGroup;
        private final long totalValueCount;
        private long consumedValueCount;
        private long rowCount;

        static ListColumnReader bind(
            ColumnReader reader,
            ColumnInfo info,
            ListCorruptionHandler corruptionHandler,
            String columnName,
            String fileLocation,
            int rowGroupOrdinal,
            long rowsBeforeGroup
        ) {
            return new ListColumnReader(
                reader,
                info.maxDefLevel(),
                info.maxRepLevel(),
                corruptionHandler,
                columnName,
                fileLocation,
                rowGroupOrdinal,
                rowsBeforeGroup
            );
        }

        private ListColumnReader(
            ColumnReader reader,
            int maxDefLevel,
            int maxRepLevel,
            ListCorruptionHandler corruptionHandler,
            String columnName,
            String fileLocation,
            int rowGroupOrdinal,
            long rowsBeforeGroup
        ) {
            this.reader = reader;
            this.maxDefLevel = maxDefLevel;
            this.maxRepLevel = maxRepLevel;
            this.corruptionHandler = corruptionHandler;
            this.columnName = columnName;
            this.fileLocation = fileLocation;
            this.rowGroupOrdinal = rowGroupOrdinal;
            this.rowsBeforeGroup = rowsBeforeGroup;
            this.totalValueCount = reader.getTotalValueCount();
        }

        ColumnReader reader() {
            return reader;
        }

        boolean hasRemaining() {
            return consumedValueCount < totalValueCount;
        }

        int currentDefinitionLevel() {
            ensureRemaining("read a definition level");
            int definitionLevel = reader.getCurrentDefinitionLevel();
            if (definitionLevel < 0 || definitionLevel > maxDefLevel) {
                throw corruption("definition level [" + definitionLevel + "] is outside [0, " + maxDefLevel + "]");
            }
            return definitionLevel;
        }

        int currentRepetitionLevel() {
            ensureRemaining("read a repetition level");
            int repetitionLevel = reader.getCurrentRepetitionLevel();
            if (repetitionLevel < 0 || repetitionLevel > maxRepLevel) {
                throw corruption("repetition level [" + repetitionLevel + "] is outside [0, " + maxRepLevel + "]");
            }
            return repetitionLevel;
        }

        /**
         * Positions the stream at the next logical row boundary. In non-strict modes, a leading
         * continuation is stream resynchronization rather than a per-row policy action: the
         * orphan cannot reliably be assigned to either adjacent row, so it consumes error budget
         * but does not drop or null the valid row that follows it.
         */
        void ensureRowStart() {
            int repetitionLevel = currentRepetitionLevel();
            if (repetitionLevel == 0) {
                return;
            }
            if (corruptionHandler.isStrict()) {
                throw corruption("row [" + (rowCount + 1) + "] starts at repetition level [" + repetitionLevel + "] instead of [0]");
            }

            long discarded = 0;
            do {
                skipCurrentValue();
                discarded++;
            } while (hasRemaining() && currentRepetitionLevel() > 0);

            if (hasRemaining() == false) {
                throw corruption(
                    "orphan continuation fragment before row ["
                        + (rowCount + 1)
                        + "] consumed the remaining ["
                        + discarded
                        + "] values without reaching repetition level [0]"
                );
            }
            corruptionHandler.recoveredOrphan(columnName, rowGroupOrdinal, rowCount + 1, rowsBeforeGroup + rowCount + 1, discarded);
        }

        void consumeAfterRead() {
            consumeCurrent();
        }

        void consumeUndefined() {
            consumeCurrent();
        }

        void skipCurrentValue() {
            if (currentDefinitionLevel() >= maxDefLevel) {
                reader.skip();
            }
            consumeCurrent();
        }

        void rowCompleted() {
            rowCount++;
        }

        void validateExhausted() {
            if (hasRemaining()) {
                throw corruption(
                    "footer row count was exhausted after ["
                        + rowCount
                        + "] rows but ["
                        + (totalValueCount - consumedValueCount)
                        + "] level values remain"
                );
            }
        }

        private void consumeCurrent() {
            ensureRemaining("consume a level value");
            reader.consume();
            consumedValueCount++;
        }

        private void ensureRemaining(String action) {
            if (hasRemaining() == false) {
                throw corruption(
                    "cannot "
                        + action
                        + ": footer row count exceeds the ["
                        + totalValueCount
                        + "] available level values after ["
                        + rowCount
                        + "] rows"
                );
            }
        }

        private IllegalArgumentException corruption(String detail) {
            return new IllegalArgumentException(
                "Malformed Parquet LIST column ["
                    + columnName
                    + "] in file ["
                    + fileLocation
                    + "] row group ["
                    + (rowGroupOrdinal + 1)
                    + "]: "
                    + detail
            );
        }
    }

    static void skipValues(ColumnReader cr, int rows) {
        for (int i = 0; i < rows; i++) {
            cr.consume();
        }
    }

    /**
     * Skips all Parquet values for the given number of rows in a LIST column,
     * respecting repetition levels to consume entire lists per row.
     */
    static void skipListValues(ListColumnReader input, int rows) {
        for (int row = 0; row < rows; row++) {
            input.ensureRowStart();
            input.skipCurrentValue();
            while (input.hasRemaining() && input.currentRepetitionLevel() > 0) {
                input.skipCurrentValue();
            }
            input.rowCompleted();
        }
    }

    // ---- List column reading ----

    /**
     * The summary header accompanying {@link #nullListElementsMessage}. Names no file on purpose: the loss follows
     * from the Block representation rather than from anything about one file, so a file-scoped summary would emit
     * one non-deduplicable line per file on a glob read. Shared with the read paths that own the collector so the
     * text has one source of truth.
     */
    static final String NULL_LIST_ELEMENTS_SUMMARY = "Lists hold null elements, which a multivalued field cannot; dropping them";

    /**
     * The per-column detail for a LIST read that dropped null elements. {@code columnName} is the attribute name the
     * query used, not the physical leaf path — for a LIST the descriptor path is {@code ints.list.element} while the
     * user wrote {@code ints}.
     *
     * <p>Constant per column by design: the drop is rediscovered on every batch, and each layer that caps this
     * channel ({@link SkipWarnings#addOnce}, {@code InformationalWarningBudget}, {@code ThreadContext}'s
     * response-header dedup) collapses repeats by exact text, so one column costs the client one line however many
     * batches, row groups, or files hit it.
     */
    static String nullListElementsMessage(String columnName) {
        return "column [" + columnName + "]: lists with null elements";
    }

    /**
     * The definition level a LIST element carries when its list entry exists but the element itself is null, or
     * {@code -1} when this column can have no such element ({@code -1} matches no real definition level).
     *
     * <p>Definition levels are prefix-defined, so {@code maxDefLevel - 1} means "every nullable node on the path is
     * defined except the deepest one". When the leaf primitive is {@code OPTIONAL} that deepest node <em>is</em> the
     * leaf, so the level reads "the repeated list entry is there, the value in it is null" — the element this decode
     * cannot represent and therefore drops. When the leaf is {@code REQUIRED} (a 3-level LIST of required elements)
     * or {@code REPEATED} (a legacy 2-level list, or a bare top-level {@code repeated} primitive, which
     * {@code buildColumnInfos} binds by {@code path[0]} and the iterators route here on {@code maxRepLevel > 0}) no
     * null element is representable at all, and {@code maxDefLevel - 1} instead means "the list is empty" — matching
     * it there would report a lossless read of every empty list.
     *
     * <p>Exact — never silent, never a false alarm — for every LIST-annotated column, which is what this reader is
     * for, and for a flattened repeated group whose leaf is its repeated node's direct child. Two boundaries worth
     * knowing:
     * <ul>
     *   <li>A nested LIST would put an inner empty list on a lower level and defeat a single comparison, but cannot
     *       reach here: {@code convertGroupTypeToEsql} types a non-primitive element {@code UNSUPPORTED} and
     *       {@code buildColumnInfos} skips it.</li>
     *   <li>An <em>un-annotated</em> {@code repeated} group with more than one nullable node beneath it (the
     *       Hive-era "2-level list of struct of struct", e.g. {@code repeated addr { optional geo { optional lat } }},
     *       which {@code collectAttributes} flattens to a {@code maxRepLevel == 1} leaf) has a second droppable
     *       level: {@code def == 1} there means the {@code addr} entry exists but {@code geo} is null, which loses a
     *       value this comparison does not catch. Recognising it exactly needs the definition level of the innermost
     *       repeated node — a schema walk, so a {@link ColumnInfo} field — which is deliberately out of scope for the
     *       LIST fix. The consequence is a residual silence on that one shape, never a wrong warning on any shape.</li>
     * </ul>
     */
    private static int nullElementDefLevel(ColumnInfo info) {
        ColumnDescriptor descriptor = info.descriptor();
        if (descriptor == null || descriptor.getPrimitiveType().getRepetition() != Type.Repetition.OPTIONAL) {
            return -1;
        }
        return info.maxDefLevel() - 1;
    }

    /**
     * Per-column tally of list elements dropped because an ES|QL multivalue has no representation for null. Carries
     * the definition level such an element has (see {@link #nullElementDefLevel}) so every row loop recognises one
     * with a single comparison, plus the flag they raise on a drop; {@link #readListColumn} turns a raised flag into
     * one response {@code Warning}. Mutable and single-threaded — one instance per {@code readListColumn} call.
     */
    private static final class NullElementTally {
        private final int nullElementDef;
        private boolean dropped;

        NullElementTally(ColumnInfo info) {
            this.nullElementDef = nullElementDefLevel(info);
        }

        /**
         * Raises the flag when {@code def} — the level of an element the caller is about to skip — is that of a null
         * element inside a list that exists. A lower level means the list itself is null or empty, which loses no
         * element and must not be reported.
         */
        void recordIfNullElement(int def) {
            if (def == nullElementDef) {
                dropped = true;
            }
        }
    }

    /**
     * Reads a LIST column using repetition levels to determine list boundaries, producing multi-valued ESQL blocks.
     * Dispatches to the appropriate typed reader based on the ESQL element type. Null lists and empty lists become
     * null positions. Unsupported types are skipped and returned as a constant null block.
     *
     * <p>A null element <em>inside</em> a list is dropped, because an ES|QL multivalue cannot hold null (a null lives
     * at position granularity: {@code Block.isNull} is per position and {@code AbstractBlockBuilder.appendNull}
     * closes any open position entry). The value cannot be preserved, so the read announces itself instead: any
     * dropped element emits one {@code Warning} through {@code lossWarnings} naming the column. That notice is
     * unconditional — unlike {@code warnings} below it must not be policy-gated, since the drop happens under every
     * {@code error_mode}.
     *
     * @param columnName the attribute name the query used for this column; names the column in both notices, so
     *                   unlike the coercion helpers downstream this one requires it
     * @param warnings   per-value declared-coercion failure sink, for a declared element type beyond the fused
     *                   pairs: the list decodes at the file's own element type, then
     *                   {@link DeclaredTypeCoercions#castBlock} coerces each element. {@code null} = strict, the
     *                   coercion failure propagates
     * @param informationalWarningSink relay for unconditional read-time notices such as out-of-range timestamps, or
     *                                 {@code null} to emit directly to the current thread's response headers
     * @param lossWarnings always-live collector for the dropped-null-element notice; required, since a {@code null}
     *                     here would silently restore the pre-fix silence
     */
    static Block readListColumn(
        ListColumnReader input,
        ColumnInfo info,
        int rows,
        BlockFactory blockFactory,
        String columnName,
        @Nullable SkipWarnings warnings,
        @Nullable IntConsumer failedPositionSink,
        @Nullable Consumer<String> informationalWarningSink,
        SkipWarnings lossWarnings
    ) {
        DataType declared = info.esqlType();
        DataType fileElementType = info.fileEsqlType();
        if (fileElementType != null
            && declared != fileElementType
            && DeclaredTypeCoercions.fusedInDecode(fileElementType, declared, info.dateFormatter() != null) == false
            && DeclaredTypeCoercions.supports(fileElementType, declared)) {
            // The recursion decodes at the file's own element type and owns the drop notice for this read; this
            // frame deliberately keeps no tally of its own, since it never reaches the row loops below.
            Block physical = readListColumn(
                input,
                info.fileTyped(),
                rows,
                blockFactory,
                columnName,
                null,
                null,
                informationalWarningSink,
                lossWarnings
            );
            try {
                return DeclaredTypeCoercions.castBlock(
                    physical,
                    fileElementType,
                    declared,
                    info.dateFormatter(),
                    blockFactory,
                    columnName,
                    warnings,
                    failedPositionSink
                );
            } finally {
                physical.close();
            }
        }
        DataType elementType = info.esqlType();
        int maxDef = info.maxDefLevel();
        NullElementTally tally = new NullElementTally(info);
        Block block = switch (elementType) {
            case INTEGER -> readListIntColumn(input, maxDef, rows, blockFactory, tally);
            case LONG -> {
                if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                    // uint32 is the one INT32 annotation that renders ESQL long, and its values above 2^31 must
                    // widen UNSIGNED. The scalar arms take that distinction as a parameter, so this one does too.
                    yield readListInt32AsLongColumn(input, maxDef, rows, blockFactory, tally, info.logicalType());
                }
                yield readListLongColumn(input, maxDef, rows, blockFactory, tally);
            }
            case UNSIGNED_LONG -> readListUnsignedLongColumn(input, maxDef, rows, blockFactory, tally);
            case DOUBLE -> readListDoubleColumn(input, maxDef, rows, blockFactory, tally);
            case BOOLEAN -> readListBooleanColumn(input, maxDef, rows, blockFactory, tally);
            case KEYWORD, TEXT -> readListBytesRefColumn(input, info, rows, blockFactory, tally);
            case DATETIME -> readListDatetimeColumn(input, info, rows, blockFactory, columnName, warnings, failedPositionSink, tally);
            case DATE_NANOS -> readListDateNanosColumn(input, info, rows, blockFactory, tally, informationalWarningSink);
            default -> {
                skipListValues(input, rows);
                yield blockFactory.newConstantNullBlock(rows);
            }
        };
        // Only after the block is built: a read that threw produced no answer, so it has nothing to caveat.
        if (tally.dropped) {
            lossWarnings.addOnce(nullListElementsMessage(columnName));
        }
        return block;
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
     *
     * <p>An element whose definition level is below {@code maxDef} has no value to append and is skipped; when it is
     * a null element of a list that exists (rather than the level of a null or empty list) that is a lossy read, so
     * it is recorded on {@code tally} for the caller to announce.
     */
    private static void readListRow(ListColumnReader input, int maxDef, Block.Builder builder, Runnable appender, NullElementTally tally) {
        input.ensureRowStart();
        int def = input.currentDefinitionLevel();
        if (def >= maxDef) {
            builder.beginPositionEntry();
            appender.run();
            input.consumeAfterRead();
            while (input.hasRemaining() && input.currentRepetitionLevel() > 0) {
                int elementDef = input.currentDefinitionLevel();
                if (elementDef >= maxDef) {
                    appender.run();
                    input.consumeAfterRead();
                } else {
                    tally.recordIfNullElement(elementDef);
                    input.consumeUndefined();
                }
            }
            builder.endPositionEntry();
        } else {
            tally.recordIfNullElement(def);
            input.consumeUndefined();
            boolean hasValues = false;
            while (input.hasRemaining() && input.currentRepetitionLevel() > 0) {
                int elementDef = input.currentDefinitionLevel();
                if (elementDef >= maxDef) {
                    if (hasValues == false) {
                        builder.beginPositionEntry();
                        hasValues = true;
                    }
                    appender.run();
                    input.consumeAfterRead();
                } else {
                    tally.recordIfNullElement(elementDef);
                    input.consumeUndefined();
                }
            }
            if (hasValues) {
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }
        input.rowCompleted();
    }

    private static Block readListIntColumn(
        ListColumnReader input,
        int maxDef,
        int rows,
        BlockFactory blockFactory,
        NullElementTally tally
    ) {
        try (var builder = blockFactory.newIntBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendInt(input.reader().getInteger());
            for (int row = 0; row < rows; row++) {
                readListRow(input, maxDef, builder, appender, tally);
            }
            return builder.build();
        }
    }

    private static Block readListLongColumn(
        ListColumnReader input,
        int maxDef,
        int rows,
        BlockFactory blockFactory,
        NullElementTally tally
    ) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendLong(input.reader().getLong());
            for (int row = 0; row < rows; row++) {
                readListRow(input, maxDef, builder, appender, tally);
            }
            return builder.build();
        }
    }

    /**
     * Reads a LIST whose {@code INT32} elements render ESQL {@code long}, widening each element the way the scalar
     * read of the same annotation widens it: {@code uint32} unsigned, everything else sign-extended. The annotation
     * is the whole input to that choice, which is why it is a parameter rather than read from the primitive.
     */
    private static Block readListInt32AsLongColumn(
        ListColumnReader input,
        int maxDef,
        int rows,
        BlockFactory blockFactory,
        NullElementTally tally,
        @Nullable LogicalTypeAnnotation logical
    ) {
        boolean unsigned = isUnsignedInt32(logical);
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = unsigned
                ? () -> builder.appendLong(Integer.toUnsignedLong(input.reader().getInteger()))
                : () -> builder.appendLong(input.reader().getInteger());
            for (int row = 0; row < rows; row++) {
                readListRow(input, maxDef, builder, appender, tally);
            }
            return builder.build();
        }
    }

    /**
     * Reads a LIST of {@code unsigned_long} (Parquet INT64 with {@code intType(64, false)}) into a {@code LongBlock}.
     * Each element is sign-flip-encoded ({@code value ^ 2^63}) on the way in, mirroring the scalar read path and the
     * indexing path, so the always-decoding output edge produces the true unsigned value.
     */
    private static Block readListUnsignedLongColumn(
        ListColumnReader input,
        int maxDef,
        int rows,
        BlockFactory blockFactory,
        NullElementTally tally
    ) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendLong(encodeUnsignedLong(input.reader().getLong()));
            for (int row = 0; row < rows; row++) {
                readListRow(input, maxDef, builder, appender, tally);
            }
            return builder.build();
        }
    }

    private static Block readListDoubleColumn(
        ListColumnReader input,
        int maxDef,
        int rows,
        BlockFactory blockFactory,
        NullElementTally tally
    ) {
        try (var builder = blockFactory.newDoubleBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendDouble(input.reader().getDouble());
            for (int row = 0; row < rows; row++) {
                readListRow(input, maxDef, builder, appender, tally);
            }
            return builder.build();
        }
    }

    private static Block readListBooleanColumn(
        ListColumnReader input,
        int maxDef,
        int rows,
        BlockFactory blockFactory,
        NullElementTally tally
    ) {
        try (var builder = blockFactory.newBooleanBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendBoolean(input.reader().getBoolean());
            for (int row = 0; row < rows; row++) {
                readListRow(input, maxDef, builder, appender, tally);
            }
            return builder.build();
        }
    }

    private static Block readListBytesRefColumn(
        ListColumnReader input,
        ColumnInfo info,
        int rows,
        BlockFactory blockFactory,
        NullElementTally tally
    ) {
        int maxDef = info.maxDefLevel();
        // UUID-annotated bytes are raw 16-byte payloads: format them as hex (matching the scalar path)
        // rather than sanitizing, which would mangle valid UUID bytes into replacement characters.
        boolean isUuid = info.logicalType() instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
        try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
            Runnable appender = isUuid
                ? () -> builder.appendBytesRef(new BytesRef(formatUuid(input.reader().getBinary().getBytes())))
                : () -> builder.appendBytesRef(Utf8Sanitizer.sanitize(new BytesRef(input.reader().getBinary().getBytes())));
            for (int row = 0; row < rows; row++) {
                readListRow(input, maxDef, builder, appender, tally);
            }
            return builder.build();
        }
    }

    private static Block readListDatetimeColumn(
        ListColumnReader input,
        ColumnInfo info,
        int rows,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings,
        @Nullable IntConsumer failedPositionSink,
        NullElementTally tally
    ) {
        // The element's decode is classified exactly as a flat column's (see DatetimeDecode), so a LIST and a
        // scalar of the same physical shape read the same way. Keying it on the physical name alone handed every
        // element that was not a bare INT64 -- a DATE's day count, a uint32, a TIME, an INT96, fixed-width text --
        // to getLong(), which the column reader does not implement for those types.
        DatetimeDecode decode = datetimeDecodeFor(info);
        if (decode == DatetimeDecode.STRING_PARSE) {
            // Declared string->datetime coercion for LIST<string> columns: parse each element via the shared
            // scalar with the column's declared format (ISO default), mirroring the flat decode paths. A parse
            // failure follows castBlock's multi-value rule (the element is removed, or the failure propagates when
            // strict).
            return readListStringDatetimeColumn(input, info, rows, blockFactory, columnName, warnings, failedPositionSink, tally);
        }
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            int maxDef = info.maxDefLevel();
            LogicalTypeAnnotation logical = info.logicalType();
            Runnable appender = switch (decode) {
                case DATE_DAYS -> () -> builder.appendLong(dateDaysToMillis(input.reader().getInteger()));
                case INT32_EPOCH_MILLIS -> () -> builder.appendLong(int32AsInferredLong(input.reader().getInteger(), logical));
                case INT64_EPOCH_MILLIS -> () -> builder.appendLong(int64AsInferredLong(input.reader().getLong(), logical));
                case INT96 -> () -> {
                    byte[] bytes = input.reader().getBinary().getBytes();
                    builder.appendLong(int96ToEpochMillis(bytes, 0, bytes.length));
                };
                case STRING_PARSE -> throw new AssertionError("handled before the loop: " + decode);
            };
            for (int row = 0; row < rows; row++) {
                readListRow(input, maxDef, builder, appender, tally);
            }
            return builder.build();
        }
    }

    /**
     * The string&rarr;datetime LIST decode: walks each row's repetition levels like
     * {@link #readListRow} (defined elements append, null elements are skipped, a row with no
     * defined element is a null position) and follows {@code castBlock}'s multi-value rule: an
     * unparseable element is removed and the parseable ones kept, the position entry opening lazily
     * on the first of them, so a row none of whose elements parses is a null position. Under
     * {@code skip_row} the first failure drops the row and the rest are not parsed, but every element
     * is still materialised so the column reader's data cursor stays in lock-step with its level
     * cursor for the rows that follow. {@code warnings} carries the per-value failure sink;
     * {@code null} = strict, the failure propagates. A skipped null element is recorded on
     * {@code tally} for {@link #readListColumn} to announce, exactly as in {@link #readListRow}.
     */
    private static Block readListStringDatetimeColumn(
        ListColumnReader input,
        ColumnInfo info,
        int rows,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings,
        @Nullable IntConsumer failedPositionSink,
        NullElementTally tally
    ) {
        int maxDef = info.maxDefLevel();
        DateFormatter dateFormatter = info.dateFormatter();
        boolean skipRow = failedPositionSink != null;
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                input.ensureRowStart();
                boolean open = false;
                boolean rowDropped = false;
                boolean rowDone = false;
                while (rowDone == false) {
                    int elementDef = input.currentDefinitionLevel();
                    if (elementDef < maxDef) {
                        tally.recordIfNullElement(elementDef);
                        input.consumeUndefined();
                    } else {
                        // Always read the value so the data cursor advances, also past a row skip_row drops. The
                        // text render stays inside the try: formatUuid rejects a UUID annotation over anything but
                        // 16 bytes, and that is a per-value failure for the error policy to handle, not a reason to
                        // abandon the read with the cursor un-consumed.
                        Binary raw = input.reader().getBinary();
                        if (rowDropped == false) {
                            try {
                                String value = declaredTextOf(raw, info.logicalType());
                                long millis = DeclaredTypeCoercions.parseDatetimeMillis(value, dateFormatter);
                                if (open == false) {
                                    builder.beginPositionEntry();
                                    open = true;
                                }
                                builder.appendLong(millis);
                            } catch (IllegalArgumentException | DateTimeException e) {
                                if (skipRow) {
                                    DeclaredTypeCoercions.onCoercionFailure(columnName, DataType.KEYWORD, DataType.DATETIME, e, warnings);
                                    failedPositionSink.accept(row);
                                    rowDropped = true;
                                } else {
                                    DeclaredTypeCoercions.onCoercionFailure(
                                        columnName,
                                        DataType.KEYWORD,
                                        DataType.DATETIME,
                                        e,
                                        warnings,
                                        true
                                    );
                                }
                            }
                        }
                        input.consumeAfterRead();
                    }
                    rowDone = input.hasRemaining() == false || input.currentRepetitionLevel() == 0;
                }
                input.rowCompleted();
                if (open) {
                    builder.endPositionEntry();
                } else {
                    // A null list, an empty list, all-null elements, or no element parsed: a null position,
                    // matching readListRow's no-defined-values branch.
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    /**
     * Reads a LIST of {@code DATE_NANOS} elements -- a Parquet INT64 {@code TIMESTAMP(MICROS|NANOS)}, or a
     * {@code TIME(MICROS|NANOS)} whose value is a nanosecond of day -- into a {@code LongBlock}. {@code MICROS}
     * elements whose scaled value would overflow the
     * representable {@code date_nanos} range are dropped from their list (mirroring how the generic list reader
     * already skips null elements); a list whose elements <em>all</em> overflow becomes a null position. A single
     * deduplicated warning is reported when any element was dropped. This uses a lazy {@code beginPositionEntry} so
     * an all-overflow defined list never triggers the empty-position assertion in {@link Block.Builder}.
     *
     * <p>Overflow and null-element drops are announced separately — they are different losses with different
     * remedies — so this loop feeds both {@code anyOverflow} and {@code tally}.
     */
    private static Block readListDateNanosColumn(
        ListColumnReader input,
        ColumnInfo info,
        int rows,
        BlockFactory blockFactory,
        NullElementTally tally,
        @Nullable Consumer<String> informationalWarningSink
    ) {
        int maxDef = info.maxDefLevel();
        LogicalTypeAnnotation logical = info.logicalType();
        boolean micros = isMicrosTemporal(logical);
        boolean[] anyOverflow = { false };
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readDateNanosListRow(input, maxDef, micros, logical, builder, anyOverflow, tally);
            }
            if (anyOverflow[0]) {
                warnTimestampOutOfRange(info, informationalWarningSink);
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
        ListColumnReader input,
        int maxDef,
        boolean micros,
        LogicalTypeAnnotation logical,
        LongBlock.Builder builder,
        boolean[] anyOverflow,
        NullElementTally tally
    ) {
        input.ensureRowStart();
        int def = input.currentDefinitionLevel();
        boolean open = false;
        if (def >= maxDef) {
            open = appendDateNanosElement(input.reader(), logical, micros, builder, false, anyOverflow);
            input.consumeAfterRead();
        } else {
            tally.recordIfNullElement(def);
            input.consumeUndefined();
        }
        while (input.hasRemaining() && input.currentRepetitionLevel() > 0) {
            int elementDef = input.currentDefinitionLevel();
            if (elementDef >= maxDef) {
                open = appendDateNanosElement(input.reader(), logical, micros, builder, open, anyOverflow);
                input.consumeAfterRead();
            } else {
                tally.recordIfNullElement(elementDef);
                input.consumeUndefined();
            }
        }
        input.rowCompleted();
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
        builder.appendLong(convertTemporalToNanos(raw, logical));
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
