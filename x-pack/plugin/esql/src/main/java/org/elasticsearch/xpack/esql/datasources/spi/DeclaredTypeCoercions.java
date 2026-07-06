/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * The single source of truth for declared-type coercion on external datasets: which
 * (physical file type &rarr; declared type) pairs an external reader coerces at read time, how a
 * decoded physical block becomes a declared-type block ({@link #castBlock}), and the one scalar
 * conversion the string &rarr; date pair uses everywhere ({@link #parseDatetimeMillis}).
 *
 * <h2>The one concept: reading a file IS ingesting it</h2>
 * A dataset mapping may declare a column type that differs from the type physically in the file
 * (Hive/Trino-style: the declaration is the table schema, readers coerce toward it). Reading a
 * file value against a declared column is the same operation as indexing a document field
 * against a mapping, so the coercion authority is the field mappers' lenient index-time
 * coercion ({@code "123"} &rarr; {@code long}, {@code long} &rarr; {@code double},
 * {@code string} &rarr; {@code datetime} via the column's declared {@code format}, &hellip;),
 * not ES|QL's query-cast rules. Once a value has been coerced into the declared shape it is an
 * ordinary ES|QL value and query-layer conversions ({@code ::}, {@code TO_*}) apply downstream
 * as usual.
 *
 * <h2>What is coercible — the mapper coercion set</h2>
 * {@link #supports} follows what the field mappers accept at ingest, deviating on the safe
 * (reject) side where a mapper is more permissive than a reader can be faithful to — the date
 * mapper's default format also parses numeric and fractional tokens ({@code epoch_millis} halves
 * a {@code 1.5} token down to a truncated instant), while {@code supports(DOUBLE, DATETIME)} is
 * deliberately {@code false} because a fractional value has no unambiguous epoch reading:
 * <ul>
 *   <li><b>numeric targets</b> ({@code integer}/{@code long}/{@code double}/
 *       {@code unsigned_long}): any numeric or string source, with the exact
 *       {@link NumberFieldMapper.NumberType#parse(Object, boolean) NumberType.parse} semantics
 *       ({@code coerce=true}, the {@code index.mapping.coerce} default: numeric strings parse,
 *       decimals truncate toward zero on whole-number targets, out-of-range throws);</li>
 *   <li><b>string targets</b> ({@code keyword}/{@code text}): any decodable scalar source —
 *       ingest stringifies the token (temporal sources render in the ISO form the default date
 *       format parses back; ip sources render as address text, never the encoded bytes). The
 *       source set is closed over exactly the types the readers can decode a block of (string,
 *       whole-number, double, boolean, temporal, ip) so a pair {@code supports} admits can never
 *       reach a value reader that has no arm for it;</li>
 *   <li><b>{@code boolean}</b>: string sources only ({@code "true"}/{@code "false"} via
 *       {@link Booleans#parseBoolean(String)}, the same strict-token primitive the boolean
 *       mapper delegates to — numbers do not ingest into a boolean field);</li>
 *   <li><b>{@code datetime}</b>: string sources parse via {@link #parseDatetimeMillis} with the
 *       column's declared {@code format} (else the ISO default), whole-number sources
 *       reinterpret as epoch milliseconds (the {@code epoch_millis} half of the default date
 *       format);</li>
 *   <li><b>{@code date_nanos}</b>: string sources parse ISO, {@code datetime} sources widen
 *       millis&rarr;nanos (what an epoch-millis token ingests to in a {@code date_nanos} field;
 *       out-of-nanos-range instants fail per value). Plain whole numbers stay out — a raw long
 *       is ambiguous between a millis and a nanos payload;</li>
 *   <li><b>{@code ip}</b>: string sources only, parsed with the same underlying primitive the ip
 *       mapper delegates to ({@code InetAddresses} parse + the 16-byte doc-values encoding).</li>
 * </ul>
 * {@code NULL}/{@code UNSUPPORTED} physical columns support nothing (the readers cannot decode a
 * value to coerce). An unsupported pair is rejected at resolution with an actionable error;
 * there is no third state — a declared type that cannot be produced from the physical column
 * must never silently read as {@code null}.
 *
 * <h2>Where the conversion runs — same predicate, different timing</h2>
 * <ul>
 *   <li><b>Columnar formats</b> (Parquet, ORC) know the physical type upfront from the file
 *       footer, so {@code ExternalSourceResolver} runs {@link #supports} once at resolution and
 *       fails fast. The readers fuse a handful of pairs directly into their decode loops
 *       ({@link #fusedInDecode}); every other supported pair decodes the column at the file's
 *       own type and coerces it with {@link #castBlock}. Per-value failures (numeric overflow,
 *       an unparseable token) follow the read's {@link ErrorPolicy} the same way the text
 *       readers' parse failures do — the default ({@code null_field}; {@code skip_row} degrades
 *       to it, a columnar batch cannot drop one row) nulls the cell and emits a response
 *       {@code Warning} header, {@code ignore_malformed}-style, while {@code fail_fast} fails
 *       the read on the first bad value. Fused arms and {@link #castBlock} route the failure
 *       through the one {@link #onCoercionFailure} chokepoint so the two paths cannot disagree.
 *       Readers also re-check {@link #supports} per file for a <b>declared</b> column, since a
 *       multi-file glob can drift from the anchor footer; an <b>inferred</b> column may only widen,
 *       so a drifted inferred type null-fills rather than taking this lossy escape (never narrows).</li>
 *   <li><b>Text formats</b> (CSV/TSV, NDJSON) have no physical schema — every value is a string,
 *       so the parse into the declared type <i>is</i> the coercion and a bad token follows the
 *       reader's own per-value error policy. Their declared date {@code format} parse goes
 *       through the same {@link #parseDatetimeMillis} scalar as the columnar
 *       string&rarr;date coercion, so the same token with the same declared format produces the
 *       same instant regardless of which format carried it.</li>
 * </ul>
 *
 * <p>TODO: the columnar-vs-text classification this predicate pairs with
 * ({@code ExternalSourceResolver.FILE_TYPED_FORMATS}) has a standing TODO to move onto the
 * {@code FormatReader} SPI as a capability; if that happens, per-format coercion support belongs
 * on the same capability surface.
 */
public final class DeclaredTypeCoercions {

    private DeclaredTypeCoercions() {}

    /**
     * Whether an external reader can coerce a value physically stored as {@code from} into the
     * declared type {@code to} at read time: exactly the pairs the field mappers coerce at
     * ingest (see the class Javadoc for the set). Equal types trivially return {@code true};
     * {@code NULL} and {@code UNSUPPORTED} always return {@code false} (the readers cannot
     * decode such a column, so there is no value to coerce). This is THE castability predicate:
     * resolution-time rejects consult it directly; the reader-side per-file null-fill validation
     * consults it only for a <b>declared</b> column (an inferred cross-file clash widens-or-nulls,
     * never narrows — see {@code ParquetFormatReader.validatePlannerTypesAgainstFile}), so a lossy
     * narrowing is admitted exactly where a declaration licenses it.
     */
    public static boolean supports(DataType from, DataType to) {
        if (from == to) {
            return true;
        }
        if (from == null || to == null || isDecodable(from) == false || isDecodable(to) == false) {
            return false;
        }
        boolean fromString = from == DataType.KEYWORD || from == DataType.TEXT;
        // Temporal sources decode to epoch longs, so they ingest into numeric targets like longs.
        boolean fromWholeNumber = from == DataType.INTEGER
            || from == DataType.LONG
            || from == DataType.UNSIGNED_LONG
            || from == DataType.DATETIME
            || from == DataType.DATE_NANOS;
        boolean fromNumeric = fromWholeNumber || from == DataType.DOUBLE;
        return switch (to) {
            // Ingest stringifies any scalar token, but the set is closed over the sources the
            // readers can decode a block of (valueReader's arms) — an open-world `true` would
            // admit pairs like version->keyword that then hard-throw inside castBlock.
            case KEYWORD, TEXT -> fromString || fromNumeric || from == DataType.BOOLEAN || from == DataType.IP;
            case LONG, INTEGER, DOUBLE, UNSIGNED_LONG -> fromString || fromNumeric;
            case BOOLEAN -> fromString; // the boolean mapper accepts only true/false tokens, never numbers
            // Epoch-millis reinterpret for whole numbers; a nanos payload is NOT millis, so
            // date_nanos sources don't reinterpret into datetime.
            case DATETIME -> fromString || from == DataType.INTEGER || from == DataType.LONG || from == DataType.UNSIGNED_LONG;
            // String parse, or the millis->nanos widen an epoch-millis token gets when ingested
            // into a date_nanos field (also the cross-file DATETIME + DATE_NANOS unification).
            // Plain whole numbers stay out: a raw long is ambiguous between millis and nanos.
            case DATE_NANOS -> fromString || from == DataType.DATETIME;
            case IP -> fromString;
            default -> false;
        };
    }

    private static boolean isDecodable(DataType type) {
        return type != DataType.NULL && type != DataType.UNSUPPORTED;
    }

    /**
     * The coercible pairs the columnar decode loops implement directly (fused into the decode,
     * no {@link #castBlock} pass): the lossless {@code integer → long} widen, the
     * {@code long → datetime} epoch-millis reinterpret, the {@code string → datetime} parse with
     * the column's declared {@code format}, and the {@code keyword ↔ text} relabel (same bytes).
     * Every other {@link #supports supported} pair decodes at the file's own type and coerces
     * through {@link #castBlock}. Both Parquet and ORC consult this so the two readers cannot
     * disagree about which path a pair takes.
     */
    public static boolean fusedInDecode(DataType from, DataType to) {
        if (from == to) {
            return true;
        }
        if (from == DataType.INTEGER && to == DataType.LONG) {
            return true;
        }
        if (from == DataType.LONG && to == DataType.DATETIME) {
            return true;
        }
        boolean fromString = from == DataType.KEYWORD || from == DataType.TEXT;
        if (fromString && (to == DataType.KEYWORD || to == DataType.TEXT)) {
            return true;
        }
        return fromString && to == DataType.DATETIME;
    }

    /**
     * Coerces a decoded physical-type block into a declared-type block, value by value, with the
     * field mappers' ingest coercion (see the class Javadoc). Preserves nulls and multi-value
     * positions. Does NOT take ownership of {@code source}; the caller closes it. The returned
     * block is a fresh reference the caller owns (for the trivial {@code from == to} case the
     * source is ref-bumped and returned).
     * <p>
     * Per-value failures — numeric overflow, an unparseable token — follow the bulk API's lenient
     * model when {@code warnings} is non-null: the whole position is nulled and one capped
     * response {@code Warning} header records the reason (never a hard read failure, never a
     * silent wrong value). With a {@code null} {@code warnings} sink the coercion is strict and
     * the failure propagates to the caller.
     *
     * @param declaredFormat the column's declared date parse pattern for the string&rarr;datetime
     *                       pair ({@code null} = the ISO default); ignored by every other pair
     * @param columnName     column name used in warning details; may be {@code null} when the
     *                       caller is strict ({@code warnings == null})
     */
    public static Block castBlock(
        Block source,
        DataType from,
        DataType to,
        @Nullable DateFormatter declaredFormat,
        BlockFactory blockFactory,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings
    ) {
        int positions = source.getPositionCount();
        if (from == to) {
            source.incRef();
            return source;
        }
        if (source.areAllValuesNull()) {
            return blockFactory.newConstantNullBlock(positions);
        }
        Function<Object, Object> coercer = scalarCoercer(from, to, declaredFormat);
        IntFunction<Object> read = valueReader(source, from);
        try (Block.Builder builder = builderFor(to, blockFactory, positions)) {
            ValueWriter write = valueWriter(builder, to);
            Object[] scratch = null;
            for (int pos = 0; pos < positions; pos++) {
                int count = source.getValueCount(pos);
                if (source.isNull(pos) || count == 0) {
                    builder.appendNull();
                    continue;
                }
                int first = source.getFirstValueIndex(pos);
                if (count == 1) {
                    Object coerced;
                    try {
                        coerced = coercer.apply(read.apply(first));
                    } catch (IllegalArgumentException | DateTimeException e) {
                        onCoercionFailure(columnName, from, to, e, warnings);
                        builder.appendNull();
                        continue;
                    }
                    write.write(coerced);
                } else {
                    // Coerce the whole position before appending: a failure mid-entry cannot be
                    // rolled back on the builder, and the bulk-API model nulls the field (the
                    // position), not just the offending value.
                    if (scratch == null || scratch.length < count) {
                        scratch = new Object[count];
                    }
                    boolean failed = false;
                    for (int v = 0; v < count && failed == false; v++) {
                        try {
                            scratch[v] = coercer.apply(read.apply(first + v));
                        } catch (IllegalArgumentException | DateTimeException e) {
                            onCoercionFailure(columnName, from, to, e, warnings);
                            failed = true;
                        }
                    }
                    if (failed) {
                        builder.appendNull();
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        write.write(scratch[v]);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    /**
     * The one coercion-failure chokepoint, shared by {@link #castBlock} and the readers' fused
     * decode arms so a failed value behaves identically whichever path decoded it: with a
     * {@code null} {@code warnings} sink (strict, {@code error_mode: fail_fast}) the failure
     * propagates and the read fails; with a live sink the caller nulls the cell/position and one
     * capped response {@code Warning} header records the reason. Callers append the null
     * themselves — this method only decides throw-vs-warn.
     */
    public static void onCoercionFailure(
        @Nullable String columnName,
        DataType from,
        DataType to,
        RuntimeException e,
        @Nullable SkipWarnings warnings
    ) {
        if (warnings == null) {
            throw e;
        }
        warnings.add(
            "Column ["
                + columnName
                + "]: cannot coerce value from ["
                + from.typeName()
                + "] to declared type ["
                + to.typeName()
                + "]: "
                + e.getMessage()
                + "; returning null"
        );
    }

    /**
     * The per-value ingest coercion for a (from, to) pair. Numeric targets run the exact number
     * mapper coercion ({@link NumberFieldMapper.NumberType#parse(Object, boolean) NumberType.parse}
     * with {@code coerce=true}); {@code unsigned_long} mirrors it with the sign-flip block
     * encoding on top; string sources parse into dates via {@link #parseDatetimeMillis} with the
     * declared format, into booleans via the strict mapper token set, into ips via the mapper's
     * doc-values encoding; whole-number sources reinterpret into dates as epoch millis; string
     * targets stringify the token (temporal sources in ISO form).
     */
    private static Function<Object, Object> scalarCoercer(DataType from, DataType to, @Nullable DateFormatter declaredFormat) {
        boolean fromString = from == DataType.KEYWORD || from == DataType.TEXT;
        return switch (to) {
            case KEYWORD, TEXT -> switch (from) {
                case DATETIME -> v -> EsqlDataTypeConverter.dateTimeToString((Long) v);
                case DATE_NANOS -> v -> EsqlDataTypeConverter.nanoTimeToString((Long) v);
                // the token text, as keyword ingest of a scalar sees it; ip sources arrive from
                // the value reader already rendered as address text
                case INTEGER, LONG, UNSIGNED_LONG, DOUBLE, BOOLEAN, KEYWORD, TEXT, IP -> String::valueOf;
                default -> throw new IllegalArgumentException("cannot coerce from [" + from.typeName() + "] blocks");
            };
            case LONG -> v -> NumberFieldMapper.NumberType.LONG.parse(v, true);
            case INTEGER -> v -> NumberFieldMapper.NumberType.INTEGER.parse(v, true);
            case DOUBLE -> v -> NumberFieldMapper.NumberType.DOUBLE.parse(v, true);
            case UNSIGNED_LONG -> DeclaredTypeCoercions::coerceToUnsignedLong;
            case BOOLEAN -> v -> Booleans.parseBoolean((String) v);
            case DATETIME -> fromString
                ? v -> parseDatetimeMillis((String) v, declaredFormat)
                // Whole-number source: epoch-millis reinterpret; the mapper coercion supplies the range check.
                : v -> NumberFieldMapper.NumberType.LONG.parse(v, true).longValue();
            case DATE_NANOS -> {
                if (fromString) {
                    yield v -> EsqlDataTypeConverter.dateNanosToLong((String) v);
                }
                if (from == DataType.DATETIME) {
                    // millis -> nanos widen; DateUtils.toNanoSeconds throws IllegalArgumentException
                    // on pre-epoch and post-2262 instants — the same range rule as TO_DATE_NANOS —
                    // so an unrepresentable instant nulls the cell instead of silently overflowing.
                    yield v -> DateUtils.toNanoSeconds((Long) v);
                }
                throw new IllegalArgumentException(
                    "cannot coerce from [" + from.typeName() + "] to [" + to.typeName() + "]; supports() must gate castBlock callers"
                );
            }
            case IP -> v -> StringUtils.parseIP((String) v);
            default -> throw new IllegalArgumentException(
                "cannot coerce from [" + from.typeName() + "] to [" + to.typeName() + "]; supports() must gate castBlock callers"
            );
        };
    }

    /**
     * The {@code unsigned_long} twin of {@link NumberFieldMapper.NumberType#parse(Object, boolean)
     * NumberType.parse} with {@code coerce=true}: numeric strings parse, decimals truncate toward
     * zero, out-of-[0, 2^64-1]-range throws. Returns the sign-flip block encoding
     * ({@link NumericUtils#asLongUnsigned(BigInteger)}), matching the index path.
     */
    private static long coerceToUnsignedLong(Object value) {
        BigInteger big;
        if (value instanceof BigInteger bigInteger) {
            big = bigInteger;
        } else if (value instanceof Double || value instanceof Float) {
            big = BigDecimal.valueOf(((Number) value).doubleValue()).toBigInteger();
        } else if (value instanceof Number number) {
            big = BigInteger.valueOf(number.longValue());
        } else {
            big = new BigDecimal(value.toString()).toBigInteger();
        }
        if (big.signum() < 0 || NumericUtils.isUnsignedLong(big) == false) {
            throw new IllegalArgumentException("Value [" + value + "] is out of range for an unsigned_long");
        }
        return NumericUtils.asLongUnsigned(big);
    }

    /** Reads one decoded value from a physical-type block in the Java shape the mapper coercions expect. */
    private static IntFunction<Object> valueReader(Block block, DataType from) {
        return switch (from) {
            case INTEGER -> i -> ((IntBlock) block).getInt(i);
            case LONG, DATETIME, DATE_NANOS -> i -> ((LongBlock) block).getLong(i);
            // unsigned_long blocks are sign-flip encoded; decode to the true Number before coercing
            case UNSIGNED_LONG -> i -> NumericUtils.unsignedLongAsNumber(((LongBlock) block).getLong(i));
            case DOUBLE -> i -> ((DoubleBlock) block).getDouble(i);
            case BOOLEAN -> i -> ((BooleanBlock) block).getBoolean(i);
            case KEYWORD, TEXT -> {
                BytesRefBlock bytes = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                yield i -> bytes.getBytesRef(i, scratch).utf8ToString();
            }
            case IP -> {
                // ip blocks hold the mapper's 16-byte encoded form; render the TO_STRING address
                // text so string targets (the only coercion out of ip) carry the address, never
                // the raw encoding bytes.
                BytesRefBlock bytes = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                yield i -> EsqlDataTypeConverter.ipToString(bytes.getBytesRef(i, scratch));
            }
            default -> throw new IllegalArgumentException("cannot coerce from [" + from.typeName() + "] blocks");
        };
    }

    private static Block.Builder builderFor(DataType to, BlockFactory blockFactory, int positions) {
        return switch (to) {
            case INTEGER -> blockFactory.newIntBlockBuilder(positions);
            case LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS -> blockFactory.newLongBlockBuilder(positions);
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(positions);
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(positions);
            case KEYWORD, TEXT, IP -> blockFactory.newBytesRefBlockBuilder(positions);
            default -> throw new IllegalArgumentException("cannot coerce into [" + to.typeName() + "] blocks");
        };
    }

    private interface ValueWriter {
        void write(Object coerced);
    }

    /**
     * Appends one coerced value in the declared type's block encoding. The scalar coercers
     * already produce block-ready shapes: {@link Number}s for numeric targets (the
     * {@code unsigned_long} coercer returns the sign-flip encoding), epoch longs for temporal
     * targets, {@link String}s for string targets, and pre-encoded {@link BytesRef}s for ip.
     */
    private static ValueWriter valueWriter(Block.Builder builder, DataType to) {
        return switch (to) {
            case INTEGER -> v -> ((IntBlock.Builder) builder).appendInt(((Number) v).intValue());
            case LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS -> v -> ((LongBlock.Builder) builder).appendLong(((Number) v).longValue());
            case DOUBLE -> v -> ((DoubleBlock.Builder) builder).appendDouble(((Number) v).doubleValue());
            case BOOLEAN -> v -> ((BooleanBlock.Builder) builder).appendBoolean((Boolean) v);
            case KEYWORD, TEXT, IP -> v -> ((BytesRefBlock.Builder) builder).appendBytesRef(
                v instanceof BytesRef bytes ? bytes : new BytesRef(v.toString())
            );
            default -> throw new IllegalArgumentException("cannot coerce into [" + to.typeName() + "] blocks");
        };
    }

    /**
     * The one string&rarr;datetime conversion for declared date columns, shared by every reader:
     * the text readers' declared-{@code format} parse (CSV/TSV, NDJSON) and the columnar readers'
     * string&rarr;datetime coercion (Parquet, ORC) all route here, so identical input bytes with
     * an identical declared format produce the identical epoch instant regardless of file format.
     * <p>
     * With a declared format the parse is strict and zone-aware ({@link DateFormatter#parseMillis}
     * defaults a missing zone to UTC) — the same parse the date field mapper runs against its
     * mapping's {@code format} at ingest; without one it falls back to
     * {@link EsqlDataTypeConverter#dateTimeToLong(String)} — ISO
     * ({@code strict_date_optional_time}) semantics. Throws {@link IllegalArgumentException} on an
     * unparseable value; callers decide whether that is a per-row error (text error policy), a
     * nulled cell with a response Warning ({@link #castBlock} with a warnings sink), or a hard
     * failure.
     */
    public static long parseDatetimeMillis(String value, @Nullable DateFormatter declaredFormat) {
        return EsqlDataTypeConverter.dateTimeToLong(value, declaredFormat);
    }
}
