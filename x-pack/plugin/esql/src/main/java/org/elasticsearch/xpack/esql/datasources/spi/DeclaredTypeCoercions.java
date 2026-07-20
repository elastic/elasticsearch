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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.type.Converter;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * The single source of truth for declared-type coercion on external datasets: which
 * (physical file type &rarr; declared type) pairs an external reader coerces at read time, how a
 * decoded physical block becomes a declared-type block ({@link #castBlock}), the block shape a
 * declared type reads into ({@link #elementTypeFor}/{@link #builderFor}, which every text and
 * columnar reader consults instead of re-deriving it), and the one scalar conversion the
 * string &rarr; date pair uses everywhere ({@link #parseDatetimeMillis}).
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
 * (reject) side where a mapper is more permissive than a reader can be faithful to. A
 * {@code double} source coerces into {@code datetime} the same way {@code ::datetime} treats a
 * double — with no declared format the value rounds to the nearest epoch millisecond; with an
 * {@code epoch_second} / calendar {@code format} it parses through that format to sub-second
 * precision (double's ~15-16 significant digits bound the resolution, not the parser):
 * <ul>
 *   <li><b>whole-number targets</b> ({@code integer}/{@code long}): any numeric or string source,
 *       reusing the ES|QL {@code ::} cast engine ({@link #numericCoercer}) so a declared read is
 *       value-identical to an explicit {@code ::long}/{@code ::integer} — numeric strings parse
 *       (fractional and scientific accepted), the result <b>rounds</b> (not truncates), out-of-range
 *       throws {@link InvalidArgumentException}. {@code unsigned_long} keeps its {@code ::}-faithful
 *       {@link #coerceToUnsignedLong} twin (truncates toward zero, matching {@code ::unsigned_long});
 *       {@code double} parses with {@code Double.parseDouble} and returns the IEEE value as-is
 *       ({@code NaN}/{@code Infinity} pass through, matching the native columnar double read and CSV —
 *       an external read preserves the file's value; the mapper's finite-only rule is an index-time
 *       concern, not a read one);</li>
 *   <li><b>string targets</b> ({@code keyword}/{@code text}): any decodable scalar source —
 *       ingest stringifies the token (temporal sources render in the ISO form the default date
 *       format parses back; ip sources render as address text, never the encoded bytes). The
 *       source set is closed over exactly the types the readers can decode a block of (string,
 *       whole-number, double, boolean, temporal, ip) so a pair {@code supports} admits can never
 *       reach a value reader that has no arm for it;</li>
 *   <li><b>{@code boolean}</b>: string sources only, parsed strictly and case-insensitively
 *       ({@link #strictParseBoolean}: only {@code true}/{@code false} in any case; every other token
 *       fails loudly). This deliberately diverges from {@code ::boolean}, which maps a non-{@code true}
 *       token silently to {@code false} — a silent wrong answer this read must not introduce;</li>
 *   <li><b>{@code datetime}</b>: string sources parse via {@link #parseDatetimeMillis} with the
 *       column's declared {@code format} (else the ISO default). A {@code date_nanos} source narrows
 *       nanos&rarr;millis ({@link DateUtils#toMilliSeconds}, truncating sub-millisecond precision) —
 *       the narrowing {@code ::datetime} performs. This is what lets an annotated
 *       {@code TIMESTAMP(MICROS|NANOS)} column (which infers as {@code date_nanos}) be declared
 *       {@code date}, the conventional dashboard type. Numeric sources follow the unit rule below
 *       ({@code date} = millis when no format is declared; a {@code double} rounds to the nearest
 *       milli, the {@code ::datetime} semantic);</li>
 *   <li><b>{@code date_nanos}</b>: string sources parse via the column's declared {@code format}
 *       (else the ISO nanos default), {@code datetime} sources widen millis&rarr;nanos (what an
 *       epoch-millis token ingests to in a {@code date_nanos} field; out-of-nanos-range instants
 *       fail per value). Numeric sources follow the unit rule below ({@code date_nanos} = nanos when
 *       no format is declared, matching the shipped CSV inline-schema numeric read). A negative epoch
 *       has no {@code date_nanos} representation (the {@code TO_DATE_NANOS} range rule) and fails per
 *       value — never a negative nanos long;</li>
 *   <li><b>{@code ip}</b>: string sources only, parsed with the same underlying primitive the ip
 *       mapper delegates to ({@code InetAddresses} parse + the 16-byte doc-values encoding).</li>
 * </ul>
 *
 * <h2>The unit rule: what a raw number in a temporal column means</h2>
 * A raw whole number carries no unit — the file says nothing, so the declaration must. One rule
 * decides it, and it is the same rule for every temporal target:
 * <ol>
 *   <li><b>The file's annotation wins.</b> A physically-typed temporal column (a parquet
 *       {@code TIMESTAMP(unit)}) is already an instant, not a raw number: its unit is known, a
 *       {@code format} on it is rejected at resolution rather than obeyed, and the declaration only
 *       chooses the target precision (nanos&rarr;millis narrows, millis&rarr;nanos widens).</li>
 *   <li><b>Else the declared {@code format} wins</b>, naming the unit / parse dialect of the number
 *       ({@code epoch_second} reads seconds, {@code yyyyMMdd} reads {@code 20260101} as a calendar
 *       date) — the semantic the CSV/NDJSON readers already apply to a numeric token.</li>
 *   <li><b>Else the declared type names the unit</b>: {@code datetime} = milliseconds,
 *       {@code date_nanos} = nanoseconds. This is the identity read — the number is assumed to be
 *       already in the type's own storage unit, so nothing is scaled.</li>
 * </ol>
 * The type always fixes what is <i>stored</i> ({@code datetime} is a millis long, {@code date_nanos}
 * a nanos long); the format only says what was <i>given</i>. So {@code {date, epoch_second}} still
 * stores millis — it scales the input, it does not make a "seconds column".
 * <p>
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
 *       readers' parse failures do — the base default {@code fail_fast} ({@link ErrorPolicy#STRICT})
 *       fails the read on the first bad value, while the opt-in {@code null_field} mode
 *       ({@code skip_row} degrades to it, a columnar batch cannot drop one row) nulls the cell and
 *       emits a response {@code Warning} header, {@code ignore_malformed}-style. Fused arms and
 *       {@link #castBlock} route the failure
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
            // Numeric sources follow the unit rule (class Javadoc): the format names the unit when
            // declared, else the type does — datetime = millis. A double rounds to epoch millis (the
            // ::datetime semantic). A date_nanos source narrows nanos -> millis: it is not a raw
            // number whose unit is unknown but an instant the file already typed, so the conversion
            // is unambiguous — the same narrowing ::datetime performs.
            case DATETIME -> fromString
                || from == DataType.INTEGER
                || from == DataType.LONG
                || from == DataType.UNSIGNED_LONG
                || from == DataType.DOUBLE
                || from == DataType.DATE_NANOS;
            // String parse, the millis->nanos widen an epoch-millis token gets when ingested into a
            // date_nanos field (also the cross-file DATETIME + DATE_NANOS unification), or a numeric
            // source under the same unit rule — the format names the unit when declared, else the
            // type does: date_nanos = nanos, matching the CSV inline-schema numeric read.
            case DATE_NANOS -> fromString
                || from == DataType.DATETIME
                || from == DataType.INTEGER
                || from == DataType.LONG
                || from == DataType.UNSIGNED_LONG;
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
     * {@code long → datetime} epoch-millis reinterpret <b>of a column with no declared format</b>, the
     * {@code string → datetime} parse with the column's declared {@code format}, and the
     * {@code keyword ↔ text} relabel (same bytes). Every other {@link #supports supported} pair decodes at
     * the file's own type and coerces through {@link #castBlock}.
     * <p>
     * This no-format overload is the plain predicate; the readers call
     * {@link #fusedInDecode(DataType, DataType, boolean)} so that a format-carrying whole-number column
     * defuses onto {@code castBlock}. Both Parquet and ORC consult that one predicate, so the two readers
     * cannot disagree about which path a pair takes.
     */
    public static boolean fusedInDecode(DataType from, DataType to) {
        return fusedInDecode(from, to, false);
    }

    /**
     * The {@link #fusedInDecode(DataType, DataType)} predicate, aware of whether the column carries a
     * declared date {@code format}. The {@code long → datetime} epoch-millis reinterpret is fused only
     * when there is <b>no</b> format: the fused loop cannot honor one, so a format-carrying whole-number
     * column defuses onto {@link #castBlock}, whose {@code DATETIME} arm parses the value through the
     * format (the epoch-unit / parse-dialect semantic the text readers already implement). The
     * {@code string → datetime} pair stays fused either way — its fused BINARY decode arm already threads
     * the declared formatter.
     */
    public static boolean fusedInDecode(DataType from, DataType to, boolean hasDeclaredFormat) {
        if (from == to) {
            return true;
        }
        if (from == DataType.INTEGER && to == DataType.LONG) {
            return true;
        }
        if (from == DataType.LONG && to == DataType.DATETIME) {
            return hasDeclaredFormat == false;
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
     * @param declaredFormat the column's declared date parse pattern, consumed by the temporal targets: it is the
     *                       parse pattern for a string source, and the epoch unit / parse dialect for a numeric
     *                       source into {@code datetime} ({@code null} = the ISO default for a string, the
     *                       epoch-millis reinterpret for a number). Ignored by the non-temporal pairs
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
                    } catch (IllegalArgumentException | DateTimeException | InvalidArgumentException e) {
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
                        } catch (IllegalArgumentException | DateTimeException | InvalidArgumentException e) {
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
     * doc-values encoding; a numeric source reinterprets into a date as epoch millis when the column
     * declares no format (a {@code double} rounding to the nearest milli, the {@code ::datetime}
     * semantic) and otherwise parses THROUGH the declared format, which is then the epoch unit /
     * parse dialect; string targets stringify the token (temporal sources in ISO form).
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
            case LONG, INTEGER -> numericCoercer(from, to);
            // double returns the IEEE value the token names: NaN / +Infinity / -Infinity pass through as
            // their double, matching the NATIVE Parquet/ORC double read (raw appendDouble, no finite
            // check) and the CSV Double.parseDouble path. We deliberately do NOT use
            // NumberType.DOUBLE.parse / ::double here: their finite-only rule ("[double] supports only
            // finite values") is an index-time constraint so range/sort behave on an indexed field, not
            // a read constraint — reading an external data lake preserves the value the file holds, and
            // the string-coercion arm was the lone path rejecting non-finite doubles. A string source
            // parses with Double.parseDouble (accepts NaN/Infinity, still rejects garbage via NFE); a
            // numeric source (declared double over a physical int/long/unsigned_long) widens.
            case DOUBLE -> fromString ? v -> Double.parseDouble((String) v) : v -> ((Number) v).doubleValue();
            case UNSIGNED_LONG -> DeclaredTypeCoercions::coerceToUnsignedLong;
            case BOOLEAN -> v -> strictParseBoolean((String) v);
            case DATETIME -> {
                if (fromString) {
                    yield v -> parseDatetimeMillis((String) v, declaredFormat);
                }
                if (from == DataType.DATE_NANOS) {
                    // An annotated TIMESTAMP(MICROS|NANOS) column infers as date_nanos; declaring it `date` narrows
                    // nanos -> millis, truncating sub-millisecond precision. Value-identical to ::datetime, which maps
                    // DATE_NANOS through this same DateUtils.toMilliSeconds (ToDatetime's DATE_NANOS evaluator), so a
                    // declared read and an explicit cast cannot disagree. A pre-epoch nanos instant throws and follows
                    // the read's error policy. No format is involved: the annotation already states the unit.
                    yield v -> DateUtils.toMilliSeconds((Long) v);
                }
                if (from == DataType.DOUBLE) {
                    // A declared double column reads as an epoch value the same way ::datetime treats a double
                    // (ToDatetime maps DOUBLE via ToLong.fromDouble == safeDoubleToLong): with no format the value IS
                    // epoch millis and its fractional part rounds; with a format the value is parsed through it
                    // (epoch_second reads fractional seconds to sub-second precision). Non-finite values follow
                    // safeDoubleToLong exactly as the cast engine does — NaN rounds to epoch 0, an infinity throws
                    // out-of-range — so a declared read and an explicit ::datetime never disagree. String.valueOf(double)
                    // renders scientific notation at magnitudes >= 1e7, which the epoch formatters reject, so the
                    // format branch renders plain-decimal.
                    // epoch_millis means the number IS already epoch-millis — the identity — so on a double it must
                    // ROUND exactly as the format-free path does, not truncate the fraction through a millis parse.
                    // declaredEpochFormatScale reports scale 1 for that case (and only that case); every other format
                    // (epoch_second's fractional seconds, calendar) keeps its parse.
                    boolean identityFormat = declaredFormat != null
                        && Long.valueOf(1L).equals(declaredEpochFormatScale(declaredFormat.pattern(), DataType.DATETIME));
                    yield declaredFormat != null && identityFormat == false
                        ? v -> parseDatetimeMillis(BigDecimal.valueOf((Double) v).toPlainString(), declaredFormat)
                        : v -> DataTypeConverter.safeDoubleToLong((Double) v);
                }
                if (declaredFormat != null) {
                    // Whole-number source WITH a declared format: the format is the parse dialect / epoch unit,
                    // exactly as the text readers already treat it (NdJsonPageDecoder.decodeDatetimeValue,
                    // CsvFormatReader.tryParseDatetime): epoch_second reads seconds, yyyyMMdd reads 20260101.
                    yield v -> parseDatetimeMillis(String.valueOf(v), declaredFormat);
                }
                // Whole-number source, no format: epoch-millis reinterpret; the mapper coercion supplies the range check.
                yield v -> NumberFieldMapper.NumberType.LONG.parse(v, true).longValue();
            }
            case DATE_NANOS -> {
                if (fromString) {
                    // Mirrors the DATETIME arm above: the declared format, when the column has one, is the parse
                    // dialect. Without it dateNanosToLong falls back to ISO-8601.
                    yield v -> EsqlDataTypeConverter.dateNanosToLong((String) v, declaredFormat);
                }
                if (from == DataType.DATETIME) {
                    // millis -> nanos widen; DateUtils.toNanoSeconds throws IllegalArgumentException
                    // on pre-epoch and post-2262 instants — the same range rule as TO_DATE_NANOS —
                    // so an unrepresentable instant nulls the cell instead of silently overflowing.
                    yield v -> DateUtils.toNanoSeconds((Long) v);
                }
                if (from == DataType.INTEGER || from == DataType.LONG || from == DataType.UNSIGNED_LONG) {
                    if (declaredFormat != null) {
                        // The unit rule, same as the DATETIME arm: a declared format names the unit / parse
                        // dialect of the number, overriding the type's default. Without this a column declared
                        // {date_nanos, format: epoch_second} would silently reinterpret seconds as nanos.
                        yield v -> EsqlDataTypeConverter.dateNanosToLong(String.valueOf(v), declaredFormat);
                    }
                    // No format: identity epoch-NANOS reinterpret — the declared type names the unit. A
                    // negative epoch has no date_nanos representation (the TO_DATE_NANOS range rule), so it
                    // fails per value through onCoercionFailure rather than ever emitting a negative nanos
                    // long. An unsigned_long source arrives from valueReader as the true Number
                    // (unsignedLongAsNumber), so a magnitude >= 2^63 longValue()s with bit 63 set — negative
                    // — and the same domain check rejects it; a wrapped positive cannot leak.
                    yield v -> {
                        long nanos = ((Number) v).longValue();
                        if (nanos < 0) {
                            throw new IllegalArgumentException("Value [" + v + "] is out of range for a date_nanos epoch-nanoseconds read");
                        }
                        return nanos;
                    };
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
     * The per-value coercion into a whole-number target ({@code long}/{@code integer}), reusing the
     * ES|QL {@code ::} cast engine so a declared read produces the identical value to an explicit
     * {@code ::long} / {@code ::integer}. A string source runs the same {@link EsqlDataTypeConverter}
     * string parse the cast uses (fractional and scientific tokens accepted; the result <b>rounds</b>
     * via {@code safeDoubleToLong}/{@code safeToInt}); a numeric source runs
     * {@link DataTypeConverter#converterFor(DataType, DataType)}, the same core converter the cast
     * dispatches to (so {@code double -> long} rounds, not truncates). Both throw
     * {@link InvalidArgumentException} on an unparseable/overflowing value, which {@link #castBlock}
     * routes through {@link #onCoercionFailure} (warn+null or fail-fast). Unlike the former
     * {@code NumberType.parse} path this is not the ingest coercion — it is the query cast, which is
     * what a user comparing a declared read to {@code ::} expects. ({@code double} is not routed here:
     * it has no rounding divergence, and it must return non-finite IEEE values — {@code NaN}/
     * {@code Infinity} — which {@code ::double} rejects; its arm uses {@code Double.parseDouble}.)
     */
    private static Function<Object, Object> numericCoercer(DataType from, DataType to) {
        if (from == DataType.KEYWORD || from == DataType.TEXT) {
            return switch (to) {
                case LONG -> v -> EsqlDataTypeConverter.stringToLong((String) v);
                case INTEGER -> v -> EsqlDataTypeConverter.stringToInt((String) v);
                default -> throw new IllegalArgumentException("numericCoercer handles long/integer, not [" + to.typeName() + "]");
            };
        }
        if (from == DataType.DATETIME || from == DataType.DATE_NANOS) {
            // Temporal sources arrive from valueReader as a raw epoch Long (millis for datetime, nanos for
            // date_nanos), NOT the ZonedDateTime the :: cast engine's DATETIME converter expects — and
            // DataTypeConverter has no DATE_NANOS numeric converter at all. Coerce the epoch value directly,
            // matching TO_LONG(datetime)/TO_INTEGER(datetime): identity to long, range-checked narrow to int
            // (a real epoch overflows int, so the narrow warn+nulls via onCoercionFailure like any overflow).
            return switch (to) {
                case LONG -> v -> ((Number) v).longValue();
                case INTEGER -> v -> DataTypeConverter.safeToInt(((Number) v).longValue());
                default -> throw new IllegalArgumentException("numericCoercer handles long/integer, not [" + to.typeName() + "]");
            };
        }
        Converter converter = DataTypeConverter.converterFor(from, to);
        if (converter == null) {
            throw new IllegalArgumentException(
                "no cast converter from [" + from.typeName() + "] to [" + to.typeName() + "]; supports() must gate castBlock callers"
            );
        }
        return converter::convert;
    }

    /**
     * Strict, case-insensitive boolean parse for a declared {@code boolean} read. Accepts only
     * {@code true}/{@code false} (any case) and fails every other token through
     * {@link #onCoercionFailure} (warn+null or fail-fast). This deliberately diverges from
     * {@code ::boolean} ({@link EsqlDataTypeConverter#stringToBoolean}, which maps every non-{@code true}
     * token — {@code "yes"}, {@code "1"}, a typo — silently to {@code false}): a silent {@code false} on
     * a bad boolean token is exactly the wrong-answer class this feature must not introduce, so the
     * read-time coercion rejects the token loudly instead. The numeric arms still reuse {@code ::}
     * verbatim; only boolean is stricter, and by design.
     */
    public static boolean strictParseBoolean(String value) {
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new InvalidArgumentException("Cannot parse [{}] as boolean; expected [true] or [false]", value);
    }

    /**
     * The {@code unsigned_long} twin of {@link NumberFieldMapper.NumberType#parse(Object, boolean)
     * NumberType.parse} with {@code coerce=true}: numeric strings parse, decimals truncate toward
     * zero, out-of-[0, 2^64-1]-range throws. Returns the sign-flip block encoding
     * ({@link NumericUtils#asLongUnsigned(BigInteger)}), matching the index path.
     * <p>
     * Public for the same reason as {@link #strictParseBoolean} and {@link #parseDatetimeMillis}: the text
     * readers (CSV/TSV, NDJSON) decode a token to a {@link String} or {@link Number} themselves and then
     * delegate the declared-type conversion here, so a declared {@code unsigned_long} produces the identical
     * block encoding regardless of file format.
     */
    public static long coerceToUnsignedLong(Object value) {
        BigInteger big;
        try {
            if (value instanceof BigInteger bigInteger) {
                big = bigInteger;
            } else if (value instanceof Double || value instanceof Float) {
                big = BigDecimal.valueOf(((Number) value).doubleValue()).toBigInteger();
            } else if (value instanceof Number number) {
                big = BigInteger.valueOf(number.longValue());
            } else {
                big = new BigDecimal(value.toString()).toBigInteger();
            }
        } catch (ArithmeticException e) {
            // BigDecimal.toBigInteger() throws (rather than returning a value we could range-check) when the
            // decimal exponent is large enough that materializing the integer would overflow BigInteger's supported
            // range -- e.g. "1e999999999", or "1e-999999999" which mathematically truncates to 0 but cannot be
            // computed to get there. Such a token cannot be materialized and so cannot be range-checked; treat it as
            // the same failure as any other unrepresentable value and reach callers as the same exception type. It is
            // remapped here, at the one place that parses, rather than in each caller's catch clause: an
            // ArithmeticException escaping this method would bypass every reader's per-cell error policy and hard-
            // fail the whole read, which is exactly the class of failure declared unsigned_long support removes.
            throw new IllegalArgumentException("Value [" + value + "] is out of range for an unsigned_long", e);
        }
        if (NumericUtils.isUnsignedLong(big) == false) {  // isUnsignedLong already rejects negatives (signum >= 0)
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

    /**
     * The {@link DataType} &rarr; {@link ElementType} authority for the declared-read path: every format reader that
     * materializes a declared or projected column into a block resolves its shape here. The <em>enumeration</em> is
     * not repeated: it is delegated to {@link PlannerUtils#toElementType}, the engine-wide mapping, so the
     * declared-read path holds no second {@code DataType} switch over block shape. What this method adds is the
     * declared-read <em>guard</em>, expressed on the {@link ElementType} axis rather than as a second {@code DataType}
     * enumeration: the scalar shapes this SPI's {@link #valueWriter} knows how to append, plus {@link ElementType#NULL}.
     * <p>
     * Expressing the guard on the shape axis is what keeps the readers honest. A future declarable type inherits its
     * block shape automatically, while a {@code DOC}/{@code COMPOSITE}/{@code AGGREGATE_METRIC_DOUBLE}-shaped type
     * still fails loudly at page setup instead of silently building the wrong block. Format readers must call this
     * rather than re-deriving the mapping locally — the reader-local copies are exactly how {@code unsigned_long}
     * came to be accepted at dataset-create time and then unreadable at query time.
     *
     * @throws IllegalArgumentException if the type has no block shape this SPI can write. This is the only exception
     *         this method throws: {@link PlannerUtils#toElementType} signals its own unmappable types with an
     *         {@code EsqlIllegalArgumentException}, which is <em>not</em> an {@code IllegalArgumentException}, so it
     *         is remapped here. Callers therefore need one catch clause, and the contract matches the per-reader
     *         mappings this method replaced.
     */
    public static ElementType elementTypeFor(DataType type) {
        final ElementType elementType;
        try {
            elementType = PlannerUtils.toElementType(type);
        } catch (EsqlIllegalArgumentException e) {
            throw new IllegalArgumentException("type [" + type.typeName() + "] is not readable as a declared column", e);
        }
        return switch (elementType) {
            case BOOLEAN, INT, LONG, DOUBLE, BYTES_REF, NULL -> elementType;
            default -> throw new IllegalArgumentException("type [" + type.typeName() + "] is not readable as a declared column");
        };
    }

    /**
     * The block builder for a declared column's target type, derived from {@link #elementTypeFor} so the builder and
     * the {@link ElementType} a reader projects can never disagree.
     */
    public static Block.Builder builderFor(DataType to, BlockFactory blockFactory, int positions) {
        return elementTypeFor(to).newBlockBuilder(positions, blockFactory);
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
     * The format may be a column's declared {@code format} or a reader's file-level {@code datetime_format} — this is
     * the shared string&rarr;datetime conversion, not a declared-only one.
     * <p>
     * With a format the parse is strict and zone-aware ({@link DateFormatter#parseMillis}
     * defaults a missing zone to UTC) — the same parse the date field mapper runs against its
     * mapping's {@code format} at ingest; without one it falls back to
     * {@link EsqlDataTypeConverter#dateTimeToLong(String)} — ISO
     * ({@code strict_date_optional_time}) semantics. Throws {@link IllegalArgumentException} on an
     * unparseable value; callers decide whether that is a per-row error (text error policy), a
     * nulled cell with a response Warning ({@link #castBlock} with a warnings sink), or a hard
     * failure.
     */
    public static long parseDatetimeMillis(String value, @Nullable DateFormatter format) {
        return EsqlDataTypeConverter.dateTimeToLong(value, format);
    }

    /**
     * How a column's DECODED value relates to the RAW value the file's statistics hold — the single question every
     * pruning decision on an external read has to answer, in one place.
     *
     * <p>Row-group statistics, page indexes, dictionary and bloom filters, and the TopN threshold all compare a
     * decoded-domain value against raw file bytes. That comparison is only valid if you know the map between them.
     * Historically each consumer re-derived it from whatever it happened to have in scope — an annotation here, a
     * primitive type there, and after this PR a declared format that none of them could see. Nine derivations of one
     * rule is why a unit skew keeps reappearing in a different consumer each time it is fixed.
     *
     * <p>The map has a direction, and the direction is NOT a property of the file alone. A {@code TIMESTAMP(MICROS)}
     * column decodes to nanos ({@code raw x 1000}) when it infers {@code date_nanos}, and to millis
     * ({@code raw / 1000}, truncating) when it is declared {@code date}. Same bytes, opposite maps, chosen by the
     * declaration. That is why this takes the declared type, and why a per-consumer switch on the annotation cannot
     * express it.
     */
    public sealed interface RawDecodeRelation {
        /** decoded == raw. Statistics are directly comparable. */
        record Identity() implements RawDecodeRelation {}

        /** decoded == raw * factor, exactly. */
        record ScaleUp(long factor) implements RawDecodeRelation {}

        /**
         * decoded == floorDiv(raw, divisor) — so ONE decoded value covers a BAND of {@code divisor} raw values.
         * Every bound conversion must account for the band, or a {@code <=} / {@code ==} prunes the rows in it.
         */
        record ScaleDown(long divisor) implements RawDecodeRelation {}
    }

    /**
     * The RAW bound to push so the pushed predicate is never STRICTER than the true one, or {@code null} to decline.
     *
     * <p>Direction matters and is asymmetric: a bound that is too LOOSE is harmless (the reader over-includes and
     * {@code FilterExec} re-checks), while a bound that is too STRICT prunes row groups the query matches — and a
     * pruned group is never decoded, so nothing downstream can recover it. Every rule below therefore rounds
     * OUTWARD, and declines rather than guess.
     *
     * <p>{@code EQ} is NOT answered here: a lossy decode makes equality a BAND, which no single raw bound expresses,
     * so callers route equality through {@link #rawEqualityBand} instead. Passing {@code EQ} is a caller contract
     * violation and throws.
     *
     * @param relation how decode relates the raw value to the decoded one
     * @param bound the query literal, in the DECODED domain
     * @param op the comparison the bound belongs to (any {@link BoundOp} except {@link BoundOp#EQ})
     */
    @Nullable
    public static Long rawBoundFor(RawDecodeRelation relation, long bound, BoundOp op) {
        return switch (relation) {
            case RawDecodeRelation.Identity ignored -> switch (op) {
                case GT, GTE, LT, LTE, NOT_EQ -> bound;
                case EQ -> throw eqRoutesThroughBand();
            };
            case RawDecodeRelation.ScaleUp up -> {
                long f = up.factor();
                // The bound arrives in the DECODED domain, so inverting `decoded == raw * f` DIVIDES it back down.
                // Round outward so the pushed predicate never excludes a raw value whose decode matches: floorDiv
                // for the ops whose truth set opens upward, ceilDiv for those that open downward.
                yield switch (op) {
                    // decoded > bound <=> raw * f > bound <=> raw > floorDiv(bound, f)
                    case GT -> Math.floorDiv(bound, f);
                    // decoded >= bound <=> raw >= ceilDiv(bound, f)
                    case GTE -> Math.ceilDiv(bound, f);
                    // decoded < bound <=> raw < ceilDiv(bound, f)
                    case LT -> Math.ceilDiv(bound, f);
                    // decoded <= bound <=> raw <= floorDiv(bound, f)
                    case LTE -> Math.floorDiv(bound, f);
                    // NOT_EQ inverts the same raw point the caller builds notEq from: only an exact multiple of f is
                    // reachable, so a non-multiple != is always true and declines.
                    case NOT_EQ -> bound % f == 0 ? bound / f : null;
                    case EQ -> throw eqRoutesThroughBand();
                };
            }
            case RawDecodeRelation.ScaleDown down -> {
                long d = down.divisor();
                try {
                    long base = Math.multiplyExact(bound, d);
                    // decoded == floorDiv(raw, d), so decoded == bound over the raw band [base, base + d - 1].
                    yield switch (op) {
                        // decoded >= bound <=> raw >= base (exact)
                        case GTE -> base;
                        // decoded < bound <=> raw < base (exact)
                        case LT -> base;
                        // decoded <= bound <=> raw <= base + d - 1 (the TOP of the band, not its floor)
                        case LTE -> Math.addExact(base, d - 1);
                        // decoded > bound <=> raw >= base + d <=> raw > base + d - 1. The EXACT bound, not the
                        // looser gt(base): a loose GT is safe pushed directly but becomes stricter-than-truth once
                        // wrapped in NOT (which the translator admits), pruning matching rows.
                        case GT -> Math.addExact(base, d - 1);
                        // NOT_EQ is the band's complement — not a single predicate — so decline.
                        case NOT_EQ -> null;
                        case EQ -> throw eqRoutesThroughBand();
                    };
                } catch (ArithmeticException e) {
                    yield null;
                }
            }
        };
    }

    private static AssertionError eqRoutesThroughBand() {
        return new AssertionError("EQ is a band under a lossy decode; callers answer it through rawEqualityBand, not rawBoundFor");
    }

    /**
     * A raw file statistic mapped INTO the decoded domain, or {@code null} to decline. The forward direction, for
     * consumers that compare a decoded bound against raw min/max (the TopN threshold skip, stripe skip). The map is
     * monotone, so a min stays a min and a max stays a max — no rounding, no band. Overflow declines, which for a
     * skip decision means "do not skip": correct, just not pruned.
     */
    @Nullable
    public static Long rawStatToDecoded(RawDecodeRelation relation, long raw) {
        return switch (relation) {
            case RawDecodeRelation.Identity ignored -> raw;
            case RawDecodeRelation.ScaleUp up -> {
                try {
                    yield Math.multiplyExact(raw, up.factor());
                } catch (ArithmeticException e) {
                    yield null;
                }
            }
            case RawDecodeRelation.ScaleDown down -> Math.floorDiv(raw, down.divisor());
        };
    }

    /** The inclusive raw range a single decoded value covers. A point for an exact map, a band for a lossy one. */
    public record RawBand(long lo, long hi) {}

    /**
     * The inclusive RAW range whose values all decode to {@code bound}, or {@code null} when none exists.
     *
     * <p>Equality is the one comparison a lossy decode cannot answer with a single number: when decode truncates,
     * one decoded value covers a whole band of raw values, and pushing any single one of them prunes the rest —
     * rows that genuinely match. Callers that can express a range should push this band and keep their pruning;
     * callers that can only test set membership (dictionary and bloom filters) must decline instead.
     *
     * <p>Returns a degenerate one-value band for exact maps, so callers need no special case.
     */
    @Nullable
    public static RawBand rawEqualityBand(RawDecodeRelation relation, long bound) {
        return switch (relation) {
            case RawDecodeRelation.Identity ignored -> new RawBand(bound, bound);
            case RawDecodeRelation.ScaleUp up -> {
                // decoded == raw * f is reachable only on multiples of f; any other literal matches nothing stored.
                if (bound % up.factor() != 0) {
                    yield null;
                }
                long raw = bound / up.factor();
                yield new RawBand(raw, raw);
            }
            case RawDecodeRelation.ScaleDown down -> {
                try {
                    long base = Math.multiplyExact(bound, down.divisor());
                    // floorDiv(raw, d) == bound <=> raw in [base, base + d - 1]
                    yield new RawBand(base, Math.addExact(base, down.divisor() - 1));
                } catch (ArithmeticException e) {
                    yield null;
                }
            }
        };
    }

    /** The comparisons a raw bound can be pushed for. */
    public enum BoundOp {
        EQ,
        NOT_EQ,
        GT,
        GTE,
        LT,
        LTE
    }

    /** Pure epoch dialects: the only declared formats whose whole-number decode is an exact linear scale. */
    private static final String EPOCH_MILLIS_PATTERN = "epoch_millis";
    private static final String EPOCH_SECOND_PATTERN = "epoch_second";

    /**
     * The exact linear scale of a whole-number decode through a declared date {@code format}, in units of the
     * declared type's storage unit per raw input tick — or {@code null} when the format is not a pure epoch dialect.
     *
     * <p>This is the stats-side counterpart of {@link #scalarCoercer}'s declared-format arm: because
     * {@code decode(v) == v * scale} exactly, a consumer holding a bound in the DECODED unit can convert it back to
     * the file's RAW unit and keep comparing against raw statistics. Only pure epoch dialects qualify. A calendar
     * format ({@code yyyyMMdd}) parses the digit string non-linearly — {@code 20240101} decodes to an instant that
     * bears no scale relation to the raw value — and a compound format ({@code a||b}) resolves per value by
     * first-matching parser, so neither admits a single scale; both return {@code null} and their consumers must
     * decline rather than push a bound the raw statistics cannot answer.
     *
     * <p>Matched on the EXACT pattern string, never a substring: formats compose with {@code ||} and resolve
     * first-match-wins, so {@code epoch_second||epoch_millis} is effectively {@code epoch_second} for every integral
     * token — a {@code contains} test would mistake it for the identity and push a bound scaled 1000x wrong.
     *
     * <p>Lives here, beside the decode it summarizes, for the same no-drift reason as
     * {@code ParquetColumnDecoding.integralDecodeScalesRelativeToRawStats}: whoever changes the coercion must see
     * the scale that describes it. Format semantics are reader-agnostic (parquet, ORC and the text readers share
     * this coercion), which is why this lives in the shared SPI while annotation semantics stay parquet-local.
     *
     * @param pattern the declared format pattern, exactly as the decode's {@link DateFormatter} was built from
     * @param declaredType the declared ES|QL type — names the storage unit the scale converts INTO
     * @return units-per-tick, or {@code null} if the format admits no exact scale (caller must decline)
     */
    @Nullable
    public static Long declaredEpochFormatScale(@Nullable String pattern, DataType declaredType) {
        if (pattern == null) {
            return null;
        }
        // Explicit branches, not a ?: chain: `cond ? 1L : null` types as primitive long and unboxes the null.
        if (declaredType == DataType.DATETIME) {
            // A datetime stores millis, so epoch_millis is the exact identity — worth pushing, not declining.
            if (EPOCH_MILLIS_PATTERN.equals(pattern)) {
                return 1L;
            }
            if (EPOCH_SECOND_PATTERN.equals(pattern)) {
                return 1_000L;
            }
            return null;
        }
        if (declaredType == DataType.DATE_NANOS) {
            // date_nanos stores nanos and ES has no epoch-nanos pattern, so it has NO identity format: every
            // declared format rescales, epoch_millis included.
            if (EPOCH_MILLIS_PATTERN.equals(pattern)) {
                return 1_000_000L;
            }
            if (EPOCH_SECOND_PATTERN.equals(pattern)) {
                return 1_000_000_000L;
            }
            return null;
        }
        // A format is only legal on a declared date/date_nanos (DeclaredSchemaValidator.validateFormat), so any
        // other declared type never reaches a scale question.
        return null;
    }
}
