/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.io.NumberInput;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.datasources.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SyntheticColumns;
import org.elasticsearch.xpack.esql.datasources.TextAggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.cache.ColumnStatsAccumulator;
import org.elasticsearch.xpack.esql.datasources.cache.CountingInputStream;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.cache.StripeStatsHarvester;
import org.elasticsearch.xpack.esql.datasources.cache.TextFormatStats;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.BufferingPageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Consumer;

/**
 * CSV/TSV format reader for external datasources.
 *
 * <h2>File format</h2>
 * <ul>
 *   <li>First non-comment line: schema — {@code column:type} pairs separated by the delimiter
 *   <li>Subsequent lines: data rows
 *   <li>A present but empty field ({@code a,,c}) reads as the empty string on {@code keyword}/{@code text}
 *       columns and as {@code null} on other types; a missing field (row shorter than the schema) is
 *       always {@code null}
 *   <li>Lines starting with the comment prefix (default {@code //}) are skipped
 * </ul>
 *
 * <h2>Supported types</h2>
 * {@code integer} ({@code int}, {@code i}), {@code long} ({@code l}),
 * {@code double} ({@code d}), {@code keyword} ({@code k}, {@code string}, {@code s}),
 * {@code text} ({@code txt}), {@code boolean} ({@code bool}),
 * {@code datetime} ({@code date}, {@code dt}), {@code date_nanos} ({@code dn}),
 * {@code ip}, {@code version} ({@code v}), {@code null} ({@code n}).
 *
 * <h2>Configurable options</h2>
 * All options are set via the {@code WITH} clause and parsed by {@link #withConfig(Map)}.
 *
 * <table>
 *   <caption>CSV options</caption>
 *   <tr><th>ES/ESQL key</th><th>Default</th><th>Description</th></tr>
 *   <tr><td>{@code delimiter}</td><td>{@code ,}</td><td>Field separator character</td></tr>
 *   <tr><td>{@code mode}</td><td>{@code quoted} ({@code .csv}) / {@code plain} ({@code .tsv})</td>
 *       <td>Named preset for how a separator-that-is-data is written: {@code quoted} (fields wrap in
 *           the quote character, RFC 4180), {@code escaped} (backslash sequences, {@code \N} null —
 *           database text exports), or {@code plain} (every byte literal). A preset over
 *           the {@code quote}/{@code escape} knobs; see the override matrix below.</td></tr>
 *   <tr><td>{@code quote}</td><td>{@code "}</td><td>Quoting character; setting it turns quoting on
 *           regardless of {@code mode}, the literal {@code none} turns it off (overrides the preset)</td></tr>
 *   <tr><td>{@code escape}</td><td>{@code \}</td><td>Escape character; setting it turns escaping on
 *           regardless of {@code mode}, the literal {@code none} turns it off (overrides the preset)</td></tr>
 *   <tr><td>{@code comment}</td><td>{@code //}</td><td>Line comment prefix</td></tr>
 *   <tr><td>{@code null_value}</td><td>(empty)</td><td>Token whose exact match reads as {@code null}. The
 *           default (empty) installs no such token: an empty field is then a present empty value (empty
 *           string on {@code keyword}/{@code text}, {@code null} on other types), not a null token</td></tr>
 *   <tr><td>{@code encoding}</td><td>{@code UTF-8}</td><td>Character encoding</td></tr>
 *   <tr><td>{@code datetime_format}</td><td>ISO-8601 / epoch</td><td>Custom datetime pattern</td></tr>
 *   <tr><td>{@code max_field_size}</td><td>10 MB</td><td>OOM protection; max bytes per field</td></tr>
 *   <tr><td>{@code multi_value_syntax}</td><td>{@code none}</td>
 *       <td>Multi-value field syntax; see "Bracket multi-value syntax" below for the
 *           {@code brackets} opt-in and the element-splitter rules (always comma, even for TSV).</td></tr>
 *   <tr><td>{@code schema_sample_size}</td><td>20,000</td><td>Number of rows to sample for type inference</td></tr>
 *   <tr><td>{@code header_row}</td><td>{@code true}</td>
 *       <td>When {@code false}, no header row is read; column names are synthesized from
 *           {@code column_prefix} and types are inferred from the sample.</td></tr>
 *   <tr><td>{@code column_prefix}</td><td>{@code col}</td>
 *       <td>Prefix for synthesized column names when {@code header_row} is {@code false};
 *           a 0-based counter is appended (e.g. {@code col0, col1, col2, ...}). Ignored when
 *           {@code header_row} is {@code true}. An empty prefix yields purely numeric names
 *           ({@code 0, 1, 2, ...}) which must be backtick-quoted in ES|QL queries.</td></tr>
 *   <tr><td>{@code trim_spaces}</td><td>{@code false}</td>
 *       <td>When {@code true}, surrounding ASCII whitespace is trimmed from field values. Default
 *           {@code false} keeps values verbatim (RFC 4180 — spaces are part of a field). Applies to
 *           string content only; typed columns tolerate padding regardless.</td></tr>
 * </table>
 *
 * <h2>Mode, quote and escape — the override matrix</h2>
 * {@code mode} is a named preset over two independent knobs: <em>quoting</em> (may a field be wrapped
 * in the quote character?) and <em>escaping</em> (is the escape character consulted?). Explicit
 * {@code quote}/{@code escape} keys always override the preset, so any combination is reachable. There
 * is no coherence check — an override that produces a wrong read is the user's to own.
 * <table>
 *   <caption>Resolved (quoting, escaping) and what each combination does</caption>
 *   <tr><th>{@code (quoting, escaping)}</th><th>Reached by</th><th>Behavior</th></tr>
 *   <tr><td>{@code (true, true)}</td><td>{@code mode: quoted} (default for {@code .csv})</td>
 *       <td>RFC 4180 quoting; backslash escapes inside quoted fields (spreadsheet / enclosed-and-escaped)</td></tr>
 *   <tr><td>{@code (false, true)}</td><td>{@code mode: escaped}</td>
 *       <td>No quoting; C-style value decode — {@code \t \n \\}, {@code \N} → null (database text exports)</td></tr>
 *   <tr><td>{@code (false, false)}</td><td>{@code mode: plain} (default for {@code .tsv})</td>
 *       <td>No quoting, no escaping; every byte literal — a field cannot contain the delimiter or a
 *           newline. Never silently corrupts input.</td></tr>
 *   <tr><td>{@code (true, false)}</td><td>{@code mode: quoted, escape: none} (no preset name)</td>
 *       <td>Pure RFC 4180: quoting with embedded-quote doubling only, backslash left as data
 *           (a Windows path inside quotes survives intact)</td></tr>
 * </table>
 * Examples of overrides: {@code mode: plain, quote: "\""} turns quoting on for an otherwise-plain
 * file; {@code mode: escaped, quote: "\""} adds quoting to backslash-escaped data (which then parses as
 * {@code (true, true)} — the C-style decode no longer runs).
 * <p>
 * The two knobs are independent: an override touches only its own knob and leaves the other at the
 * mode's preset. So {@code quote: none} on a {@code .csv} yields {@code (false, true)} — no quoting,
 * but escaping stays on (harmless on data without backslash sequences; for fully-literal reading use
 * {@code mode: plain}, or also pass {@code escape: none}).
 * <p>
 * Two response {@code Warning} headers (the channel the query author actually sees, not a DEBUG log)
 * guard the escape-decode foot-guns. (1) When decoding is off and a sampled value is the whole-field
 * {@code \N} null marker — {@code plain}, or {@code quoted} with {@code escape: none}, where the
 * marker reaches the sample literally — a data-driven warning nudges toward {@code mode: escaped}.
 * (2) When {@code mode: escaped} is combined with a {@code quote} override (resolving to
 * {@code (true, true)}, which hands the escape char to Jackson so {@code \N} is rewritten before the
 * sample exists and the data scan can't see it), a deterministic config-time warning states that the
 * decode was disabled.
 *
 * <h2>Bracket multi-value syntax</h2>
 * When {@code multi_value_syntax} is {@code brackets}, array-like values support:
 * <ul>
 *   <li>{@code [a,b,c]} — unquoted elements</li>
 *   <li>{@code ["a","b","c"]} — quoted elements (quotes stripped)</li>
 *   <li>{@code [a,"b,c"]} — mixed; commas inside quotes are literal</li>
 * </ul>
 * <p>With comma delimiter, a cell like {@code [hello,world]} is treated as one column:
 * commas inside {@code [...]} are not column delimiters.
 *
 * <h2>Error handling</h2>
 * Controlled by {@link ErrorPolicy} and its {@link ErrorPolicy.Mode}:
 * <table>
 *   <caption>Error modes</caption>
 *   <tr><th>ES/ESQL key</th><th>Behaviour</th></tr>
 *   <tr><td>{@code fail_fast}</td><td>Abort on first error (default)</td></tr>
 *   <tr><td>{@code skip_row}</td><td>Drop the entire bad row</td></tr>
 *   <tr><td>{@code null_field}</td><td>Null-fill unparseable fields, keep the row</td></tr>
 * </table>
 *
 * <h2>Examples</h2>
 * A dataset is read with {@code FROM <dataset>}; the settings below are dataset configuration, not query syntax.
 * <ul>
 *   <li>A tab-separated export that tolerates bad rows: {@code delimiter=\t}, {@code error_mode=skip_row},
 *       {@code max_errors=100}</li>
 *   <li>A column holding {@code [a,b,c]} arrays: {@code multi_value_syntax=brackets}</li>
 *   <li>A gzipped file with no header line: {@code header_row=false}</li>
 * </ul>
 *
 * <p>Works with any {@link org.elasticsearch.xpack.esql.datasources.spi.StorageProvider}
 * (HTTP, S3, local filesystem).
 */
public class CsvFormatReader implements SegmentableFormatReader {

    private static final Logger logger = LogManager.getLogger(CsvFormatReader.class);

    private static final int READER_BUFFER_SIZE = 64 * 1024;

    /** Sentinel passed to {@link CsvBatchIterator#onRowError} when the offending row could not be tokenised. */
    private static final String[] EMPTY_ROW = new String[0];

    /**
     * Shared empty {@link BytesRef} stored for a present-but-empty string cell. Safe to share across
     * rows and blocks because block builders copy the bytes on append; nothing mutates this instance.
     */
    private static final BytesRef EMPTY_STRING = new BytesRef(BytesRef.EMPTY_BYTES);

    /**
     * Value for a field that is present in the row but has empty text: the empty string on
     * {@code KEYWORD}/{@code TEXT} columns, {@code null} on every other type (which has no empty
     * representation). A MISSING field (row shorter than the schema) is handled by the callers and is
     * always {@code null}, independent of this method.
     */
    private static Object presentEmptyValue(DataType dataType) {
        return DataType.isString(dataType) ? EMPTY_STRING : null;
    }

    /**
     * Whether a field that closed with no content (an unquoted trailing delimiter, e.g. {@code a,b,})
     * counts as a present-but-empty field rather than a dropped trailing delimiter beyond the schema.
     * Shared by the fused bracket walker ({@code splitAndConvertProjected}) and the split-then-convert
     * bracket route ({@code splitCommaDelimiterBracketAwareFields}) so the two routes agree on where the
     * schema boundary falls.
     * <p>
     * The {@code priorFieldCount > 0} guard requires a preceding field: an unquoted trailing empty is only
     * meaningful after a delimiter that closed a real field (the {@code b,} in {@code a,b,}). A tokenization
     * that produced no fields at all is not a row ending in a bare delimiter, so it fabricates no empty
     * field. This also makes the predicate always {@code false} for a single-column schema
     * ({@code schemaColumnCount == 1} leaves no integer with {@code 0 < priorFieldCount < 1}): a
     * single-column present-empty cell arrives instead through the quoted-empty path ({@code ""}), while a
     * blank line is skipped before tokenization.
     */
    private static boolean isPresentTrailingEmpty(int priorFieldCount, int schemaColumnCount) {
        return priorFieldCount > 0 && priorFieldCount < schemaColumnCount;
    }

    /**
     * Reused {@link DateFormatter} that delegates to ES's hand-rolled
     * {@code Iso8601DateTimeParser}: covers the {@code YYYY-MM-DDTHH:MM:SS[.fff][Z|+HH:MM]} family
     * (plus date-only inputs like {@code YYYY-MM-DD}) without the {@link DateTimeFormatter}
     * {@code Parsed} HashMap allocation and copy that dominates {@code tryParseDatetime} on
     * datetime-heavy queries. Space-separated {@code YYYY-MM-DD HH:MM:SS} inputs are NOT
     * accepted by this parser and are handled separately by
     * {@link #tryParseSpaceSeparatedDatetimeMillis(String)}.
     * <p>
     * Note: deliberately not chained with {@code .withZone(UTC)} (unlike
     * {@code DateUtils.UTC_DATE_TIME_FORMATTER}). The downstream {@code DateFormatters.from} call
     * already defaults to {@link ZoneOffset#UTC} when the parsed accessor carries no zone, so the
     * extra {@code withZone} call would only allocate a second {@link DateFormatter} for no
     * behavioural difference on this hot path.
     */
    private static final DateFormatter ISO_DATETIME_FAST_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");

    /**
     * Sentinel returned by {@link #tryParseSpaceSeparatedDatetimeMillis(String)} when the input
     * does not match the supported space-separated template. {@link Long#MIN_VALUE} is chosen
     * because it cannot be produced by a legal {@code YYYY-MM-DD HH:MM:SS} value (the
     * {@link LocalDateTime} range tops out billions of years before that millisecond).
     */
    static final long FAST_PATH_MISS = Long.MIN_VALUE;

    /**
     * Hand-rolled fast parser for {@code YYYY-MM-DD HH:MM:SS} (length 19) and
     * {@code YYYY-MM-DD HH:MM:SS.fff} (length 23). Returns the epoch millisecond value, or
     * {@link #FAST_PATH_MISS} if {@code value} does not match the supported template (caller
     * then falls through to the general-purpose parser).
     * <p>
     * This is the documented hot path on datetime-heavy queries that filter, sort or group by
     * a space-separated DATETIME column: the JDK {@link DateTimeFormatter} builds a {@code Parsed}
     * intermediate (HashMap keyed by {@code TemporalField}) and copies it during resolve,
     * accounting for ~16% of CPU on profiled CSV scans even though the input is fixed-width and
     * can be parsed in a constant number of digit comparisons.
     * <p>
     * Only inputs that yield the same epoch milliseconds as
     * {@link DateUtils#asDateTime(String)} are accepted; any rejected input still goes through
     * the original parser, preserving the existing surface contract.
     */
    static long tryParseSpaceSeparatedDatetimeMillis(String value) {
        int len = value.length();
        if (len != 19 && len != 23) {
            return FAST_PATH_MISS;
        }
        if (value.charAt(4) != '-'
            || value.charAt(7) != '-'
            || value.charAt(10) != ' '
            || value.charAt(13) != ':'
            || value.charAt(16) != ':') {
            return FAST_PATH_MISS;
        }
        int year = parseFixedDigits(value, 0, 4);
        int month = parseFixedDigits(value, 5, 2);
        int day = parseFixedDigits(value, 8, 2);
        int hour = parseFixedDigits(value, 11, 2);
        int minute = parseFixedDigits(value, 14, 2);
        int second = parseFixedDigits(value, 17, 2);
        if ((year | month | day | hour | minute | second) < 0) {
            return FAST_PATH_MISS;
        }
        int millis = 0;
        if (len == 23) {
            if (value.charAt(19) != '.') {
                return FAST_PATH_MISS;
            }
            millis = parseFixedDigits(value, 20, 3);
            if (millis < 0) {
                return FAST_PATH_MISS;
            }
        }
        // LocalDateTime.of validates calendar bounds (month 1..12, day-of-month per month, leap
        // years etc.). Trapping the DateTimeException here keeps the fast path side-effect-free on
        // invalid dates: the caller falls through to the general-purpose Stage 3 parser, which
        // preserves the existing DateUtils.asDateTime semantics (including the user-facing
        // "Failed to parse" error message it produces for genuinely invalid inputs).
        try {
            LocalDateTime ldt = LocalDateTime.of(year, month, day, hour, minute, second);
            return ldt.toInstant(ZoneOffset.UTC).toEpochMilli() + millis;
        } catch (DateTimeException e) {
            return FAST_PATH_MISS;
        }
    }

    /**
     * Parses {@code count} consecutive ASCII decimal digits starting at {@code offset} and
     * returns the integer value, or {@code -1} if any character is outside {@code '0'..'9'}.
     * Used by the datetime fast path so a non-digit triggers a fall-through instead of an
     * exception.
     */
    private static int parseFixedDigits(String value, int offset, int count) {
        int result = 0;
        for (int i = 0; i < count; i++) {
            char c = value.charAt(offset + i);
            if (c < '0' || c > '9') {
                return -1;
            }
            result = result * 10 + (c - '0');
        }
        return result;
    }

    static final String CONFIG_DELIMITER = "delimiter";
    static final String CONFIG_MODE = "mode";
    static final String CONFIG_QUOTE = "quote";
    static final String CONFIG_ESCAPE = "escape";
    static final String CONFIG_COMMENT = "comment";
    static final String CONFIG_NULL_VALUE = "null_value";
    static final String CONFIG_ENCODING = "encoding";
    static final String CONFIG_DATETIME_FORMAT = "datetime_format";
    static final String CONFIG_MAX_FIELD_SIZE = "max_field_size";
    static final String CONFIG_MULTI_VALUE_SYNTAX = "multi_value_syntax";
    static final String CONFIG_HEADER_ROW = "header_row";
    static final String CONFIG_COLUMN_PREFIX = "column_prefix";
    static final String CONFIG_TRIM_SPACES = "trim_spaces";
    static final String CONFIG_SCHEMA_SAMPLE_SIZE = "schema_sample_size";

    /** Keys recognised by {@link #withConfigTrackingConsumedKeys(Map)}. */
    static final Set<String> RECOGNIZED_KEYS = Set.of(
        CONFIG_DELIMITER,
        CONFIG_MODE,
        CONFIG_QUOTE,
        CONFIG_ESCAPE,
        CONFIG_COMMENT,
        CONFIG_NULL_VALUE,
        CONFIG_ENCODING,
        CONFIG_DATETIME_FORMAT,
        CONFIG_MAX_FIELD_SIZE,
        CONFIG_MULTI_VALUE_SYNTAX,
        CONFIG_HEADER_ROW,
        CONFIG_COLUMN_PREFIX,
        CONFIG_TRIM_SPACES,
        CONFIG_SCHEMA_SAMPLE_SIZE
    );

    private final BlockFactory blockFactory;
    private final CsvMapper sharedCsvMapper;
    private final CsvFormatOptions options;
    private final String format;
    private final List<String> extensions;
    private final List<Attribute> resolvedSchema;
    private final int schemaSampleSize;
    // Mutable reader-level counters surfaced as a Map<String, Object> via {@link #statusSnapshot()};
    // shared across the parallel {@link CsvBatchIterator} segments spawned by {@link #read}.
    private final CsvReaderCounters counters;
    /**
     * ErrorPolicy used by the planning-time {@link #metadata} call (which has no per-query
     * {@link FormatReadContext}). Resolved from the {@code WITH} options in {@link #withConfig}
     * so a dataset configured with {@code error_mode=skip_row} also applies it to schema
     * sampling — matching common database readers' error-tolerance semantics.
     * Defaults to {@link #defaultErrorPolicy()} (FAIL_FAST), so unset implies "fail at planning
     * if the file cannot be sampled cleanly", consistent with the rest of the system.
     */
    private final ErrorPolicy effectivePolicy;
    /**
     * Node-stable identity of the row-interpretation-affecting {@code WITH} config, as produced by
     * {@link SchemaCacheKey#buildFormatConfig}. Used as the external-stats cache fingerprint. It is
     * deliberately derived from the canonical config rather than the parsed options or the resolved
     * schema: a data node reads only the query's projected columns and an instance-local options
     * object, so a projection/options-derived fingerprint would differ from the coordinator's and the
     * coordinator would reject the data node's shipped-back stats — silently disabling the warm
     * short-circuit in any real (coordinator != data node) cluster. Empty until {@link #withConfig} runs.
     */
    private final String canonicalConfig;
    /**
     * Per-column declared date parse-patterns, keyed by <b>physical</b> (file) column name (the caller applied any
     * {@code path} rename). Set via {@link #withDeclaredDateFormats}; empty when no column declares a {@code format}.
     * A {@link CsvBatchIterator} turns this into a per-projected-column {@link DateFormatter} array and parses those
     * columns' timestamps with the ES {@link DateFormatter} (zone-aware) instead of the ISO / file-level path.
     */
    private final Map<String, String> declaredDateFormats;
    /**
     * True when some column declared a {@code path}, so a pinned (strict) schema binds to the file BY NAME rather
     * than by position — see {@link org.elasticsearch.xpack.esql.datasources.spi.FormatReader#withDeclaredPathBinding}.
     * False (the default) keeps the positional declared-schema contract byte-for-byte.
     */
    private final boolean declaredPathBinding;

    /**
     * When {@code true} (default), eligible non-bracket reads use the direct-to-block path that parses
     * logical records straight into typed {@code Block} builders: plain (unquoted) reads take the
     * simplest walk, and RFC 4180 quoted reads (with or without backslash escapes) take the
     * quote/escape-aware walk. Controlled by the node setting {@code esql.csv.direct_block.enabled}
     * via {@link #withDirectBlockEnabled(boolean)}; turning it off forces the byte-equivalent Jackson
     * bulk path everywhere.
     */
    private final boolean directBlockEnabled;

    public CsvFormatReader(BlockFactory blockFactory) {
        this(
            blockFactory,
            CsvFormatOptions.DEFAULT,
            "csv",
            List.of(".csv", ".tsv"),
            null,
            CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE,
            ErrorPolicy.STRICT,
            "",
            true,
            Map.of(),
            false
        );
    }

    public CsvFormatReader(BlockFactory blockFactory, String format, List<String> extensions) {
        this(
            blockFactory,
            CsvFormatOptions.DEFAULT,
            format,
            extensions,
            null,
            CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE,
            ErrorPolicy.STRICT,
            "",
            true,
            Map.of(),
            false
        );
    }

    public CsvFormatReader(BlockFactory blockFactory, CsvFormatOptions options, String format, List<String> extensions) {
        this(
            blockFactory,
            options,
            format,
            extensions,
            null,
            CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE,
            ErrorPolicy.STRICT,
            "",
            true,
            Map.of(),
            false
        );
    }

    private CsvFormatReader(
        BlockFactory blockFactory,
        CsvFormatOptions options,
        String format,
        List<String> extensions,
        List<Attribute> resolvedSchema,
        int schemaSampleSize,
        ErrorPolicy effectivePolicy,
        String canonicalConfig,
        boolean directBlockEnabled,
        Map<String, String> declaredDateFormats,
        boolean declaredPathBinding
    ) {
        this.blockFactory = blockFactory;
        this.options = options;
        this.format = format;
        this.extensions = extensions;
        this.resolvedSchema = resolvedSchema;
        this.schemaSampleSize = schemaSampleSize;
        this.effectivePolicy = effectivePolicy;
        this.canonicalConfig = canonicalConfig;
        this.directBlockEnabled = directBlockEnabled;
        this.declaredDateFormats = declaredDateFormats != null ? Map.copyOf(declaredDateFormats) : Map.of();
        this.declaredPathBinding = declaredPathBinding;
        this.counters = new CsvReaderCounters(format);
        this.sharedCsvMapper = createMapper(options);
    }

    /**
     * Returns a copy of this reader with the direct-to-block read path toggled. Threaded from the
     * {@code esql.csv.direct_block.enabled} node setting at reader-construction time.
     */
    public CsvFormatReader withDirectBlockEnabled(boolean enabled) {
        if (enabled == directBlockEnabled) {
            return this;
        }
        return new CsvFormatReader(
            blockFactory,
            options,
            format,
            extensions,
            resolvedSchema,
            schemaSampleSize,
            effectivePolicy,
            canonicalConfig,
            enabled,
            declaredDateFormats,
            declaredPathBinding
        );
    }

    /**
     * Single source of truth for "may Jackson tokenize a record?". Jackson's CSV grammar coincides with
     * the direct-to-block walkers (and the house {@link #splitRecordFields}) only when {@code trim_spaces}
     * is on: with it off, (1) padding before a quote makes Jackson treat the field as unquoted — literal
     * quotes and a column-count explosion for {@code 1, "a,b"} — and (2) {@code SKIP_EMPTY_LINES} eats the
     * first column's leading whitespace on every row, independent of the quote char. Under trim both
     * quirks are masked (the trim would have removed that whitespace anyway and quote detection is
     * restored), so Jackson's tokenization is safe.
     *
     * <p>Escaped mode (quoting off, escaping on) is also kept on Jackson even under no-trim: it is the only
     * dialect where {@link #decodeFieldValue} is non-identity, so routing escaped mode through the house
     * splitter would diverge from inference. The direct walkers exclude escaped mode for the same reason (no
     * house grammar to mirror), so this keeps the house path confined to exactly the QUOTED / PLAIN dialects
     * the walkers serve, where {@code decodeFieldValue} is identity.
     *
     * <p>Consequence — the escaped-mode no-trim residual: because escaped mode stays on Jackson even under
     * no-trim, it also KEEPS Jackson's {@code SKIP_EMPTY_LINES} first-column leading-whitespace eating (a
     * padded {@code  x} at column 0 reads back as {@code x}; non-first columns keep their padding). This is a
     * real no-trim gap for escaped mode that the QUOTED / PLAIN house grammar does not have, but it is uniform
     * across every escaped arm (per-record, bulk, inference), so there is no cross-path misbind. Pinned by
     * {@code CsvModeReadTests.testEscapedModeStillEatsColumnZeroLeadingWhitespaceUnderNoTrim}.
     */
    private boolean jacksonGrammarApplies() {
        return options.trimSpaces() || options.decodesEscapes();
    }

    private static CsvMapper createMapper(CsvFormatOptions opts) {
        CsvMapper mapper = new CsvMapper();
        if (opts.trimSpaces()) {
            // TRIM_SPACES is gated on so mode:plain (and any opt-out) keeps field bytes verbatim; typed
            // columns tolerate padding independently (see tryConvertValue). This mapper is only consulted
            // when jacksonGrammarApplies() (trim on, or escaped mode): under no-trim Jackson's grammar
            // diverges from the walkers — it mis-splits padded quotes AND SKIP_EMPTY_LINES eats the first
            // column's leading whitespace on every row — so the record paths tokenize with
            // splitRecordFields instead and this mapper is not used for them.
            mapper.enable(CsvParser.Feature.TRIM_SPACES);
        }
        mapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        if (opts.maxFieldSize() > 0) {
            mapper.getFactory().setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(opts.maxFieldSize()).build());
        }
        return mapper;
    }

    /**
     * Merge {@code WITH} options into {@code baseline} (the reader's current {@link CsvFormatOptions}).
     * Absent keys keep baseline values so e.g. TSV's tab delimiter is preserved when only {@code header_row}
     * is overridden.
     */
    private static CsvFormatOptions parseOptionsFromConfig(Map<String, Object> config, CsvFormatOptions baseline) {
        // `mode` is a named preset over the (quoting, escaping) pair; explicit quote/escape keys then
        // override whatever the preset (or the extension baseline) chose. Overrides always win — we no
        // longer reject an "incoherent" combination, so a resulting silent misread is the user's to own.
        // The one structural rule that survives is bracket multi-values, which need a quote-aware scan.
        CsvFormatOptions.MultiValueSyntax multiValueSyntax = parseMultiValueSyntax(
            config.get(CONFIG_MULTI_VALUE_SYNTAX),
            baseline.multiValueSyntax()
        );
        CsvFormatOptions.Mode parsedMode = CsvFormatOptions.Mode.parse(
            config.get(CONFIG_MODE) == null ? null : config.get(CONFIG_MODE).toString()
        );
        if (parsedMode == null && multiValueSyntax == CsvFormatOptions.MultiValueSyntax.BRACKETS) {
            // Bracket cells carry quoted elements, so bare brackets selects QUOTED even on the
            // no-quote .tsv baseline. An explicit no-quote mode + brackets is rejected below.
            parsedMode = CsvFormatOptions.Mode.QUOTED;
        }

        boolean quoting;
        boolean escaping;
        char quoteChar;
        char escapeChar;
        if (parsedMode != null) {
            quoting = parsedMode.usesQuote();
            escaping = parsedMode.usesEscape();
            quoteChar = CsvFormatOptions.DEFAULT_QUOTE;
            escapeChar = CsvFormatOptions.DEFAULT_ESCAPE;
        } else {
            quoting = baseline.quoting();
            escaping = baseline.escaping();
            quoteChar = baseline.quoteChar();
            escapeChar = baseline.escapeChar();
        }
        // Explicit quote/escape override the preset. A bare character turns the knob on with that
        // character; the literal "none" turns it off (so e.g. `mode: quoted, escape: none` is pure
        // RFC 4180 with no backslash escape, and `mode: escaped, quote: "\""` quotes backslash-escaped data).
        Object quoteValue = config.get(CONFIG_QUOTE);
        if (isExplicitlySet(quoteValue)) {
            if (isNone(quoteValue)) {
                quoting = false;
            } else {
                quoting = true;
                quoteChar = parseChar(quoteValue, quoteChar);
            }
        }
        Object escapeValue = config.get(CONFIG_ESCAPE);
        if (isExplicitlySet(escapeValue)) {
            if (isNone(escapeValue)) {
                escaping = false;
            } else {
                escaping = true;
                escapeChar = parseChar(escapeValue, escapeChar);
            }
        }
        if (multiValueSyntax == CsvFormatOptions.MultiValueSyntax.BRACKETS && quoting == false) {
            throw new IllegalArgumentException(
                "multi_value_syntax [brackets] requires quoting (mode [quoted] or an explicit quote character); "
                    + "the bracket scanner honors quoted fields"
            );
        }
        if (parsedMode == CsvFormatOptions.Mode.ESCAPED && quoting) {
            // The user named the C-style decode (mode: escaped) but a quote override turned quoting on,
            // which resolves to (true, true) and hands the escape char to Jackson — so \N/\t are no
            // longer C-style-decoded. The data-driven null-marker warning can't catch this (Jackson
            // rewrites \N to N before the sample is built), so warn deterministically here, at config
            // time, on the response header the query author actually reads.
            HeaderWarning.addWarning(
                "Mode [escaped] with a quote override turns quoting on, which disables the escaped-mode decode "
                    + "(\\N to null, \\t to tab). To keep decoding, do not set quote; "
                    + "keep it to parse quoted fields instead."
            );
        }

        char delimiter = parseChar(config.get(CONFIG_DELIMITER), baseline.delimiter());
        String commentPrefix = parseString(config.get(CONFIG_COMMENT), baseline.commentPrefix());
        String nullValue = parseString(config.get(CONFIG_NULL_VALUE), baseline.nullValue());
        Charset encoding = parseEncoding(config.get(CONFIG_ENCODING), baseline.encoding());
        DateFormatter datetimeFormatter = parseDatetimeFormat(config.get(CONFIG_DATETIME_FORMAT), baseline.datetimeFormatter());
        int maxFieldSize = parseInt(config.get(CONFIG_MAX_FIELD_SIZE), baseline.maxFieldSize());
        boolean headerRow = parseBooleanOption(CONFIG_HEADER_ROW, config.get(CONFIG_HEADER_ROW), baseline.headerRow());
        String columnPrefix = parseString(config.get(CONFIG_COLUMN_PREFIX), baseline.columnPrefix());
        boolean trimSpaces = parseBooleanOption(CONFIG_TRIM_SPACES, config.get(CONFIG_TRIM_SPACES), baseline.trimSpaces());

        CsvFormatOptions merged = new CsvFormatOptions(
            delimiter,
            quoteChar,
            escapeChar,
            commentPrefix,
            nullValue,
            encoding,
            datetimeFormatter,
            maxFieldSize,
            multiValueSyntax,
            headerRow,
            columnPrefix,
            quoting,
            escaping,
            trimSpaces
        );
        return merged.equals(baseline) ? null : merged;
    }

    /** An option counts as user-supplied only when present AND non-empty (empty string = "use the default"). */
    private static boolean isExplicitlySet(Object value) {
        return value != null && value.toString().isEmpty() == false;
    }

    /** The literal {@code "none"} (case-insensitive) turns a quoting/escaping knob off via an override. */
    private static boolean isNone(Object value) {
        return value != null && "none".equalsIgnoreCase(value.toString().trim());
    }

    private static CsvFormatOptions.MultiValueSyntax parseMultiValueSyntax(Object value, CsvFormatOptions.MultiValueSyntax baseline) {
        if (value == null || value.toString().isEmpty()) {
            return baseline;
        }
        String s = value.toString().trim().toLowerCase(Locale.ROOT);
        if ("none".equals(s)) {
            return CsvFormatOptions.MultiValueSyntax.NONE;
        }
        if ("brackets".equals(s)) {
            return CsvFormatOptions.MultiValueSyntax.BRACKETS;
        }
        throw new IllegalArgumentException("Invalid multi_value_syntax [" + value + "]. Accepted values: \"none\", \"brackets\"");
    }

    private static char parseChar(Object value, char defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        String s = value.toString();
        if (s.isEmpty()) {
            return defaultValue;
        }
        if (s.length() == 1) {
            return s.charAt(0);
        }
        if ("\\t".equals(s)) {
            return '\t';
        }
        if ("\\n".equals(s)) {
            return '\n';
        }
        if ("\\r".equals(s)) {
            return '\r';
        }
        if ("\\\\".equals(s)) {
            return '\\';
        }
        return s.charAt(0);
    }

    private static String parseString(Object value, String defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return value.toString();
    }

    private static int parseInt(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value [" + value + "]", e);
        }
    }

    /**
     * Parse a configuration boolean option leniently:
     * <ul>
     *   <li>{@code null} → {@code defaultValue} (option absent).</li>
     *   <li>Native {@link Boolean} (e.g. JSON {@code true}/{@code false}) is returned as-is.</li>
     *   <li>String values are {@link String#trim() trimmed} (so {@code " true "} is accepted) and
     *       lowercased, then delegated to {@link Booleans#parseBoolean(String)}, which itself
     *       accepts only {@code "true"} or {@code "false"}; the lowercase normalization here is
     *       what gives us case-insensitive matching ({@code "TRUE"}, {@code "False"}, ...).</li>
     *   <li>An empty/whitespace-only string falls back to {@code defaultValue} so users can write
     *       {@code "header_row": ""} to explicitly request the default.</li>
     * </ul>
     * Anything else throws {@link IllegalArgumentException} naming the offending option key.
     */
    private static boolean parseBooleanOption(String key, Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        String s = value.toString().trim();
        if (s.isEmpty()) {
            return defaultValue;
        }
        try {
            return Booleans.parseBoolean(s.toLowerCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid boolean value [" + value + "] for option [" + key + "]", e);
        }
    }

    private static Charset parseEncoding(Object value, Charset baseline) {
        if (value == null || value.toString().isEmpty()) {
            return baseline;
        }
        try {
            return Charset.forName(value.toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid encoding [" + value + "]", e);
        }
    }

    /**
     * Compiles the file-level {@code datetime_format} option into an ES {@link DateFormatter} — the same engine the
     * per-column declared {@code format} ({@link #withDeclaredDateFormats}), the NDJSON reader's identically-named
     * option and the date field mapper all parse with. One option name must not mean two pattern dialects.
     */
    private static DateFormatter parseDatetimeFormat(Object value, DateFormatter baseline) {
        if (value == null || value.toString().isEmpty()) {
            return baseline;
        }
        try {
            return DateFormatter.forPattern(value.toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid datetime format [" + value + "]", e);
        }
    }

    public CsvFormatReader withOptions(CsvFormatOptions newOptions) {
        return new CsvFormatReader(
            blockFactory,
            newOptions,
            format,
            extensions,
            resolvedSchema,
            schemaSampleSize,
            effectivePolicy,
            canonicalConfig,
            directBlockEnabled,
            declaredDateFormats,
            declaredPathBinding
        );
    }

    @Override
    public CsvFormatReader withSchema(List<Attribute> schema) {
        return new CsvFormatReader(
            blockFactory,
            options,
            format,
            extensions,
            schema,
            schemaSampleSize,
            effectivePolicy,
            canonicalConfig,
            directBlockEnabled,
            declaredDateFormats,
            declaredPathBinding
        );
    }

    @Override
    public CsvFormatReader withDeclaredPathBinding(boolean binding) {
        if (binding == declaredPathBinding) {
            return this;
        }
        return new CsvFormatReader(
            blockFactory,
            options,
            format,
            extensions,
            resolvedSchema,
            schemaSampleSize,
            effectivePolicy,
            canonicalConfig,
            directBlockEnabled,
            declaredDateFormats,
            binding
        );
    }

    @Override
    public boolean declaredNameBindingNeedsFileStart() {
        // Headered + declared path binds against the header line, which only the first split carries. Headerless binds
        // from the names alone, so it stays splittable.
        return declaredPathBinding && options.headerRow();
    }

    /**
     * Maps each position of a pinned (strict) schema to the raw field index it reads, so a declared {@code path}
     * binds the same column it binds under {@code dynamic:true}. Returns {@code null} when no {@code path} was
     * declared — the caller then keeps the positional contract, identity-mapped, byte-for-byte as before.
     * <p>
     * Headerless files self-bind: the physical name IS the position ({@code col4} -> field 4), so no file content is
     * needed and strict stays content-independent. Headered files bind against {@code headerFields}, which the caller
     * has already read off the file.
     *
     * @param headerFields the file's header names, or {@code null} for a headerless file
     */
    private int[] declaredPathFieldIndexes(List<Attribute> readSchema, String[] headerFields, StorageObject object) {
        if (declaredPathBinding == false || readSchema == null) {
            return null;
        }
        int[] bound = new int[readSchema.size()];
        for (int i = 0; i < bound.length; i++) {
            String physical = readSchema.get(i).name();
            bound[i] = headerFields == null ? headerlessFieldIndex(physical, object) : headerFieldIndex(physical, headerFields, object);
        }
        return bound;
    }

    /** Sentinel raw field index for a declared column the file does not supply: the slot null-fills (see the emit paths). */
    static final int ABSENT_FIELD = -1;

    /** Largest headerless {@code col<N>} index that binds; a higher one is {@link #ABSENT_FIELD}. Bounds projection sizing. */
    static final int MAX_HEADERLESS_COLUMN_INDEX = 1_000_000;

    /** Digit-length ceiling for a headerless index — longer names are {@link #ABSENT_FIELD}, which also guards parse overflow. */
    private static final int MAX_HEADERLESS_INDEX_DIGITS = 7;

    /**
     * Emit one client-visible warning per declared column the file did not supply (bound to {@link #ABSENT_FIELD}).
     * The message carries NO file path or split, so a column absent from many files of a glob — or re-bound on every
     * headerless split — collapses to a single response warning through the identical-string dedup of the warning
     * layer, rather than flooding one per file.
     */
    private static void warnAbsentDeclaredColumns(int[] schemaFieldIndex, List<Attribute> readSchema, Consumer<String> warningSink) {
        if (schemaFieldIndex == null || warningSink == null) {
            return;
        }
        for (int i = 0; i < schemaFieldIndex.length; i++) {
            if (schemaFieldIndex[i] == ABSENT_FIELD) {
                String name = readSchema.get(i).name();
                warningSink.accept("declared column [" + name + "] is not present in some source files and reads null there");
            }
        }
    }

    /**
     * The raw field index a headerless physical name denotes: {@code <columnPrefix><N>} -> N. No file read. A name is
     * {@link #ABSENT_FIELD} (null + warning) — the file supplies no such column — when it is not of that form, is
     * NON-CANONICAL ({@code col007} is not how the file names field 7; inference produces exactly {@code col7}), or
     * names an index beyond {@link #MAX_HEADERLESS_COLUMN_INDEX}. The cap keeps a pathological declaration
     * ({@code col500000000}) from sizing a multi-gigabyte projection array or overflowing the bound; a real
     * {@code col<N>} beyond the row's width still null-fills structurally at read time.
     */
    private int headerlessFieldIndex(String physical, StorageObject object) {
        String prefix = options.columnPrefix();
        String digits = physical != null && physical.startsWith(prefix) ? physical.substring(prefix.length()) : null;
        if (digits == null
            || digits.isEmpty()
            || digits.chars().allMatch(c -> c >= '0' && c <= '9') == false
            || (digits.length() > 1 && digits.charAt(0) == '0') // non-canonical leading zero (col007 != col7)
            || digits.length() > MAX_HEADERLESS_INDEX_DIGITS) {  // longer than any capped index; also guards parse overflow
            return ABSENT_FIELD;
        }
        int index = Integer.parseInt(digits);
        return index <= MAX_HEADERLESS_COLUMN_INDEX ? index : ABSENT_FIELD;
    }

    /**
     * A duplicate header name makes by-name binding ambiguous — a declared name could resolve to either column. The
     * inference path rejects duplicate header names, so a declared read must too, rather than silently binding the
     * first. This is a genuine error (a malformed file), not the absent-column null-fill case.
     */
    private void rejectDuplicateHeaderNames(String[] headerNames, StorageObject object) {
        Set<String> seen = new HashSet<>(headerNames.length);
        for (String name : headerNames) {
            if (seen.add(name) == false) {
                throw new IllegalArgumentException(
                    "the header of [" + object.path() + "] has duplicate column name [" + name + "]; declared columns cannot bind by name"
                );
            }
        }
    }

    /**
     * The raw field index a headered physical name denotes, looked up in the file's own header line, or
     * {@link #ABSENT_FIELD} when the header does not carry that name — a declared column the file does not supply, which
     * reads null with a warning rather than failing.
     */
    private int headerFieldIndex(String physical, String[] headerFields, StorageObject object) {
        for (int i = 0; i < headerFields.length; i++) {
            if (headerFields[i].equals(physical)) {
                return i;
            }
        }
        return ABSENT_FIELD;
    }

    @Override
    public CsvFormatReader withDeclaredDateFormats(Map<String, String> physicalNameToPattern) {
        if (physicalNameToPattern == null || physicalNameToPattern.isEmpty()) {
            return this;
        }
        return new CsvFormatReader(
            blockFactory,
            options,
            format,
            extensions,
            resolvedSchema,
            schemaSampleSize,
            effectivePolicy,
            canonicalConfig,
            directBlockEnabled,
            physicalNameToPattern,
            declaredPathBinding
        );
    }

    @Override
    public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(this);
        }
        CsvFormatOptions parsed = parseOptionsFromConfig(config, options);
        int newSampleSize = parseInt(config.get(CONFIG_SCHEMA_SAMPLE_SIZE), schemaSampleSize);
        Check.isTrue(newSampleSize > 0, CONFIG_SCHEMA_SAMPLE_SIZE + " must be positive, got: {}", newSampleSize);
        ErrorPolicy resolvedPolicy = ErrorPolicy.fromConfig(config, effectivePolicy);
        CsvFormatReader result = parsed != null ? withOptions(parsed) : this;
        // Pin the node-stable config identity from THIS query's WITH config. buildFormatConfig filters
        // to format-affecting params (dropping credentials, split keys, and any per-node augmentation),
        // so a coordinator and a data node configured from the same logical query derive the same value.
        String canon = SchemaCacheKey.buildFormatConfig(config);
        result = new CsvFormatReader(
            result.blockFactory,
            result.options,
            result.format,
            result.extensions,
            result.resolvedSchema,
            newSampleSize,
            resolvedPolicy,
            canon,
            result.directBlockEnabled,
            result.declaredDateFormats,
            result.declaredPathBinding
        );
        return Configured.fromKnownSubset(result, config, RECOGNIZED_KEYS);
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        List<Attribute> schema = readSchema(object);
        String location = object.path().toString();
        // mtime required for cache participation; sizeInBytes best-effort (stream-only sources throw from length()).
        long mtimeMillis;
        try {
            Instant mtime = object.lastModified();
            if (mtime == null) {
                return new SimpleSourceMetadata(schema, formatName(), location);
            }
            mtimeMillis = mtime.toEpochMilli();
        } catch (IOException e) {
            return new SimpleSourceMetadata(schema, formatName(), location);
        }
        OptionalLong cachedSize;
        try {
            cachedSize = OptionalLong.of(object.length());
        } catch (IOException | UnsupportedOperationException e) {
            cachedSize = OptionalLong.empty();
        }
        final OptionalLong sizeInBytes = cachedSize;
        String configFingerprint = computeConfigFingerprint();
        // Cold resolution publishes only the file size + identity (mtime, fingerprint); row/column
        // stats arrive later via the data-node capture → coordinator reconcile into SchemaCacheEntry.
        SourceStatistics stats = TextFormatStats.build(Optional.empty(), sizeInBytes, schema);
        Map<String, Object> baseSourceMetadata = Map.of(
            ExternalStats.MTIME_MILLIS_KEY,
            mtimeMillis,
            ExternalStats.CONFIG_FINGERPRINT_KEY,
            configFingerprint
        );
        Map<String, Object> sourceMetadata = SourceStatisticsSerializer.embedStatistics(baseSourceMetadata, stats);
        return new SimpleSourceMetadata(schema, formatName(), location, stats, null, sourceMetadata, null);
    }

    /**
     * Node-stable identity of the row-interpretation-affecting {@code WITH} config — the same
     * canonical string {@link SchemaCacheKey#buildFormatConfig} stores on the cache key, so a data
     * node's shipped-back contribution and the coordinator's cache entry compare equal across JVMs.
     */
    private String computeConfigFingerprint() {
        return canonicalConfig;
    }

    private List<Attribute> readSchema(StorageObject object) throws IOException {
        String sourceLocation = object.path().toString();
        InputStream stream = object.newStream();
        // Abort rather than close: providers like S3 drain remaining bytes on close() to reuse
        // the connection. We read only the schema prefix of what may be a multi-GB file, so
        // draining would block for the full object transfer time. Schema results are cached,
        // so discarding the connection here is acceptable. The abort is wrapped in a Closeable
        // so try-with-resources attaches any abort-time error as a suppressed exception on the
        // primary failure rather than replacing it.
        try (Closeable abortOnExit = () -> object.abortStream(stream)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, options.encoding()), READER_BUFFER_SIZE);
            CsvLogicalRecordReader recordReader = new CsvLogicalRecordReader(
                reader,
                options.quoteChar(),
                options.delimiter(),
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                options.encoding(),
                options.quoting()
            );
            if (options.headerRow() == false) {
                return inferSchemaWithSyntheticNames(recordReader, sourceLocation);
            }
            String headerLine = null;
            String record;
            while ((record = recordReader.readRecord(false)) != null) {
                String trimmed = record.trim();
                if (trimmed.isEmpty() || (options.commentPrefix().isEmpty() == false && trimmed.startsWith(options.commentPrefix()))) {
                    continue;
                }
                headerLine = trimmed;
                break;
            }
            if (headerLine == null) {
                throw new IOException("CSV file has no schema line");
            }
            List<Attribute> typedSchema = parseSchema(headerLine);
            if (typedSchema != null) {
                checkUniqueAttributeNames(typedSchema);
                return typedSchema;
            }
            List<Attribute> inferred = inferSchemaFromSample(headerLine, recordReader, sourceLocation);
            checkUniqueAttributeNames(inferred);
            return inferred;
        }
    }

    private List<Attribute> inferSchemaFromSample(String headerLine, CsvLogicalRecordReader recordReader, String sourceLocation)
        throws IOException {
        String[] columnNames = splitFieldsForOptions(headerLine, options);
        if (options.quoting()) {
            // No type annotations on this path, so the fields are bare names — unwrap RFC 4180 quoting.
            unquoteHeaderNames(columnNames, options.quoteChar());
        }
        Iterator<List<?>> csvIterator = newCsvIterator(recordReader);
        CircuitBreaker breaker = blockFactory.breaker();
        SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, breaker, effectivePolicy);
        try {
            maybeHintUndecodedNullMarker(sample.rows(), sourceLocation);
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows(), options.datetimeFormatter());
        } finally {
            breaker.addWithoutBreaking(-sample.reservedBytes());
        }
    }

    private List<Attribute> inferSchemaWithSyntheticNames(CsvLogicalRecordReader recordReader, String sourceLocation) throws IOException {
        Iterator<List<?>> csvIterator = newCsvIterator(recordReader);
        CircuitBreaker breaker = blockFactory.breaker();
        SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, breaker, effectivePolicy);
        try {
            if (sample.rows().isEmpty()) {
                throw new IOException("CSV file has no data rows");
            }
            maybeHintUndecodedNullMarker(sample.rows(), sourceLocation);
            return inferSyntheticSchema(sample.rows(), options.columnPrefix(), options.datetimeFormatter());
        } finally {
            breaker.addWithoutBreaking(-sample.reservedBytes());
        }
    }

    /**
     * Visibility for the escape-decode foot-guns of the independent-knobs model: whenever C-style
     * decoding is OFF ({@link CsvFormatOptions#decodesEscapes()} is false — i.e. any mode except
     * {@code escaped}: {@code plain}, {@code quoted}, or {@code quoted} with {@code escape: none}) but
     * the sample carries the whole-field {@code \N} null marker, the data is almost certainly a
     * database text export read under a mode that won't decode it — {@code \N} stays the
     * literal two characters instead of null. This catches both the safe {@code plain} default and the
     * {@code mode: escaped, quote: …} case (which resolves to quoted, dropping the decode).
     * <p>
     * Surfaced as a response {@code Warning} header (via {@link HeaderWarning}) rather than a log line,
     * because the audience is the query author — who reads the response, not the node's DEBUG log; this
     * is the same channel the skipped-row warnings use. {@link HeaderWarning} dedupes identical
     * messages, so a query sees at most one such line regardless of how many inference paths fire.
     * Scanned only over the already-materialized sample (bounded by {@code schema_sample_size}), so
     * there is no per-row hot-path cost, and the trigger is the whole-field {@code \N} marker rather
     * than any backslash sequence, so a literal Windows path like {@code C:\temp} never produces a
     * false nudge. Returns on the first match.
     */
    private void maybeHintUndecodedNullMarker(List<String[]> sampleRows, String sourceLocation) {
        if (options.decodesEscapes()) {
            return;
        }
        for (int r = 0; r < sampleRows.size(); r++) {
            String[] row = sampleRows.get(r);
            if (row == null) {
                continue;
            }
            for (int c = 0; c < row.length; c++) {
                if (isBackslashNullMarker(row[c])) {
                    HeaderWarning.addWarning(
                        "["
                            + format
                            + "] read from ["
                            + sourceLocation
                            + "]: the value at data row ["
                            + (r + 1)
                            + "], column ["
                            + (c + 1)
                            + "] is the \\N null marker, but the current mode keeps it as literal text instead of reading "
                            + "it as null. Set mode=escaped to decode it; do not also set a "
                            + "quote, which turns the decode back off."
                    );
                    return;
                }
            }
        }
    }

    /** Whether a sampled cell is exactly the two-character {@code \N} — the universal DB-export null marker. */
    private static boolean isBackslashNullMarker(String cell) {
        return cell != null && cell.length() == 2 && cell.charAt(0) == '\\' && cell.charAt(1) == 'N';
    }

    /**
     * Build a schema for a headerless CSV: count the widest sample row, synthesize names from
     * {@code prefix}, and run type inference. Pure on its inputs — does not touch the circuit
     * breaker. Both call sites must guarantee {@code sampleRows} is non-empty (and surface the
     * user-facing "CSV file has no data rows" {@link IOException} themselves); the assertion is
     * just a programmer-error guard.
     */
    static List<Attribute> inferSyntheticSchema(List<String[]> sampleRows, String prefix, @Nullable DateFormatter datetimeFormatter) {
        assert sampleRows.isEmpty() == false : "sampleRows must be non-empty for synthetic schema inference";
        int columnCount = 0;
        for (String[] row : sampleRows) {
            if (row.length > columnCount) {
                columnCount = row.length;
            }
        }
        String[] columnNames = synthesizeColumnNames(columnCount, prefix);
        return CsvSchemaInferrer.inferSchema(columnNames, sampleRows, datetimeFormatter);
    }

    static String[] synthesizeColumnNames(int count, String prefix) {
        String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            names[i] = prefix + i;
        }
        return names;
    }

    /**
     * Throws a {@link ParsingException} when the inferred or typed header has duplicate column names.
     * Without this guard the optimizer's {@code PlanConsistencyChecker} would later 500 with a
     * "duplicate output attribute" error that is hard to map back to the CSV input.
     */
    private static void checkUniqueAttributeNames(List<Attribute> attributes) {
        Set<String> seen = new HashSet<>(attributes.size());
        LinkedHashSet<String> duplicates = null;
        for (Attribute a : attributes) {
            if (seen.add(a.name()) == false) {
                if (duplicates == null) {
                    duplicates = new LinkedHashSet<>();
                }
                duplicates.add(a.name());
            }
        }
        if (duplicates != null) {
            // Render as ['a', '', 'b'] so empty-string names (a common cause via leading double commas)
            // are visible instead of collapsing to [].
            StringJoiner rendered = new StringJoiner(", ", "[", "]");
            for (String dup : duplicates) {
                rendered.add("'" + dup + "'");
            }
            throw new ParsingException(
                "CSV header has duplicate column names {}; if the file has no header row, " + "set header_row=false",
                rendered.toString()
            );
        }
    }

    /**
     * Per-record iterator used for schema reading, sampling, and bootstrap paths where each record is
     * materialized into a {@link String} before being tokenized ({@link CsvRecordIterator#parseRecord}
     * tokenizes with Jackson when {@link #jacksonGrammarApplies()}, else with the house
     * {@link #splitRecordFields}). Cost is bounded by {@link #schemaSampleSize} on inference paths. When
     * Jackson's grammar applies the bulk read loop swaps over to {@link #newJacksonBulkIterator} after
     * schema resolution so the per-row hot path stays on Jackson's direct bulk char-buffer tokenization;
     * under no-trim (Jackson's grammar does not apply) the data path stays on this per-record iterator so
     * every record is tokenized by the house grammar, matching the direct-to-block walkers.
     */
    private Iterator<List<?>> newCsvIterator(CsvLogicalRecordReader recordReader) throws IOException {
        return new CsvRecordIterator(recordReader, newCsvSchema());
    }

    /**
     * True when the failure chain carries a {@link CsvRecordTooLargeException} — an over-{@code max_record_size}
     * record dropped by the record-reader path and laundered into an unchecked wrapper by
     * {@link ExternalFailures#surface} (see {@link CsvRecordIterator#hasNext}). Detected by cause-walk so the
     * pragma-dependent survivor loss can safe-miss the stats publish, matching the bracket path's typed catch.
     */
    private static boolean isRecordCapDrop(Throwable e) {
        return ExceptionsHelper.unwrap(e, CsvRecordTooLargeException.class) != null;
    }

    /**
     * Bulk-path iterator: hands the raw {@link Reader} straight to Jackson's {@link CsvParser} so the
     * per-row hot loop tokenizes characters in Jackson's internal char buffer instead of re-materializing
     * each logical record into a {@link StringBuilder} for a follow-up Jackson parse. The byte-level
     * {@code max_record_size} cap is enforced upstream by {@link CsvRecordCappingInputStream}, so this
     * path no longer needs the per-char accounting that {@link CsvLogicalRecordReader#readRecord} added.
     * Used after schema resolution / sampling, where every subsequent record flows through this iterator —
     * but only when {@link #jacksonGrammarApplies()} (trim on, or escaped mode). Under no-trim the data
     * path routes to the per-record {@link #newCsvIterator} + house {@link #splitRecordFields} instead, so
     * Jackson's diverging no-trim grammar (padded-quote mis-split, col-0 whitespace eating) is not used.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Iterator<List<?>> newJacksonBulkIterator(Reader reader) throws IOException {
        return (Iterator) sharedCsvMapper.readerFor(List.class).with(newCsvSchema()).readValues(reader);
    }

    /**
     * The Jackson schema shared by both read paths. Quoting is the authoritative knob: when it is off
     * (PLAIN / ESCAPED) the schema calls {@code withoutQuoteChar()} so a {@code "} byte is data, not a
     * field wrapper — without this a field-leading quote opens a region Jackson scans across newlines,
     * gluing records (the original ClickBench failure). On the no-quote path the escape character is
     * withheld too: Jackson's escape is "next char is literal" (so {@code \t} would become {@code t}),
     * not the C-style sequences the escaped mode needs; the backslash must reach
     * {@link #decodeFieldValue} untouched. When quoting is on, the escape character is installed only
     * if escaping is also on (enclosed-and-escaped style); pure RFC 4180 (escaping off) leaves the backslash
     * as data so e.g. a Windows path inside quotes survives intact.
     */
    private CsvSchema newCsvSchema() {
        CsvSchema schema = CsvSchema.emptySchema().withColumnSeparator(options.delimiter());
        // Only a non-empty custom null_value installs a Jackson null token. The default empty
        // null_value must NOT null-fill empty cells: empty string cells survive as "" so the
        // empty-vs-null decision is made per type in tryConvertValue (empty string on string columns,
        // null otherwise). Setting withNullValue("") here would collapse empty to null before we ever
        // see the value, hiding present-empty string cells.
        if (options.nullValue().isEmpty() == false) {
            schema = schema.withNullValue(options.nullValue());
        }
        if (options.quoting() == false) {
            return schema.withoutQuoteChar();
        }
        schema = schema.withQuoteChar(options.quoteChar());
        return options.escaping() ? schema.withEscapeChar(options.escapeChar()) : schema;
    }

    /**
     * Holds sample rows collected for schema inference together with the number of bytes
     * reserved on the circuit breaker. Callers must release {@link #reservedBytes} via
     * {@link CircuitBreaker#addWithoutBreaking(long)} when the rows are no longer needed.
     * <p>
     * {@link #rowStartBytes} carries each row's file-relative byte offset when the caller passed
     * a non-null {@code recordReader} to {@link #collectSampleRows} (i.e. the data-path sampling
     * sites that replay the prefetched rows). {@code null} otherwise — the planning-time
     * {@code metadata()} path discards the sample after type inference, so the offsets are dead
     * data and skipping their capture keeps that call site allocation-free.
     */
    record SchemaSample(List<String[]> rows, long reservedBytes, long[] rowStartBytes, boolean recordCapDropped) {}

    /** Hard cap on consecutive parse failures during schema sampling, applied INDEPENDENTLY of
     *  the user's {@link ErrorPolicy}. Jackson's stream-based CSV parser cannot guarantee
     *  resync after a malformed record (the tokeniser may have consumed bytes mid-field), so
     *  even a generous {@code max_errors} budget cannot make progress past a permanently
     *  confused parser state. Native C++ readers don't need this guard because their
     *  parsers can resync reliably. */
    static final int MAX_CONSECUTIVE_SAMPLING_FAILURES = 16;

    /** Maximum number of distinct error excerpts captured for the failure message. Keeps the
     *  eventual {@link ParsingException} small. */
    private static final int MAX_CAPTURED_SAMPLING_ERRORS = 3;

    /**
     * Samples rows for schema inference, honouring the given {@link ErrorPolicy} the same way
     * the data-row path does:
     * <ul>
     *   <li>{@code FAIL_FAST}: throw {@link ParsingException} (HTTP 400) on the first malformed
     *       row, with a capped row excerpt and a hint pointing at {@code skip_row}.</li>
     *   <li>{@code SKIP_ROW} / {@code NULL_FIELD}: skip bad rows, continue sampling, throw if
     *       the budget ({@code max_errors} / {@code max_error_ratio}) is exceeded.</li>
     * </ul>
     * Independent of the policy, sampling bails after
     * {@link #MAX_CONSECUTIVE_SAMPLING_FAILURES} consecutive failures (Jackson resync guard),
     * and throws if zero rows could be collected.
     *
     * <p>Aligning sampling with the runtime policy matches common database readers'
     * error-tolerance semantics (one budget covering both phases) and
     * means a dataset configured with {@code error_mode=skip_row} is honoured at planning time
     * too — not just once data starts flowing.
     */
    static SchemaSample collectSampleRows(
        Iterator<List<?>> csvIterator,
        String commentPrefix,
        int sampleSize,
        CircuitBreaker breaker,
        ErrorPolicy policy
    ) {
        return collectSampleRows(csvIterator, commentPrefix, sampleSize, breaker, policy, null, 0L);
    }

    /**
     * Variant that additionally records the file-relative byte offset of each sampled row (split
     * start byte added by the caller). Used by the data-path sampling sites so the prefetched
     * rows can replay with their original {@code _rowPosition} value attached. {@code recordReader}
     * must be the source reader feeding {@code csvIterator}; pass {@code null} to skip offset
     * capture (planning-time {@code metadata()} call site).
     */
    static SchemaSample collectSampleRows(
        Iterator<List<?>> csvIterator,
        String commentPrefix,
        int sampleSize,
        CircuitBreaker breaker,
        ErrorPolicy policy,
        CsvLogicalRecordReader recordReader,
        long splitStartByte
    ) {
        List<String[]> sampleRows = new ArrayList<>();
        boolean trackOffsets = recordReader != null;
        // Backed by a List<Long> so it grows with sampleRows without pre-allocating sampleSize
        // slots (sampling can short-circuit on consecutive failures well before sampleSize is hit).
        List<Long> rowStartBytesList = trackOffsets ? new ArrayList<>() : null;
        long reservedBytes = 0;
        boolean success = false;
        boolean capDropped = false;
        List<String> capturedErrors = null;
        Throwable firstCause = null;
        long errorCount = 0;
        long totalRowCount = 0;
        int consecutiveFailures = 0;
        boolean failFast = policy.mode() == ErrorPolicy.Mode.FAIL_FAST;
        try {
            boolean hasCommentFilter = commentPrefix != null && commentPrefix.isEmpty() == false;
            while (sampleRows.size() < sampleSize) {
                try {
                    if (csvIterator.hasNext() == false) {
                        break;
                    }
                    List<?> rowList = csvIterator.next();
                    totalRowCount++;
                    String[] row = new String[rowList.size()];
                    for (int i = 0; i < rowList.size(); i++) {
                        Object val = rowList.get(i);
                        row[i] = val != null ? val.toString() : null;
                    }
                    if (hasCommentFilter && row.length > 0 && row[0] != null) {
                        if (row[0].trim().startsWith(commentPrefix)) {
                            continue;
                        }
                    }
                    long rowBytes = estimateRowBytes(row);
                    breaker.addEstimateBytesAndMaybeBreak(rowBytes, "csv_schema_inference");
                    reservedBytes += rowBytes;
                    if (trackOffsets) {
                        // Anchored to the just-returned record. bytesRead is cumulative across all
                        // record reads including any skipped blank/comment lines, so the difference
                        // gives the file-relative start byte of THIS row's first character.
                        long rowStartByte = splitStartByte + recordReader.bytesRead() - recordReader.lastRecordBytes();
                        rowStartBytesList.add(rowStartByte);
                    }
                    sampleRows.add(row);
                    consecutiveFailures = 0;
                } catch (RuntimeException e) {
                    totalRowCount++;
                    errorCount++;
                    if (isRecordCapDrop(e)) {
                        // A cap-dropped row within the sampling window is a pragma-dependent survivor loss that
                        // would replay N-1 into the batch and publish with recordCapDropped still false; propagate
                        // so the caller safe-misses the stats publish. (FAIL_FAST throws below and never publishes.)
                        capDropped = true;
                    }
                    if (failFast) {
                        // Single point of truth for FAIL_FAST: same exception type and hint as
                        // the data-row path so users see consistent error messages whether
                        // sampling or reading actually tripped the failure.
                        throw failFastSamplingError(totalRowCount, e);
                    }
                    consecutiveFailures++;
                    if (firstCause == null) {
                        firstCause = e;
                    }
                    if (capturedErrors == null) {
                        capturedErrors = new ArrayList<>(MAX_CAPTURED_SAMPLING_ERRORS);
                    }
                    if (capturedErrors.size() < MAX_CAPTURED_SAMPLING_ERRORS) {
                        capturedErrors.add(CsvErrorMessages.summarize(e.getMessage()));
                    }
                    if (policy.isBudgetExceeded(errorCount, totalRowCount)) {
                        throw budgetExceededSamplingError(errorCount, totalRowCount, policy, capturedErrors, firstCause);
                    }
                    if (consecutiveFailures >= MAX_CONSECUTIVE_SAMPLING_FAILURES) {
                        // Jackson cannot resync; bail with whatever we have. If we have at least
                        // one row this is a successful (partial) sample; otherwise the empty
                        // check below converts it to a ParsingException.
                        break;
                    }
                }
            }
            if (sampleRows.isEmpty() && capturedErrors != null) {
                throw zeroRowsSamplingError(capturedErrors, firstCause);
            }
            success = true;
            long[] offsets = null;
            if (trackOffsets) {
                offsets = new long[rowStartBytesList.size()];
                for (int i = 0; i < offsets.length; i++) {
                    offsets[i] = rowStartBytesList.get(i);
                }
            }
            return new SchemaSample(sampleRows, reservedBytes, offsets, capDropped);
        } finally {
            if (success == false) {
                breaker.addWithoutBreaking(-reservedBytes);
            }
        }
    }

    private static ParsingException failFastSamplingError(long row, Throwable cause) {
        Exception e = cause instanceof Exception ex ? ex : null;
        return new ParsingException(
            e,
            Source.EMPTY,
            "{}",
            "CSV schema sampling failed at row ["
                + row
                + "]: "
                + CsvErrorMessages.summarize(cause != null ? cause.getMessage() : "(no message)")
                + "; set error_mode=skip_row (or null_field) to skip and warn instead of failing"
        );
    }

    private static ParsingException budgetExceededSamplingError(
        long errorCount,
        long rowCount,
        ErrorPolicy policy,
        List<String> capturedErrors,
        Throwable firstCause
    ) {
        Exception cause = firstCause instanceof Exception ex ? ex : null;
        StringBuilder details = new StringBuilder("CSV schema sampling exceeded error budget: [").append(errorCount)
            .append("] errors in [")
            .append(rowCount)
            .append("] sampled rows, maximum allowed is [")
            .append(policy.maxErrors())
            .append("] errors or [")
            .append(policy.maxErrorRatio())
            .append("] ratio; first errors: ");
        appendCapturedErrors(details, capturedErrors);
        return new ParsingException(cause, Source.EMPTY, "{}", details.toString());
    }

    private static ParsingException zeroRowsSamplingError(List<String> capturedErrors, Throwable firstCause) {
        Exception cause = firstCause instanceof Exception ex ? ex : null;
        StringBuilder details = new StringBuilder("CSV schema inference failed: no rows could be parsed; first errors: ");
        appendCapturedErrors(details, capturedErrors);
        return new ParsingException(cause, Source.EMPTY, "{}", details.toString());
    }

    private static void appendCapturedErrors(StringBuilder details, List<String> capturedErrors) {
        for (int i = 0; i < capturedErrors.size(); i++) {
            if (i > 0) {
                details.append("; ");
            }
            details.append('[').append(capturedErrors.get(i)).append(']');
            if (details.length() >= CsvErrorMessages.MAX_MESSAGE_CHARS) {
                break;
            }
        }
    }

    /**
     * Estimates heap usage for a {@code String[]} row: array header + reference slots +
     * per-string overhead (object header, fields, char storage).
     */
    static long estimateRowBytes(String[] row) {
        long bytes = 16L + (long) row.length * 8;
        for (String s : row) {
            if (s != null) {
                bytes += 40L + (long) s.length() * 2;
            }
        }
        return bytes;
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        InputStream rawStream = object.newStream();
        // CountingInputStream tracks decompressed-byte consumption for stream-only sources whose
        // length() throws UnsupportedOperationException. The byte count flows through {@link
        // ExternalStats} as sizeInBytes when the file lacks a publishable length.
        CountingInputStream stream = new CountingInputStream(rawStream);
        // Scope the byte-level cap wrap to the Jackson bulk path. Bracket-aware parsing relies on
        // CsvLogicalRecordReader.addBytes for a recoverable per-record cap; wrapping the underlying
        // stream would let the cap fire mid-{@link BufferedReader#fill}, leaving the reader at an
        // undefined offset and turning a per-row recovery into a stream-fatal abort. When bracket
        // mode is enabled the data path goes through CsvLogicalRecordReader and never reaches the
        // Jackson bulk iterator, so the wrap brings no defense-in-depth there either.
        boolean useBracketAware = options.multiValueSyntax() == CsvFormatOptions.MultiValueSyntax.BRACKETS && options.delimiter() == ',';
        // _rowPosition projected (_id / _file.record_ref requested) forces the same CsvLogicalRecordReader
        // data path as bracket mode: the Jackson bulk iterator bypasses recordReader's per-record byte
        // accounting, so the composed file-global offset would stay pinned at the header boundary for every
        // data row. That path enforces max_record_size per record (char-decoded), so it must not also carry
        // the byte-level cap wrap, for the same mid-fill desync reason as bracket mode.
        boolean rowPositionProjected = SyntheticColumns.rowPositionIndexInNames(context.projectedColumns()) >= 0;
        // Direct-to-block path: non-bracket, non-escaped-mode reads parse logical records straight into
        // typed Block builders, skipping the Jackson tokenizer. PLAIN (unquoted) takes the simplest
        // walk; QUOTED (RFC 4180, with or without backslash escapes) takes the quote/escape-aware walk.
        // Like the bracket-aware path both rely on CsvLogicalRecordReader.addBytes for a recoverable
        // per-record cap, so the byte-level cap wrap is skipped here too (wrapping the underlying stream
        // would let the cap fire mid-fill, leaving the reader at an undefined offset). _rowPosition
        // projection is excluded: the bulk record walk does not advance recordReader's byte accounting,
        // so those reads route through the recordReader-backed offset path below instead.
        boolean directEligible = directBlockEnabled
            && useBracketAware == false
            && rowPositionProjected == false
            && options.multiValueSyntax() == CsvFormatOptions.MultiValueSyntax.NONE
            && options.decodesEscapes() == false
            // ALL scope harvests EVERY file column (incl. unprojected) from the raw String[] via
            // harvestAllColumns; the direct path stages only projected typed values (no String[]), so ALL
            // routes to the String[]-materialising path instead — matching the fused bracket path's ALL exclusion.
            && context.statsColumnScope() != StripeColumnScope.ALL;
        boolean useDirectBlockPlain = directEligible && options.quoting() == false;
        boolean useDirectBlockQuoted = directEligible && options.quoting();
        boolean useDirectBlock = useDirectBlockPlain || useDirectBlockQuoted;
        // The quoted walk decodes escapes (Jackson's quoted-escape rule), so the record reader must carry escaped
        // terminators/quotes verbatim to match Jackson's record boundaries (see CsvLogicalRecordReader).
        // This is needed on every recordReader-backed data path for a QUOTED + escaping dialect — the
        // direct quoted walk, the _rowPosition per-record read, and the no-trim reroute onto the house
        // tokenizer — otherwise a `\`-escaped newline would terminate the record early and split one
        // logical row in two. The Jackson bulk path (trim on / escaped mode) installs the escape char in
        // its own schema instead, so it does not need the recordReader escape-aware. Bracket mode scans
        // its own boundaries and is excluded.
        boolean recordEscapeAware = options.quoting()
            && options.escaping()
            && useBracketAware == false
            && (useDirectBlockQuoted || rowPositionProjected || jacksonGrammarApplies() == false);
        // The recordReader-backed path (bracket mode, _rowPosition projection, or the no-trim reroute onto
        // the house per-record tokenizer) and the direct-to-block path all enforce the per-record cap via
        // CsvLogicalRecordReader.addBytes, so none carry the byte-level cap wrap (wrapping the underlying
        // stream would let the cap fire mid-fill, leaving the reader at an undefined offset). The no-trim
        // disjunct mirrors the routing below (rowPositionSlot >= 0 || jacksonGrammarApplies() == false):
        // when Jackson's grammar does not apply and the read is not direct-eligible, the data path runs
        // through newCsvIterator(recordReader), so it must skip the wrap exactly like the _rowPosition path.
        boolean useRecordReaderPath = useBracketAware
            || rowPositionProjected
            || (useDirectBlock == false && jacksonGrammarApplies() == false);
        InputStream capped = (useRecordReaderPath || useDirectBlock)
            ? stream
            : new CsvRecordCappingInputStream(stream, context.maxRecordBytes());
        BufferedReader reader = new BufferedReader(new InputStreamReader(capped, options.encoding()), READER_BUFFER_SIZE);
        CsvLogicalRecordReader recordReader = recordEscapeAware
            ? new CsvLogicalRecordReader(
                reader,
                options.quoteChar(),
                options.delimiter(),
                options.escapeChar(),
                context.maxRecordBytes(),
                options.encoding(),
                options.quoting(),
                true
            )
            : new CsvLogicalRecordReader(
                reader,
                options.quoteChar(),
                options.delimiter(),
                context.maxRecordBytes(),
                options.encoding(),
                options.quoting()
            );
        // Only the direct-to-block path lets this reader own the stream end to end, so bulk read-ahead
        // is safe there. The Jackson path skips the header through this reader then resumes on the same
        // underlying BufferedReader, so it must stay non-buffered (no read-ahead) to avoid swallowing
        // bytes Jackson still needs.
        if (useDirectBlock) {
            recordReader.enableBulkBuffering();
        }
        // _rowPosition byte-axis invariant: context.splitStartByte() and recordReader.bytesRead()
        // must both be decompressed-byte offsets, or the composed file-global offset is garbage.
        // Every current dispatch path honors this; a future compressed macro-split (bzip2 /
        // zstd-indexed) would pass a compressed splitStartByte and MUST translate it first.
        //
        // Falls back to effectivePolicy (resolved from the dataset config in withConfig) so an
        // error_mode=skip_row dataset also applies it to the data path when no
        // upstream caller has built a FormatReadContext with an explicit policy. The planner
        // path always sets context.errorPolicy() explicitly.
        ErrorPolicy effective = context.errorPolicy() != null ? context.errorPolicy() : effectivePolicy;
        List<Attribute> effectiveSchema;
        List<Attribute> readSchema = context.readSchema();
        // Raw field index per declared column, or null for the positional contract. Set only when a path was
        // declared; see declaredPathFieldIndexes.
        int[] schemaFieldIndex = null;
        if (logger.isDebugEnabled()) {
            logger.debug(
                "CSV read [{}]: readSchema={}, firstSplit={}, recordAligned={}, projection={}",
                object.path(),
                readSchema == null ? "null" : "present(" + readSchema.size() + ")",
                context.firstSplit(),
                context.recordAligned(),
                context.projectedColumns() == null ? "null" : context.projectedColumns().size()
            );
        }
        if (readSchema != null) {
            if (context.firstSplit() && options.headerRow()) {
                // A declared (pinned) schema binds its columns to the header BY NAME (when declaredPathBinding), which
                // consumes the header line — so it is read here, not skipped. Runs before ownership of the stream chain
                // transfers to the returned iterator, so the reader must be closed here or the file handle leaks
                // (caught by LeakFS in CI).
                try {
                    schemaFieldIndex = validateDeclaredHeaderBinding(consumeHeaderLine(recordReader), readSchema, object);
                } catch (Exception e) {
                    try {
                        reader.close();
                    } catch (IOException suppressed) {
                        e.addSuppressed(suppressed);
                    }
                    throw e;
                }
            }
            if (options.headerRow() == false && declaredPathBinding) {
                // A headerless file's physical names ARE positions (col4 -> field 4), so binding needs no file
                // content and runs on EVERY split — macro-splits past the first stay correctly bound.
                schemaFieldIndex = declaredPathFieldIndexes(readSchema, null, object);
            } else if (options.headerRow() && declaredPathBinding && context.firstSplit() == false) {
                // The split gate (declaredNameBindingNeedsFileStart) must keep a headered by-name read whole-file;
                // if a non-first split reaches here the gate failed, so fail loudly rather than mis-bind positionally.
                throw new IllegalStateException(
                    "headered path-bound read of ["
                        + object.path()
                        + "] reached a non-first split; the declared-name split gate did not hold"
                );
            }
            warnAbsentDeclaredColumns(schemaFieldIndex, readSchema, context.informationalWarningSink());
            effectiveSchema = readSchema;
        } else if (context.firstSplit()) {
            // resolvedSchema from withSchema(...) is the projected output, not the file's column
            // layout — using it as positional schema would mis-align columns. Only trust it when
            // recordAligned=true (streaming-parallel pre-bound the FULL file schema from chunk 0).
            if (context.recordAligned() && resolvedSchema != null) {
                if (options.headerRow()) {
                    consumeHeaderLine(recordReader);
                }
                effectiveSchema = resolvedSchema;
            } else {
                effectiveSchema = null;
            }
        } else if (context.recordAligned()) {
            // Streaming-parallel chunk sliced on a record boundary; no partial line, no header.
            effectiveSchema = resolvedSchema;
        } else {
            // Byte-range macro-split (bzip2 / zstd-indexed); leading partial record was emitted by
            // the prior split.
            try {
                recordReader.readRecord(
                    options.multiValueSyntax() == CsvFormatOptions.MultiValueSyntax.BRACKETS && options.delimiter() == ','
                );
            } catch (CsvRecordTooLargeException e) {
                if (effective.isStrict()) {
                    throw e;
                }
            }
            effectiveSchema = resolvedSchema;
        }
        // mtime is pinned here at open-time so a mid-scan file replacement cannot pair a new
        // mtime with old data. Two cacheable shapes:
        // - wholeFileRead: first + last split, no parallel slicing — iterator publishes a full
        // SourceStatistics for the file.
        // - parallel-parsing chunk (recordAligned=true): iterator publishes a partial keyed by
        // the file path; the ParallelParsingCoordinator publishes a finalize marker at clean
        // whole-file completion. Coordinator-side reconciliation only commits the merge when
        // the finalize marker is present.
        boolean wholeFileRead = context.firstSplit() && context.recordAligned() == false && context.lastSplit();
        boolean chunkMode = context.recordAligned();
        boolean cacheable = wholeFileRead || chunkMode;
        long pinnedMtimeMillis = -1L;
        if (cacheable) {
            try {
                Instant openMtime = object.lastModified();
                if (openMtime != null) {
                    pinnedMtimeMillis = openMtime.toEpochMilli();
                } else {
                    cacheable = false;
                    wholeFileRead = false;
                    chunkMode = false;
                }
            } catch (IOException e) {
                cacheable = false;
                wholeFileRead = false;
                chunkMode = false;
            }
        }
        // Fingerprint is computed lazily in CsvBatchIterator.close() once the schema is resolved
        // (effectiveSchema is often null here for the firstSplit cold-resolve path).
        return new CsvBatchIterator(
            reader,
            recordReader,
            stream,
            context.projectedColumns(),
            context.batchSize(),
            effectiveSchema,
            schemaFieldIndex,
            effective,
            object.path().toString(),
            cacheable ? object : null,
            cacheable ? stream : null,
            pinnedMtimeMillis,
            chunkMode,
            counters,
            useDirectBlockPlain,
            useDirectBlockQuoted,
            context.splitStartByte(),
            chunkMode ? context.statsStripeSize() : -1L,
            context.statsFileFinal(),
            context.statsColumnScope(),
            context.informationalWarningSink()
        );
    }

    /**
     * Returns an immutable typed snapshot of the CSV reader's counters for the operator-status
     * envelope. Zero-valued counters when no batches have run.
     */
    @Override
    public CsvReaderStatus statusSnapshot() {
        return counters.snapshot();
    }

    @Override
    public RecordSplitter recordSplitter() {
        return recordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES);
    }

    @Override
    public RecordSplitter recordSplitter(int maxRecordBytes) {
        // Splitter chosen once, from the (quoting, escaping) pair - never a per-byte branch. Only plain
        // data (no quoting, no escaping: every byte literal) has raw line terminators that are always
        // record boundaries, so only it can use the cheap strided terminator scan. When quoting is on a
        // raw newline can sit inside a quoted field, and when escaping is on a backslash-escaped raw
        // newline is in-field content; either way boundary detection needs the quote/escape state machine
        // and must be driven sequentially from a known record start.
        if (options.quoting() == false && options.escaping() == false) {
            return new NewlineRecordSplitter(maxRecordBytes);
        }
        return new CsvRecordSplitter(options, maxRecordBytes);
    }

    static boolean isAsciiCsvFieldLeadingWhitespace(int ib) {
        return ib == ' ' || ib == '\t' || ib == '\f';
    }

    @Override
    public String formatName() {
        return format;
    }

    @Override
    public List<String> fileExtensions() {
        return extensions;
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return new TextAggregatePushdownSupport();
    }

    @Override
    public RowPositionStrategy rowPositionStrategy() {
        // CSV's per-batch CsvBatchIterator fills the {@code _rowPosition} slot natively from the
        // file-global byte offset computed at record-read time (see {@link CsvBatchIterator#rowStartBytes}).
        return PassThroughRowPositionStrategy.INSTANCE;
    }

    @Override
    public void close() throws IOException {}

    /**
     * Consumes one header line from {@code reader}, skipping over leading empty lines and
     * comment lines, and returns it ({@code null} when the input has no non-comment line).
     * Used by {@link #read} when a schema is already bound but the input split still starts
     * with the file header.
     */
    private String consumeHeaderLine(CsvLogicalRecordReader recordReader) throws IOException {
        String record;
        while ((record = recordReader.readRecord(false)) != null) {
            String trimmed = record.trim();
            if (trimmed.isEmpty() || (options.commentPrefix().isEmpty() == false && trimmed.startsWith(options.commentPrefix()))) {
                continue;
            }
            return record;
        }
        return null;
    }

    /**
     * Width tripwire for an externally-supplied (declared) positional schema against the file's actual header.
     *
     * <p>A declared schema binds text columns <b>positionally</b>: the declared names replace the header's names in
     * order (the same contract as DuckDB {@code columns=} / ClickHouse {@code structure}), so declared names are NOT
     * cross-checked against header names — renaming by position is intended. What CAN be checked is width: a
     * declaration WIDER than the file's header means the file cannot supply the declared columns (a drifted file, or
     * the wrong file entirely) — fail loudly at the first read instead of null-splicing every row. Fewer declared
     * columns than the header is allowed: the declaration binds the leading columns and the rest stay unread.
     *
     * <p>When a {@code path} WAS declared ({@link #declaredPathBinding}), the declaration no longer binds
     * positionally: it names its columns, so this returns the raw field index each declared column reads and the
     * width tripwire does not apply (naming {@code col100} of a 105-column file is legitimate with 1 declared
     * column). Returns {@code null} otherwise — the caller then keeps the positional contract.
     *
     * @return the raw field index per {@code readSchema} position, or {@code null} for positional binding
     */
    private int[] validateDeclaredHeaderBinding(String headerLine, List<Attribute> readSchema, StorageObject object) {
        if (headerLine == null) {
            return null; // empty file — nothing to validate, and nothing to read
        }
        String[] fields = splitFieldsForOptions(headerLine, options);
        if (declaredPathBinding) {
            String[] headerNames = headerColumnNames(headerLine, fields);
            rejectDuplicateHeaderNames(headerNames, object);
            return declaredPathFieldIndexes(readSchema, headerNames, object);
        }
        if (readSchema.size() > fields.length) {
            throw new IllegalArgumentException(
                "declared schema has "
                    + readSchema.size()
                    + " columns but the header of ["
                    + object.path()
                    + "] has "
                    + fields.length
                    + "; a declared schema binds text columns in order (for a headerless file set header_row=false)"
            );
        }
        return null;
    }

    /**
     * The header's column NAMES, derived exactly the way the inference path derives them: a typed header
     * ({@code emp_no:integer}) contributes {@code emp_no}, and a bare header is unquoted per the dialect. Routing both
     * through the same derivation is what lets a declared {@code path} name the same column that {@code dynamic:true}
     * would expose — any divergence here would recreate the strict-vs-dynamic split this binding exists to close.
     *
     * @param fields the already-split header fields, parallel to the returned names
     */
    private String[] headerColumnNames(String headerLine, String[] fields) {
        List<Attribute> typed = parseSchema(headerLine); // non-null iff the header carries type annotations
        String[] names = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            if (typed != null && i < typed.size()) {
                names[i] = typed.get(i).name();
            } else {
                names[i] = options.quoting() ? unquoteHeaderName(fields[i], options.quoteChar()).trim() : fields[i].trim();
            }
        }
        return names;
    }

    private List<Attribute> parseSchema(String schemaLine) {
        String[] columns = splitFieldsForOptions(schemaLine, options);
        if (hasTypeAnnotations(columns)) {
            return parseTypedSchema(columns);
        }
        return null;
    }

    private boolean hasTypeAnnotations(String[] columns) {
        char quote = options.quoteChar();
        for (String column : columns) {
            String trimmed = column.trim();
            if (trimmed.length() >= 2 && trimmed.charAt(0) == quote && trimmed.charAt(trimmed.length() - 1) == quote) {
                continue;
            }
            if (trimmed.contains(":")) {
                return true;
            }
        }
        return false;
    }

    private List<Attribute> parseTypedSchema(String[] columns) {
        List<Attribute> attributes = new ArrayList<>(columns.length);
        for (String column : columns) {
            String trimmedColumn = column.trim();
            String[] parts = trimmedColumn.split(":");
            if (parts.length != 2) {
                throw new ParsingException("Invalid CSV schema format: [{}]. Expected 'name:type'", column);
            }
            String name = options.quoting() ? unquoteHeaderName(parts[0], options.quoteChar()) : parts[0].trim();
            String trimmedType = parts[1].trim();
            String typeName = trimmedType.toUpperCase(Locale.ROOT);
            DataType dataType = parseDataType(typeName);
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, name, dataType, Nullability.TRUE, null, false));
        }
        return attributes;
    }

    /**
     * Parse CSV type names to ESQL DataType. Small numeric types (SHORT, BYTE, FLOAT, etc.)
     * are widened to INTEGER/DOUBLE since the planner expects widened types.
     * Typed-schema aliases TEXT and TXT map to KEYWORD (same string family as KEYWORD/STRING).
     */
    private DataType parseDataType(String typeName) {
        String upper = typeName.toUpperCase(Locale.ROOT);
        return switch (upper) {
            // Widened to INTEGER/DOUBLE: ESQL's compute engine lacks native blocks for these small numeric types.
            // Remove widening once https://github.com/elastic/elasticsearch/issues/112691 lands.
            case "SHORT", "BYTE" -> DataType.INTEGER;
            case "INTEGER", "INT", "I" -> DataType.INTEGER;
            case "LONG", "L" -> DataType.LONG;
            case "UNSIGNED_LONG", "UL" -> DataType.UNSIGNED_LONG;
            case "FLOAT", "F", "HALF_FLOAT", "SCALED_FLOAT" -> DataType.DOUBLE;
            case "DOUBLE", "D" -> DataType.DOUBLE;
            case "KEYWORD", "K", "STRING", "S", "TEXT", "TXT" -> DataType.KEYWORD;
            case "BOOLEAN", "BOOL" -> DataType.BOOLEAN;
            case "DATETIME", "DATE", "DT" -> DataType.DATETIME;
            case "DATE_NANOS", "DN" -> DataType.DATE_NANOS;
            case "IP" -> DataType.IP;
            case "VERSION", "V" -> DataType.VERSION;
            case "NULL", "N" -> DataType.NULL;
            default -> throw EsqlIllegalArgumentException.illegalDataType(typeName);
        };
    }

    /**
     * Whether {@code current} contains only whitespace — treated like an empty field prefix so a following {@code [}
     * still opens bracket MVC parsing (mirrors parallel record-boundary scanning).
     */
    private static boolean isWhitespaceOnlyFieldPrefix(StringBuilder current) {
        for (int k = 0; k < current.length(); k++) {
            if (Character.isWhitespace(current.charAt(k)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns {@code true} if {@code line} is blank (whitespace-only or empty) or starts with
     * the given comment prefix after skipping leading whitespace. Scans at most
     * {@code line.length()} characters and allocates nothing — unlike
     * {@code line.trim().isEmpty() || line.trim().startsWith(prefix)} which always creates a
     * new {@link String}.
     */
    static boolean isBlankOrComment(String line, String commentPrefix) {
        int len = line.length();
        int firstNonWs = 0;
        while (firstNonWs < len && line.charAt(firstNonWs) <= ' ') {
            firstNonWs++;
        }
        if (firstNonWs == len) {
            return true;
        }
        return commentPrefix != null
            && commentPrefix.isEmpty() == false
            && firstNonWs + commentPrefix.length() <= len
            && line.regionMatches(firstNonWs, commentPrefix, 0, commentPrefix.length());
    }

    /**
     * Blank/comment classification for the direct-to-block path. A line is blank when it is empty or
     * all whitespace. A line is a comment when its first cell, as Jackson would parse it, trimmed,
     * starts with {@code commentPrefix}. Jackson decides on the first parsed cell, so unlike
     * {@link #isBlankOrComment} the prefix match is bounded to the region before the first delimiter:
     * a leading delimiter (for example a TAB in TSV) yields an empty first cell, which is not a comment.
     *
     * <p>One rare case is not matched here: a quoted or escaped first cell whose decoded content
     * begins with the prefix (for example {@code "//x",a}). Detecting that needs field decoding, so
     * the quoted direct path handles it separately via {@code decodedFirstCellIsComment} (guarded by
     * {@code firstCellMayDecodeToComment} so the common, decode-insensitive first cell stays on this
     * cheap raw check).
     */
    static boolean isBlankOrCommentFirstCell(char[] buf, int from, int to, String commentPrefix, char delim) {
        int firstNonWs = from;
        while (firstNonWs < to && buf[firstNonWs] <= ' ') {
            firstNonWs++;
        }
        if (firstNonWs == to) {
            return true;
        }
        if (commentPrefix == null || commentPrefix.isEmpty()) {
            return false;
        }
        int cellEnd = to;
        for (int k = from; k < to; k++) {
            if (buf[k] == delim) {
                cellEnd = k;
                break;
            }
        }
        int prefixLen = commentPrefix.length();
        if (firstNonWs >= cellEnd || firstNonWs + prefixLen > cellEnd) {
            return false;
        }
        return regionEquals(buf, firstNonWs, commentPrefix);
    }

    /** True if {@code buf[start, start+s.length())} equals {@code s} (case-sensitive). Callers ensure the range fits. */
    static boolean regionEquals(char[] buf, int start, String s) {
        for (int k = 0; k < s.length(); k++) {
            if (buf[start + k] != s.charAt(k)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads a single logical CSV record from {@code reader}, tracking quote and bracket state so
     * that {@code \r} and {@code \n} inside quoted fields or bracket-MVC cells are preserved
     * verbatim as field content. Only {@code \n} (or {@code \r\n}) <em>outside</em> quoted/bracket
     * context terminates the record — lone {@code \r} outside quotes also terminates (for
     * {@code \r}-only line-ending files) but is not included in the returned string.
     *
     * <p>Mirrors the quoting contract of {@link CsvRecordSplitter}.
     *
     * <p>Uses the same ASCII-only whitespace predicate ({@code ' '}, {@code '\t'}, {@code '\f'}) as
     * the boundary scanners for field-start detection, via {@link #isAsciiCsvFieldLeadingWhitespace}.
     *
     * @return the logical record (without trailing line terminator), or {@code null} at EOF
     */
    static String readCsvRecord(Reader reader, char quoteChar, char delimiter, boolean bracketAware) throws IOException {
        return readCsvRecord(reader, quoteChar, delimiter, bracketAware, CsvFormatOptions.DEFAULT.encoding());
    }

    static String readCsvRecord(Reader reader, char quoteChar, char delimiter, boolean bracketAware, Charset encoding) throws IOException {
        return new CsvLogicalRecordReader(reader, quoteChar, delimiter, SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES, encoding)
            .readRecord(bracketAware);
    }

    /**
     * Converts accumulated field content to a string. When {@code trimSpaces} is set, surrounding
     * whitespace is trimmed, with a fast path that skips {@link String#trim()} when the first and last
     * characters are already non-whitespace (the common case for clean CSV data); when it is not set the
     * content is returned verbatim (the default — RFC 4180 keeps surrounding spaces).
     */
    static String emitField(StringBuilder current, boolean trimSpaces) {
        String s = current.toString();
        if (trimSpaces == false) {
            return s;
        }
        int len = s.length();
        if (len == 0 || (s.charAt(0) > ' ' && s.charAt(len - 1) > ' ')) {
            return s;
        }
        return s.trim();
    }

    /**
     * Whether {@code line} starting at {@code openBracketIndex} contains a balanced bracket suffix that closes the
     * MVC cell. Only {@code [} and {@code ]} adjust depth — quote/escape/delimiter characters inside the bracket
     * cell are treated as literal data (matching the splitter's {@code bracketDepth > 0} branch). A stray {@code "}
     * or {@code \} inside the cell is data, not a quote toggle — otherwise real-world rows like
     * {@code [text",1,2013-...,38,-12345]} would look unclosed here, the splitter would treat the leading {@code [}
     * as literal, and the inner commas would become delimiters, yielding extra columns and the
     * {@code "row has [N+k] columns but schema defines [N]"} failure.
     */
    private static boolean hasMvcBracketClose(String line, int openBracketIndex) {
        if (openBracketIndex < 0 || openBracketIndex >= line.length() || line.charAt(openBracketIndex) != '[') {
            return false;
        }
        int depth = 0;
        for (int j = openBracketIndex; j < line.length(); j++) {
            char c = line.charAt(j);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Splits a <strong>header</strong> line into fields. A quoting dialect (CSV) tokenizes the header by the
     * same delimiter/quote/escape awareness as data cells — so a quoted name, a quoted field containing the
     * delimiter, and RFC 4180 {@code ""} all round-trip the way they do for data — while <em>preserving the
     * original substring</em> of each field, quotes included: header fields like {@code "host:port"} keep their
     * quotes so {@link #hasTypeAnnotations} can tell a quoted name from a {@code name:type} annotation (the
     * name is unquoted later, once that decision has been made). A non-quoting dialect ({@code mode: plain} /
     * {@code escaped}) treats a quote as literal data, so the raw delimiter split is correct there.
     */
    private static String[] splitFieldsForOptions(String line, CsvFormatOptions options) {
        // The header is the file's first line, so a UTF-8 BOM (Excel/Windows) lands on its first field.
        line = stripLeadingBom(line);
        if (options.quoting()) {
            return splitHeaderQuoteAware(
                line,
                options.delimiter(),
                options.quoteChar(),
                options.escapeChar(),
                options.multiValueSyntax() == CsvFormatOptions.MultiValueSyntax.BRACKETS
            );
        }
        // A quote/escape-aware split, not a naive line.split(delimiter): a quoted field may embed the
        // delimiter (a header "a,b",c is two columns, not three), and the read-side Jackson parser honours
        // quoting — so a naive split mis-counts the header width and (Julian's report) the declared-schema
        // width tripwire wrongly rejects or admits a file. Mirrors splitHeaderQuoteAware for the non-quoting
        // delimiters (comma, tab).
        return splitFieldsQuoteAware(line, options.delimiter(), options.quoteChar(), options.escapeChar());
    }

    /**
     * General quote- and escape-aware field split for a single header/schema line over an arbitrary
     * delimiter (comma, tab). A quote opens only at a field boundary (leading, ignoring whitespace);
     * a doubled quote inside a quoted field is a literal quote; an escape char before the delimiter
     * inside quotes keeps the delimiter literal. Fields are trimmed, matching
     * {@link #splitHeaderQuoteAware}; quotes are retained in the token exactly as that quote-aware
     * sibling retains them, so the two paths agree on width and shape.
     */
    private static String[] splitFieldsQuoteAware(String line, char delim, char quote, char esc) {
        List<String> entries = new ArrayList<>();
        int start = 0;
        boolean inQuotes = false;
        boolean fieldHasNonWhitespace = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == quote) {
                    if (i + 1 < line.length() && line.charAt(i + 1) == quote) {
                        i++; // doubled quote inside a quoted field is a literal quote
                        continue;
                    }
                    inQuotes = false;
                } else if (c == esc && i + 1 < line.length() && line.charAt(i + 1) == delim) {
                    i++; // escaped delimiter inside quotes stays literal
                }
                continue;
            }
            if (c == delim) {
                entries.add(line.substring(start, i).trim());
                start = i + 1;
                fieldHasNonWhitespace = false;
                continue;
            }
            if (c == quote && fieldHasNonWhitespace == false) {
                inQuotes = true;
                continue;
            }
            if (Character.isWhitespace(c) == false) {
                fieldHasNonWhitespace = true;
            }
        }
        entries.add(line.substring(start).trim());
        return entries.toArray(String[]::new);
    }

    /**
     * Header-only variant of {@link #splitCommaDelimiterBracketAwareFields}: tracks the same quote/escape
     * (and, when {@code bracketsMode}, bracket) state machine but emits the raw substring between delimiters
     * instead of accumulating into a {@link StringBuilder} that strips quotes — the caller unquotes the
     * resolved names after {@link #hasTypeAnnotations} has run. Used by schema discovery / inference for any
     * delimiter (comma for CSV, tab for TSV).
     */
    private static String[] splitHeaderQuoteAware(String line, char delim, char quote, char esc, boolean bracketsMode) {
        List<String> entries = new ArrayList<>();
        int start = 0;
        boolean inQuotes = false;
        int bracketDepth = 0;
        boolean fieldHasNonWhitespace = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == quote) {
                    if (i + 1 < line.length() && line.charAt(i + 1) == quote) {
                        i++;
                        continue;
                    }
                    inQuotes = false;
                } else if (c == esc && i + 1 < line.length() && line.charAt(i + 1) == delim) {
                    i++;
                }
                continue;
            }
            if (bracketDepth > 0) {
                if (c == '[') {
                    bracketDepth++;
                } else if (c == ']') {
                    bracketDepth--;
                }
                continue;
            }
            // A delimiter outside quotes/brackets ends the field. Unlike the data splitter, an escaped
            // delimiter OUTSIDE quotes (e.g. a\,b in an escaping dialect) is not un-escaped here — an exotic
            // header shape whose full header/data parity belongs to the shared-tokenizer follow-up. Quoting
            // ("a,b") is the RFC 4180 way to carry a delimiter in a header name and is handled above.
            if (c == delim) {
                entries.add(line.substring(start, i).trim());
                start = i + 1;
                fieldHasNonWhitespace = false;
                continue;
            }
            if (c == quote && fieldHasNonWhitespace == false) {
                inQuotes = true;
                continue;
            }
            if (bracketsMode && c == '[' && fieldHasNonWhitespace == false && hasMvcBracketClose(line, i)) {
                bracketDepth = 1;
                continue;
            }
            if (Character.isWhitespace(c) == false) {
                fieldHasNonWhitespace = true;
            }
        }
        entries.add(line.substring(start).trim());
        // Match String.split's default (limit 0), which the plain/escaped header path still uses: drop
        // trailing empty fields so a trailing delimiter ("a,b,") yields the same column count everywhere,
        // not a spurious empty-named last column.
        int last = entries.size();
        while (last > 0 && entries.get(last - 1).isEmpty()) {
            last--;
        }
        return entries.subList(0, last).toArray(String[]::new);
    }

    /** Leading UTF-8 byte-order mark; decoded to this char by the {@link InputStreamReader} before it reaches us. */
    private static final char BOM = '\uFEFF';

    /**
     * Strips a leading byte-order mark from the first line of a file. Excel/Windows CSV exports prepend a
     * UTF-8 BOM ({@code EF BB BF}); without this the BOM would otherwise prefix the first column name.
     */
    private static String stripLeadingBom(String line) {
        return line != null && line.isEmpty() == false && line.charAt(0) == BOM ? line.substring(1) : line;
    }

    /**
     * Strips one layer of surrounding {@code quote} characters from a resolved header name and unescapes RFC
     * 4180 doubled quotes ({@code ""} -> {@code "}), so an unquoted {@code id} and a quoted {@code "id"} both
     * resolve to {@code id}. A name that is not fully quoted is returned trimmed and otherwise unchanged.
     * Only meaningful for a quoting dialect; the caller does not invoke it for {@code plain}/{@code escaped},
     * where a quote is literal name data.
     */
    private static String unquoteHeaderName(String name, char quote) {
        String trimmed = name.trim();
        if (trimmed.length() >= 2 && trimmed.charAt(0) == quote && trimmed.charAt(trimmed.length() - 1) == quote) {
            String inner = trimmed.substring(1, trimmed.length() - 1);
            return inner.replace(String.valueOf(quote) + quote, String.valueOf(quote));
        }
        return trimmed;
    }

    /** Applies {@link #unquoteHeaderName} to each name in place; called on the inference path when quoting. */
    private static void unquoteHeaderNames(String[] names, char quote) {
        for (int i = 0; i < names.length; i++) {
            names[i] = unquoteHeaderName(names[i], quote);
        }
    }

    /**
     * Bracket- and quote-aware comma split; must stay aligned with {@link CsvBatchIterator#splitLineBracketAware}.
     * {@code schemaColumnCount} lets the end-of-line handling reproduce the fused walker's trailing present-empty
     * rule: a row-ending delimiter inside the schema yields a present empty field, beyond it a lone trailing
     * delimiter is dropped.
     */
    private static String[] splitCommaDelimiterBracketAwareFields(
        String line,
        char quote,
        char esc,
        int schemaColumnCount,
        boolean trimSpaces
    ) {
        final char delim = ',';
        List<String> entries = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        // Mirrors splitAndConvertProjected: true once the current (last) field has been started by
        // content, an opening quote, or an opening bracket. It distinguishes a trailing field that was
        // opened but yielded no text (a quoted empty `""`, a present empty field) from a genuinely
        // absent field, so both bracket routes agree at end of line.
        boolean trailingFieldHasContent = false;
        int bracketDepth = 0;
        // Remember where the parser entered the unclosed state so error messages can anchor on
        // the actual fault site instead of head/tail-truncating a long line and hiding it.
        int quoteOpenAt = -1;
        int bracketOpenAt = -1;
        int i = 0;
        while (i < line.length()) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == quote) {
                    if (i + 1 < line.length() && line.charAt(i + 1) == quote) {
                        current.append(quote);
                        i += 2;
                        continue;
                    }
                    inQuotes = false;
                    // Trailing whitespace between the closing quote and the delimiter/EOL is not part of the value
                    // (matches the direct quoted walker), so skip it; otherwise no-trim bracket mode keeps it
                    // ("y"+ws -> value+ws) while the quoted grammar drops it. Non-whitespace after the closing
                    // quote is still concatenated; bracket mode is lenient there by design.
                    int j = i + 1;
                    while (j < line.length() && line.charAt(j) <= ' ') {
                        j++;
                    }
                    if (j == line.length() || line.charAt(j) == delim) {
                        i = j;
                        continue;
                    }
                } else if (c == esc && i + 1 < line.length() && line.charAt(i + 1) == delim) {
                    current.append(delim);
                    i += 2;
                    continue;
                } else {
                    current.append(c);
                }
                i++;
            } else if (bracketDepth > 0) {
                // Inside an MVC cell: only `[` and `]` adjust depth; quotes and the field delimiter are literal.
                // When `]` brings depth back to zero we deliberately keep `current` and fall through to the regular
                // text-accumulation branch on subsequent iterations: real-world rows like `[37] Title text,...`
                // mean "[37] Title text" is one field. Closing the cell here would split off the trailing text
                // into a phantom extra column, which is exactly the "row has [N+1] columns" failure.
                trailingFieldHasContent = true;
                current.append(c);
                if (c == '[') {
                    bracketDepth++;
                } else if (c == ']') {
                    bracketDepth--;
                }
                i++;
            } else if (c == quote && (current.length() == 0 || isWhitespaceOnlyFieldPrefix(current))) {
                trailingFieldHasContent = true;
                // Drop any whitespace-only prefix accumulated before the opening quote so ` "y"` yields
                // `y`, matching the direct quoted walker (splitAndConvertQuoted skips outer whitespace before
                // quote detection). Behavior-neutral under trim_spaces (emitField would have trimmed it).
                current.setLength(0);
                inQuotes = true;
                quoteOpenAt = i;
                i++;
            } else if (c == '[' && (current.length() == 0 || isWhitespaceOnlyFieldPrefix(current))) {
                trailingFieldHasContent = true;
                if (hasMvcBracketClose(line, i)) {
                    bracketDepth = 1;
                    bracketOpenAt = i;
                }
                current.append(c);
                i++;
            } else if (c == delim) {
                if (i > 0 && line.charAt(i - 1) == esc) {
                    current.append(c);
                } else {
                    entries.add(emitField(current, trimSpaces));
                    current = new StringBuilder();
                    trailingFieldHasContent = false;
                }
                i++;
            } else {
                trailingFieldHasContent = true;
                current.append(c);
                i++;
            }
        }
        if (inQuotes) {
            throw MalformedRowException.unclosedQuotedField(line, quoteOpenAt);
        }
        if (bracketDepth > 0) {
            throw MalformedRowException.unclosedBracketCell(line, bracketOpenAt);
        }
        if (current.length() > 0) {
            entries.add(emitField(current, trimSpaces));
        } else if (trailingFieldHasContent) {
            // The trailing field was opened but yielded no text (e.g. a quoted empty `""`): a present
            // empty field. Not bounded by schemaColumnCount: an over-count is caught by column-count
            // validation downstream, matching the fused walker.
            entries.add("");
        } else if (isPresentTrailingEmpty(entries.size(), schemaColumnCount)) {
            // The line ended on a field-closing delimiter (e.g. `a,b,`): a present empty trailing field
            // that falls inside the schema. Beyond the schema a lone trailing delimiter is dropped (lenient).
            entries.add("");
        }
        return entries.toArray(String[]::new);
    }

    /**
     * The detail portion of the over-max-field-size error, byte-for-byte the text Jackson's
     * {@code StreamReadConstraints.maxStringLength} violation produces. Shared by the direct-block
     * walker ({@link CsvBatchIterator#rejectFieldTooLarge}, which prefixes {@code "CSV parse error: "})
     * and the house record tokenizer ({@link #splitRecordFields}, which throws it as a
     * {@link MalformedRowException} that the batch loop re-prefixes identically), so both arms emit the
     * same message. ASCII digits on purpose (locale-independent), unlike Jackson's {@code FORMAT}-locale
     * number formatting.
     */
    static String fieldSizeExceededDetail(int valueLen, int maxFieldChars) {
        return "String value length ("
            + valueLen
            + ") exceeds the maximum allowed ("
            + maxFieldChars
            + ", from `StreamReadConstraints.getMaxStringLength()`)";
    }

    /**
     * House record tokenizer for the no-trim, non-escaped-mode dialects (QUOTED and PLAIN). Produces the
     * same field values <em>and</em> field counts as the direct-to-block walkers
     * ({@link CsvBatchIterator#splitAndConvertPlain} / {@link CsvBatchIterator#splitAndConvertQuoted}), so
     * a record materialized through this splitter agrees byte-for-byte with a direct read of the same file.
     * Used in place of Jackson whenever {@link #jacksonGrammarApplies()} is false — Jackson's tokenization
     * only coincides with the walkers under {@code trim_spaces} (see that method), and it eats first-column
     * leading whitespace on every row via {@code SKIP_EMPTY_LINES}, so under no-trim the walkers are the
     * grammar and this splitter mirrors them for the record-materialized paths (per-record iterator,
     * inference sampling, {@code _rowPosition} reads, bulk fallback).
     *
     * <p>Values are returned raw: an empty field is {@code ""} (not {@code null}) — downstream
     * {@code tryConvertValue} maps empty / {@code null-marker} to null identically for both arms, so the
     * split stays a pure tokenizer. Per field the same {@code maxFieldChars} cap the walkers enforce is
     * applied, throwing a {@link MalformedRowException} whose message equals
     * {@link #fieldSizeExceededDetail} so the error policy sees identical text on both arms.
     *
     * <p>Only reached when {@code decodeFieldValue} is the identity (QUOTED or PLAIN); the escaped mode
     * (quoting off, escaping on), where {@code decodeFieldValue} is non-identity, keeps
     * {@link #jacksonGrammarApplies()} true and never routes here.
     */
    static String[] splitRecordFields(String record, CsvFormatOptions options, int maxFieldChars) {
        return options.quoting()
            ? splitRecordFieldsQuoted(record, options, maxFieldChars)
            : splitRecordFieldsPlain(record, options, maxFieldChars);
    }

    /**
     * PLAIN (unquoted) split: walk on the delimiter, emitting one field per delimiter and one for the
     * trailing segment (so a trailing delimiter yields an empty last field, exactly like
     * {@link CsvBatchIterator#splitAndConvertPlain}'s {@code from..to} inclusive scan). A quote byte is
     * literal data. No first-column whitespace is eaten (the col-0 fix for PLAIN dialects).
     */
    private static String[] splitRecordFieldsPlain(String record, CsvFormatOptions options, int maxFieldChars) {
        final char delim = options.delimiter();
        final boolean trimSpaces = options.trimSpaces();
        final int len = record.length();
        List<String> fields = new ArrayList<>();
        int start = 0;
        for (int i = 0; i <= len; i++) {
            if (i == len || record.charAt(i) == delim) {
                fields.add(emitPlainSplitField(record, start, i, trimSpaces, maxFieldChars));
                start = i + 1;
            }
        }
        return fields.toArray(String[]::new);
    }

    /**
     * Extracts the value of a simple (unquoted, unescaped) field {@code record[start, end)}, mirroring
     * {@link CsvBatchIterator#emitPlainField}'s whitespace and cap semantics: under {@code trim_spaces} the
     * value is trimmed and the cap governs the trimmed length; otherwise the raw span is kept verbatim and
     * the cap governs the raw length. Type-specific trimming (a typed column always parses trimmed) is left
     * to the shared {@code tryConvertValue} downstream, so this stays type-agnostic.
     */
    private static String emitPlainSplitField(String record, int start, int end, boolean trimSpaces, int maxFieldChars) {
        if (trimSpaces) {
            while (start < end && record.charAt(start) <= ' ') {
                start++;
            }
            while (end > start && record.charAt(end - 1) <= ' ') {
                end--;
            }
        }
        int fieldLen = end - start;
        if (fieldLen > maxFieldChars) {
            throw new MalformedRowException(fieldSizeExceededDetail(fieldLen, maxFieldChars));
        }
        return record.substring(start, end);
    }

    /**
     * QUOTED (RFC 4180, optional backslash escapes) split — the string-domain twin of
     * {@link CsvBatchIterator#splitAndConvertQuoted}. A field-leading quote (after optional outer
     * whitespace, never the delimiter itself even when it is a whitespace byte such as TAB) opens a quoted
     * region where {@code ""} is a literal quote and, when escaping is on, {@code \}+char is decoded with Jackson's quoted-escape rule;
     * quoted content (including inner whitespace) is kept verbatim, an empty quoted field is {@code ""}
     * (downstream null), and only whitespace may follow a closing quote before the delimiter. Unquoted
     * fields are extracted by {@link #emitPlainSplitField} (no escape) or {@link #emitUnquotedEscapedSplitField}
     * (escape present). An unclosed quote throws {@link MalformedRowException#unclosedQuotedField}.
     */
    private static String[] splitRecordFieldsQuoted(String record, CsvFormatOptions options, int maxFieldChars) {
        final char delim = options.delimiter();
        final char quote = options.quoteChar();
        final char esc = options.escapeChar();
        final boolean escapeAware = options.escaping();
        final boolean trimSpaces = options.trimSpaces();
        final int len = record.length();
        List<String> fields = new ArrayList<>();
        int i = 0;
        while (true) {
            // Skip leading outer whitespace to decide quoted vs unquoted. The delimiter is never skipped
            // even when it is itself a whitespace byte (a TAB in TSV), so an empty field before a quoted
            // field is not mistaken for the quoted field. The unquoted branch re-scans from i (not p) so
            // that leading whitespace stays part of an unquoted field.
            int p = i;
            while (p < len && record.charAt(p) <= ' ' && record.charAt(p) != delim) {
                p++;
            }
            boolean lastField;
            int next;
            if (p < len && record.charAt(p) == quote) {
                StringBuilder value = new StringBuilder();
                int decodedLen = 0;
                int q = p + 1;
                boolean closed = false;
                while (q < len) {
                    char c = record.charAt(q);
                    if (c == quote) {
                        if (q + 1 < len && record.charAt(q + 1) == quote) {
                            value.append(quote);
                            decodedLen++;
                            q += 2;
                            continue;
                        }
                        closed = true;
                        q++;
                        break;
                    }
                    if (escapeAware && c == esc) {
                        if (q + 1 < len) {
                            value.append(decodeQuotedEscapeChar(record.charAt(q + 1)));
                            decodedLen++;
                            q += 2;
                        } else {
                            q++; // trailing lone escape: dropped
                        }
                        continue;
                    }
                    value.append(c);
                    decodedLen++;
                    q++;
                }
                if (closed == false) {
                    throw MalformedRowException.unclosedQuotedField(record, p);
                }
                // Jackson checks maxStringLength on the aggregated value right after the closing quote,
                // before inspecting trailing content, so the cap precedes the content-after-quote check.
                if (decodedLen > maxFieldChars) {
                    throw new MalformedRowException(fieldSizeExceededDetail(decodedLen, maxFieldChars));
                }
                int r = q;
                while (r < len && record.charAt(r) <= ' ' && record.charAt(r) != delim) {
                    r++;
                }
                if (r < len && record.charAt(r) != delim) {
                    throw new MalformedRowException("CSV row has unexpected content after a closing quote");
                }
                fields.add(value.toString());
                lastField = r >= len;
                next = r + 1;
            } else {
                int j = i;
                boolean hasEsc = false;
                while (j < len) {
                    char c = record.charAt(j);
                    if (escapeAware && c == esc) {
                        hasEsc = true;
                        j += 2; // skip the escaped char (even if it is a delimiter)
                        continue;
                    }
                    if (c == delim) {
                        break;
                    }
                    j++;
                }
                int fieldEnd = Math.min(j, len);
                fields.add(
                    hasEsc
                        ? emitUnquotedEscapedSplitField(record, i, fieldEnd, options, maxFieldChars)
                        : emitPlainSplitField(record, i, fieldEnd, trimSpaces, maxFieldChars)
                );
                lastField = j >= len;
                next = fieldEnd + 1;
            }
            if (lastField) {
                break;
            }
            i = next;
        }
        return fields.toArray(String[]::new);
    }

    /**
     * Extracts an unquoted field {@code record[start, end)} that contains the escape character, the
     * string-domain twin of {@link CsvBatchIterator#emitUnquotedEscapedField}. Under {@code trim_spaces}
     * the raw leading whitespace is stripped before the decode loop and trailing decoded whitespace after
     * it (Jackson's TRIM_SPACES order), with the cap on the trimmed length. Under no-trim the whole span is
     * decoded verbatim and the cap governs the full decoded length; type-specific trimming for a typed
     * column is left to {@code tryConvertValue}. A trailing lone escape (no following char) is dropped.
     */
    private static String emitUnquotedEscapedSplitField(String record, int start, int end, CsvFormatOptions options, int maxFieldChars) {
        final char esc = options.escapeChar();
        final boolean trimSpaces = options.trimSpaces();
        if (trimSpaces) {
            while (start < end && record.charAt(start) <= ' ') {
                start++;
            }
        }
        StringBuilder value = new StringBuilder(end - start);
        for (int k = start; k < end; k++) {
            char c = record.charAt(k);
            if (c == esc) {
                if (k + 1 < end) {
                    value.append(decodeQuotedEscapeChar(record.charAt(++k)));
                }
                // else: trailing lone escape, dropped
            } else {
                value.append(c);
            }
        }
        int endLen = value.length();
        if (trimSpaces) {
            while (endLen > 0 && value.charAt(endLen - 1) <= ' ') {
                endLen--;
            }
        }
        if (endLen > maxFieldChars) {
            throw new MalformedRowException(fieldSizeExceededDetail(endLen, maxFieldChars));
        }
        return endLen == value.length() ? value.toString() : value.substring(0, endLen);
    }

    private class CsvRecordIterator implements Iterator<List<?>> {
        private final CsvLogicalRecordReader recordReader;
        private final CsvSchema csvSchema;
        private List<?> next;
        private boolean eof;

        CsvRecordIterator(CsvLogicalRecordReader recordReader, CsvSchema csvSchema) {
            this.recordReader = recordReader;
            this.csvSchema = csvSchema;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            if (eof) {
                return false;
            }
            try {
                next = readNextParsedRecord();
                eof = next == null;
                return eof == false;
            } catch (IOException e) {
                throw ExternalFailures.surface(e, "Failed to read CSV record");
            }
        }

        @Override
        public List<?> next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            List<?> result = next;
            next = null;
            return result;
        }

        private List<?> readNextParsedRecord() throws IOException {
            String record;
            while ((record = recordReader.readRecord(false)) != null) {
                List<?> row = parseRecord(record);
                if (row != null) {
                    return row;
                }
            }
            return null;
        }

        private List<String> parseRecord(String record) throws IOException {
            if (jacksonGrammarApplies() == false) {
                // No-trim, non-escaped-mode: the direct-block walkers are the grammar, so tokenize with
                // their string-domain twin instead of Jackson (whose grammar diverges under no-trim — see
                // jacksonGrammarApplies). Blank records are skipped here exactly as SKIP_EMPTY_LINES +
                // the empty-row check below do on the Jackson path; comments are filtered by the callers on
                // the first cell, so they are not dropped here. The decodeFieldValue seam runs unchanged —
                // it is the identity for the QUOTED / PLAIN dialects this branch is gated to.
                if (isBlankOrComment(record, null)) {
                    return null;
                }
                int maxFieldChars = options.maxFieldSize() > 0 ? options.maxFieldSize() : Integer.MAX_VALUE;
                String[] fields = splitRecordFields(record, options, maxFieldChars);
                // A configured null marker maps to null here, mirroring what Jackson's withNullValue did at
                // tokenization on the pre-B1 path and what the direct walkers' emitPlainField / tryConvertValue
                // do (exact equality, gated on a non-empty marker, all fields incl. quoted — splitRecordFields
                // strips quotes just as Jackson nulled a quoted "NA"). Without this the raw marker would reach
                // CsvSchemaInferrer, which only knows empty / "null", and flip the inferred type. The default
                // empty marker stays untouched: an empty cell already reads as null downstream.
                boolean hasCustomNullValue = options.nullValue().isEmpty() == false;
                String nullValueStr = options.nullValue();
                List<String> row = new ArrayList<>(fields.length);
                for (String field : fields) {
                    String decoded = decodeFieldValue(field);
                    row.add(hasCustomNullValue && nullValueStr.equals(decoded) ? null : decoded);
                }
                return row.isEmpty() ? null : row;
            }
            try (CsvParser parser = sharedCsvMapper.getFactory().createParser(record)) {
                parser.setSchema(csvSchema);
                List<String> row = new ArrayList<>();
                JsonToken token;
                while ((token = parser.nextToken()) != null) {
                    if (token == JsonToken.VALUE_NULL) {
                        row.add(null);
                    } else if (token.isScalarValue()) {
                        row.add(decodeFieldValue(parser.getValueAsString()));
                    } else if (token != JsonToken.START_ARRAY && token != JsonToken.END_ARRAY) {
                        throw new IOException("Unexpected CSV token [" + token + "] while parsing record");
                    }
                }
                return row.isEmpty() ? null : row;
            }
        }
    }

    /**
     * Value decode for the {@code escaped} mode (C-style backslash text semantics): a whole-field {@code \N} is null
     * and {@code \}-sequences un-escape C-style — the named cases {@code \t \n \r \0 \b \f} are
     * the standard C-style output escape set, and any other {@code \c} is {@code c} (its parse
     * rule). Runs only when {@link CsvFormatOptions#decodesEscapes()} (escaping on, quoting off);
     * identity otherwise, and lazy — a field without the escape character (the overwhelmingly common
     * case) is returned as-is, so the decode stays off the hot path. Boundary scanning is untouched by
     * design: an in-field tab/newline is the two bytes {@code \}+{@code t}/{@code n} on disk, so raw
     * terminators remain unambiguous.
     */
    private String decodeFieldValue(String value) {
        if (options.decodesEscapes() == false || value == null) {
            return value;
        }
        char esc = options.escapeChar();
        int firstEscape = value.indexOf(esc);
        if (firstEscape < 0) {
            return value;
        }
        if (value.length() == 2 && firstEscape == 0 && value.charAt(1) == 'N') {
            return null; // \N — the DB-export null representation
        }
        StringBuilder sb = new StringBuilder(value.length());
        sb.append(value, 0, firstEscape);
        for (int i = firstEscape; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != esc || i + 1 == value.length()) {
                sb.append(c);
                continue;
            }
            char next = value.charAt(++i);
            sb.append(decodeEscapeChar(next));
        }
        return sb.toString();
    }

    /**
     * C-style escape decode: maps {@code \t}, {@code \n}, {@code \r}, {@code \0}, {@code \b},
     * {@code \f} to their control-character equivalents; any other {@code \c} decodes to {@code c}.
     * This is the {@code mode: escaped} (quoting off) semantics, used by {@link #decodeFieldValue}.
     * The QUOTED + escaping dialect uses {@link #decodeQuotedEscapeChar} instead — that dialect follows
     * Jackson's CSV escape, which is a strictly smaller control set.
     */
    static char decodeEscapeChar(char next) {
        return switch (next) {
            case 't' -> '\t';
            case 'n' -> '\n';
            case 'r' -> '\r';
            case '0' -> '\0';
            case 'b' -> '\b';
            case 'f' -> '\f';
            // Any other \c is c — including \\ and \' — matching the C-style parse rule.
            default -> next;
        };
    }

    /**
     * Escape decode for the QUOTED dialect with escaping on ({@code quote}/{@code escape} both live),
     * matching Jackson's {@code CsvDecoder}: only {@code \t}, {@code \n}, {@code \r}, {@code \0} are
     * control escapes; every other {@code \c} — including {@code \b} and {@code \f} — is the literal
     * {@code c} (the escape merely protects the next character). This is a strict subset of the C-style
     * {@link #decodeEscapeChar} set, which additionally maps {@code \b}/{@code \f} to control chars; that
     * fuller set stays confined to {@code mode: escaped}, whose fallback is Jackson-with-C-style anyway.
     */
    static char decodeQuotedEscapeChar(char next) {
        return switch (next) {
            case 't' -> '\t';
            case 'n' -> '\n';
            case 'r' -> '\r';
            case '0' -> '\0';
            // Everything else — including \b, \f, \\ and \' — is the literal next character (Jackson's CSV rule).
            default -> next;
        };
    }

    private class CsvBatchIterator extends BufferingPageIterator {
        private final BufferedReader reader;
        private final CsvLogicalRecordReader recordReader;
        private final InputStream stream;
        private final List<String> projectedColumns;
        private final int batchSize;
        private final List<Attribute> preResolvedSchema;
        private final ErrorPolicy errorPolicy;
        private final int modeOrdinal;
        private final boolean logErrors;
        private final boolean hasCommentFilter;
        private final boolean hasCustomNullValue;
        private final String nullValueStr;
        private final DateFormatter datetimeFormatter;
        private final boolean bracketMultiValues;
        private final String sourceLocation;
        private final SkipWarnings skipWarnings;
        private List<Attribute> schema;
        /**
         * Raw field index per pinned-schema position, or {@code null} when the schema binds positionally (no
         * {@code path} declared). Lets a declared {@code path} read the column it names rather than the column
         * that happens to sit at its declaration position.
         */
        @Nullable
        private final int[] schemaFieldIndex;
        private int[] projectedIdx;
        /**
         * Widest row this schema accepts before it reads as drift. A positional declaration binds the file's leading
         * columns 1:1, so a wider row means the file does not match the declaration — fail loudly. A declared
         * {@code path} binds BY NAME, so a wider file is the intended case (declare 5 columns of a 105-column file)
         * and only rows too narrow to hold a bound index matter — those the short-row handling already covers.
         */
        private int rowWidthLimit;
        /**
         * One past the widest raw field index any projected column binds — the addressable length of
         * {@link #sourceToBufferIndex}. Equals the schema size under positional binding; a declared {@code path} can
         * push it beyond that (bind {@code col100} of a 105-column file) or leave it short of the file's width.
         */
        private int sourceIndexBound;
        private DataType[] projectedTypes;
        private Attribute[] projectedAttrs;
        /**
         * Per-projected-column declared date formatter, or {@code null} for a column with no declared {@code format}.
         * {@code null} array when no column declares one. Resolved ONCE in {@link #initProjection} (never per row) from
         * the reader's physical-keyed {@code declaredDateFormats}, aligned to {@link #projectedAttrs}.
         */
        private DateFormatter[] declaredFormatters;
        /**
         * Projection slots backed by a {@code KEYWORD}/{@code TEXT} source column: the only columns
         * that receive a byte-size hint when building blocks (see {@link #convertRowsToPage}).
         * Precomputed once here so the per-batch hint pass iterates only string columns rather than
         * scanning every projected column for every row (which would add an O(rows x columns) walk
         * even for purely numeric projections).
         */
        private int[] byteHintColumns;
        /**
         * Inverse of {@link #byteHintColumns}: maps a projected column index to its byte-hint slot, or
         * {@code -1} for columns that are not byte-hinted. Lets the direct-block batch loop decide, per
         * column, whether to append straight to the builder or retain the value so the builder can be
         * sized up-front (see {@link #convertDirectBatchToPage}).
         */
        private int[] columnToByteHintSlot;
        // Per-batch retain buffers for the direct-block byte-hint pass, one entry per byte-hint column.
        // The direct path streams records, so a keyword column's total byte size is unknown until the
        // whole batch is parsed. Rather than let the BytesRefArray regrow (and to match the byte-hinted
        // Jackson path), keyword/text values are retained here while the batch is parsed, then replayed
        // into a builder sized with the exact total. The arrays are reused across batches (grown, never
        // shrunk) so the steady state allocates nothing.
        private byte[][] kwRetainData;
        private int[] kwRetainDataLen;
        private int[][] kwRetainValueLen;
        private int kwRetainRows;
        private int columnCount;
        /** Total number of columns in the file schema (not just projected). */
        private int schemaColumnCount;
        /** Which source-column indices are projected (for skipping non-projected fields during splitting). */
        private BitSet projectedFieldSet;
        /** Maps a source-column index to its slot in {@link #rowBuffer} / {@link #projectedTypes}; -1 if not projected. */
        private int[] sourceToBufferIndex;
        private Object[] rowBuffer;
        /**
         * Projection slot of the synthetic {@code _rowPosition} column, or {@code -1} when not
         * requested. Not a CSV source column — filled from {@link #rowStartBytes}, the per-record
         * file-global byte offset (split start byte plus bytes consumed up to the record's first
         * character), so the value is identical for the same physical record across any split
         * layout. Matches the NDJSON {@code recordFileOffset} shape.
         */
        private int rowPositionSlot = -1;
        /**
         * Parallel to {@link #projectedIdx}: the {@link SyntheticColumns.Kind} at slots whose
         * {@code projectedIdx[i] < 0} (i.e. slots with no backing CSV source column), {@code null}
         * elsewhere.
         */
        private SyntheticColumns.Kind[] syntheticKinds;
        /**
         * Parallel to the currently-batched rows: byte offset of each row's first character within
         * the file, computed as {@code splitStartByte + (bytesRead - lastRecordBytes)} at the moment
         * the row was emitted by {@link CsvLogicalRecordReader#readRecord}. Emitted into the
         * {@code _rowPosition} slot during conversion. Sized to the current batch and re-allocated
         * per batch; entries align positionally with {@code rows} / {@code lines}.
         */
        private long[] rowStartBytes;
        /**
         * Parallel to {@link #prefetchedRows}: byte offsets captured at sampling time so that when
         * the prefetched rows are replayed into the data path their {@code _rowPosition} values
         * stay anchored to where they sat in the file.
         */
        private long[] prefetchedRowStartBytes;
        private Iterator<List<?>> csvIterator;
        /**
         * Which value contract the live {@link #csvIterator} carries — the two record sources disagree, and the
         * batch loop must decode exactly once. {@link CsvRecordIterator} (via {@code parseRecord}) already applied
         * {@link #decodeFieldValue}; the Jackson bulk iterators deliver raw values because {@link #newCsvSchema}
         * withholds the escape char in the no-quote modes. Always assigned together with {@link #csvIterator}
         * through {@link #routeCsvIterator}.
         */
        private boolean csvIteratorDeliversDecoded;
        private List<String[]> prefetchedRows;
        private long prefetchedRowsBytes;
        // Inner close flag: gates hasNext() short-circuit after close and the one-shot teardown below. The base
        // BufferingPageIterator separately gates close()/closeInternal() re-entry; this one also stops iteration.
        private boolean closed = false;
        private long errorCount = 0;
        private long totalRowCount = 0;
        private String lastFieldError;
        /** Non-null iff the iterator is eligible to populate {@link ExternalStats} on close (whole-file read). */
        private final StorageObject cacheableObject;
        /** Non-null iff stats capture is enabled. Wraps the underlying stream so bytesRead is available at close. */
        private final CountingInputStream byteCounter;
        private long rowsEmittedForCache = 0;
        /** True only when {@link #hasNext()} returned false from natural exhaustion (not from close or an exception). */
        private boolean naturallyExhausted = false;
        /** Lazily built once the schema and projection are known. {@code null} until the first batch resolves them. */
        private ColumnStatsAccumulator columnStats;
        /**
         * ALL-scope, non-stripe (whole-file) accumulator over the FULL file schema, fed from the raw parsed
         * record (every file column, including unprojected). {@code null} unless scope is ALL on the
         * whole-file path. Snapshotted at close alongside {@link #columnStats} — its superset wins per column.
         */
        private ColumnStatsAccumulator allFileColumnStats;
        /** True when the direct path feeds {@link #columnStats} inline (cacheable read with tracked columns). */
        private boolean accumulateDirectStats;
        /** True when the most recent batch already accumulated stats inline, so {@link #captureBlockStats} must skip it. */
        private boolean lastBatchAccumulatedStats;

        /** Pinned at iterator open; closes the open-vs-close mtime-race window for the cache key. */
        private final long pinnedMtimeMillis;
        /** True for parallel-parsing chunks — close-time publish carries the partial-chunk marker. */
        private final boolean chunkMode;

        // Reader-level counters shared across this iterator and any sibling segments.
        private final CsvReaderCounters counters;

        /** True when the direct-to-block plain (unquoted) path is eligible (decided once in {@link #read}). */
        private final boolean directBlockPlain;
        /** True when the direct-to-block quoted (RFC 4180) path is eligible (decided once in {@link #read}). */
        private final boolean directBlockQuoted;
        /** Per-projected-column reusable UTF-8 scratch for keyword/text fields; null until first use. */
        private BytesRef[] keywordScratch;
        /** Reusable accumulator for quoted/escaped field content on the direct quoted path. */
        private StringBuilder quotedBuf;
        /**
         * Effective per-field value-length cap for the direct-to-block path, in characters. Mirrors
         * Jackson's {@code StreamReadConstraints.maxStringLength} (which {@link #read} wires from
         * {@code maxFieldSize}). {@code maxFieldSize == 0} means "no limit", normalised here to
         * {@link Integer#MAX_VALUE} so the hot-path comparison never fires.
         */
        private final int maxFieldChars;

        // Typed per-row staging for the direct-to-block path. A row is parsed into these typed slots
        // and only flushed to the block builders once it is accepted in full (all-or-nothing), so a
        // mid-row rejection never leaves a partial row in the builders. Using primitive arrays instead
        // of an Object[] keeps longs/ints/doubles/booleans off the heap: no per-cell boxing. Each
        // column writes only the slot matching its element type ({@link #directElements}).
        private ElementType[] directElements;
        private long[] stageLong;
        private int[] stageInt;
        private double[] stageDouble;
        private boolean[] stageBool;
        private BytesRef[] stageRef;
        private boolean[] stageNull;

        // Current direct-path record view (a range into the record reader's reusable char[]), captured so
        // the cold error/warning paths can lazily materialize the raw row String without allocating one
        // on the accepted hot path. Set at the top of splitAndConvertDirect for each row.
        private char[] directRecBuf;
        private int directRecFrom;
        private int directRecTo;

        // Number of data records (blank/comment/cap-skipped records excluded) consumed by the most recent
        // convertDirectBatchToPage call. Zero distinguishes end of stream from an all-skipped lenient batch.
        private int directBatchRecordsRead;

        // advanceDirectRecord outcomes: a usable data record, a record to skip (blank/comment/over-cap), end of stream.
        private static final int DIRECT_DATA = 0;
        private static final int DIRECT_SKIP = 1;
        private static final int DIRECT_EOF = 2;

        /**
         * File-global byte offset of this split's first byte. Combined with the recordReader's
         * cumulative byte count to produce a split-invariant {@code _rowPosition} for every
         * emitted record. Same role as NDJSON's {@code recordOffsetBase}.
         */
        private final long splitStartByte;

        /** Canonical-stripe grid in bytes for per-stripe stats capture; {@code <= 0} disables it. */
        private final long statsStripeSize;
        /**
         * How much per-stripe statistics this read harvests. {@link StripeColumnScope#NONE} harvests nothing;
         * {@link StripeColumnScope#COUNT} the per-stripe row count only; {@link StripeColumnScope#PROJECTED}
         * row count plus min/max/null for the projected columns; {@link StripeColumnScope#ALL} adds min/max/null
         * for EVERY file column — harvested by {@link #harvestAllColumns} straight from the raw parsed record
         * (the output page only carries the projected columns), so ALL's committed set is a strict superset of
         * PROJECTED's. Row count is harvested in every mode except {@code NONE} — including a zero-projection
         * {@code COUNT(*)} read.
         */
        private final StripeColumnScope statsColumnScope;
        /** Whether this read reaches the file's true end — only then may its last stripe be terminal. */
        private final boolean statsFileFinal;
        /**
         * Shared canonical-stripe harvester (byte-range cover model). Non-null only when stripe capture is
         * active ({@code statsStripeSize > 0}); owns the per-stripe accumulators and the close-time emit loop.
         */
        private final StripeStatsHarvester stripeHarvester;
        /**
         * Set when per-stripe capture must safe-miss — row/page misalignment during accumulation, or a
         * setup-time offset gap. Only meaningful in chunk mode ({@code statsStripeSize > 0}): once set,
         * accumulation stops and the close hook emits no stripe fragments (the file re-scans warm). The
         * non-chunk whole-file publish does not consult it — stripe attribution is irrelevant there.
         */
        private boolean stripeCaptureDisabled = false;
        /**
         * Set when the error policy RECOVERED an over-{@code max_record_size} record by dropping it (the
         * bracket/record-reader path). Unlike a normal SKIP_ROW drop, that survivor loss is determined by the
         * {@code max_record_size} query PRAGMA, which is NOT in the cache fingerprint -- so a warm query under a
         * larger cap would serve this scan's under-count instead of its own N. Suppress the whole publish
         * (safe-miss, re-scan) rather than cache a pragma-dependent count. Rare (pathological oversized records).
         */
        private boolean recordCapDropped = false;
        /**
         * The byte offsets of the rows that SURVIVED into the current page, in page order. {@link #rowStartBytes}
         * holds an offset for every PARSED row (including ones later dropped by a structural/field error during
         * {@link #convertRowsToPage}); this array holds only the accepted rows, so it stays length-aligned with
         * the emitted page and per-stripe capture keeps working across a row drop. {@code null} when offsets are
         * not tracked.
         */
        private long[] acceptedRowStartBytes;
        /**
         * Maps the Jackson bulk path's per-row char offset to a file-global byte offset without
         * re-decoding. {@code null} on the per-record reader path (already byte-exact) and when stripe
         * capture is off or the encoding is not UTF-8 (then stripe capture safe-misses).
         */
        private ByteOffsetTrackingReader bulkByteTracker;
        /** The Jackson bulk mapping iterator, retained to read each row's start char offset. */
        private MappingIterator<List<?>> bulkRows;
        /** Last bulk-path char offset queried, to enforce the tracker's non-decreasing contract / detect anomalies. */
        private long bulkLastCharOffset = 0L;

        CsvBatchIterator(
            BufferedReader reader,
            CsvLogicalRecordReader recordReader,
            InputStream stream,
            List<String> projectedColumns,
            int batchSize,
            List<Attribute> preResolvedSchema,
            @Nullable int[] schemaFieldIndex,
            ErrorPolicy errorPolicy,
            String sourceLocation,
            StorageObject cacheableObject,
            CountingInputStream byteCounter,
            long pinnedMtimeMillis,
            boolean chunkMode,
            CsvReaderCounters counters,
            boolean directBlockPlain,
            boolean directBlockQuoted,
            long splitStartByte,
            long statsStripeSize,
            boolean statsFileFinal,
            StripeColumnScope statsColumnScope,
            @Nullable Consumer<String> warningSink
        ) {
            this.reader = reader;
            this.recordReader = recordReader;
            this.stream = stream;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.preResolvedSchema = preResolvedSchema;
            this.schemaFieldIndex = schemaFieldIndex;
            this.errorPolicy = errorPolicy;
            this.modeOrdinal = errorPolicy.mode().ordinal();
            this.logErrors = errorPolicy.logErrors();
            this.hasCommentFilter = options.commentPrefix().isEmpty() == false;
            this.hasCustomNullValue = options.nullValue().isEmpty() == false;
            this.nullValueStr = options.nullValue();
            this.datetimeFormatter = options.datetimeFormatter();
            this.bracketMultiValues = options.multiValueSyntax() == CsvFormatOptions.MultiValueSyntax.BRACKETS;
            this.sourceLocation = sourceLocation;
            this.cacheableObject = cacheableObject;
            this.byteCounter = byteCounter;
            this.pinnedMtimeMillis = pinnedMtimeMillis;
            this.chunkMode = chunkMode;
            this.counters = counters;
            this.directBlockPlain = directBlockPlain;
            this.directBlockQuoted = directBlockQuoted;
            this.maxFieldChars = options.maxFieldSize() > 0 ? options.maxFieldSize() : Integer.MAX_VALUE;
            this.splitStartByte = splitStartByte;
            this.statsStripeSize = statsStripeSize;
            this.statsColumnScope = statsColumnScope != null ? statsColumnScope : StripeColumnScope.PROJECTED;
            this.statsFileFinal = statsFileFinal;
            this.stripeHarvester = statsStripeSize > 0 ? new StripeStatsHarvester(statsStripeSize, statsFileFinal) : null;
            this.skipWarnings = SkipWarnings.of(
                errorPolicy,
                "CSV read from ["
                    + sourceLocation
                    + "] encountered parse errors handled per policy (policy: "
                    + errorPolicy.modeName()
                    + "); affected rows/fields are listed below",
                warningSink
            );
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (nextPage != null) {
                return true;
            }
            long startNanos = System.nanoTime();
            long startTotal = totalRowCount;
            long startError = errorCount;
            try {
                nextPage = readNextBatch();
                if (nextPage == null) {
                    naturallyExhausted = true;
                    return false;
                }
                return true;
            } catch (IOException e) {
                throw ExternalFailures.surface(e, "Failed to read CSV batch");
            } finally {
                long deltaTotal = totalRowCount - startTotal;
                long deltaErrors = errorCount - startError;
                counters.addRowsEmitted(deltaTotal - deltaErrors);
                counters.addParseErrors(deltaErrors);
                counters.addReadNanos(System.nanoTime() - startNanos);
            }
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            Page result = nextPage;
            nextPage = null;
            rowsEmittedForCache += result.getPositionCount();
            captureBlockStats(result);
            return result;
        }

        private void captureBlockStats(Page page) {
            // NONE: harvest nothing. Otherwise we harvest at minimum the per-stripe row count — including for a
            // zero-projection COUNT(*) read (columnCount == 0), the regression this scope gate fixes: the old
            // `columnCount == 0` early-return killed all harvest, so a warm COUNT(*) re-scanned the whole file.
            if (cacheableObject == null || projectedAttrs == null || statsColumnScope == StripeColumnScope.NONE) {
                return;
            }
            // The projected-column harvest below feeds from the output page (the query's projected columns;
            // zero blocks for COUNT(*)). ALL's UNPROJECTED file columns are harvested separately, straight
            // from the raw parsed record, in {@link #harvestAllColumns} during conversion — there is no block
            // here for a column the query never projected.
            if (statsStripeSize > 0) {
                // Chunk-parallel read: per-stripe fragments are the only cacheable contribution. Accumulate
                // while capture stays aligned; once disabled it is a permanent safe-miss, so stop — the
                // whole-chunk harvest below would feed a doomed PARTIAL the coordinator just discards.
                if (rowStartBytes != null && stripeCaptureDisabled == false) {
                    accumulateStripes(page);
                }
                return;
            }
            // Non-stripe whole-file path. Only PROJECTED harvests the projected columns here; COUNT harvests
            // none (the close hook still publishes rowsEmittedForCache) and ALL harvests EVERY file column
            // straight from the raw record in harvestAllColumns (a strict superset), so the projected
            // re-harvest is skipped — mirrors NDJSON.
            if (statsColumnScope != StripeColumnScope.PROJECTED || columnCount == 0) {
                return;
            }
            // The direct path already accumulated this page's stats inline during parsing (see
            // appendStagedRow); re-walking the blocks here would double count.
            if (lastBatchAccumulatedStats) {
                return;
            }
            if (columnStats == null) {
                columnStats = ColumnStatsAccumulator.forProjectedAttributes(projectedAttrs);
            }
            for (int i = 0; i < columnCount; i++) {
                columnStats.acceptBlockAt(i, page.getBlock(i));
            }
        }

        /**
         * Per-stripe accumulation: attribute each row in {@code page} to its canonical stripe by its own
         * file-global byte start ({@link #rowStartBytes}), summing rows + per-column min/max/null per stripe.
         * A page that stays within one stripe (the common case) is folded whole with no per-position
         * filtering. If the page's positions don't line up with the captured offsets, capture safe-misses
         * for the whole file rather than risk misattribution. Byte ranges / cover anchors are NOT tracked
         * here — they are derived from the chunk's byte geometry in {@link #emitPerStripe}.
         */
        private void accumulateStripes(Page page) {
            int n = page.getPositionCount();
            // Attribute by the SURVIVING rows' offsets (page-aligned): a row dropped during convertRowsToPage is
            // absent from both the page and this array. The shared run-walk owns the offset/page alignment invariant
            // (fail-loud assert + safe-miss); this consumer only folds the projected columns + row count per run.
            // COUNT scope harvests rows only (no column accumulator). Only PROJECTED builds a per-stripe
            // projected-column accumulator (when there are any — a zero-projection COUNT(*) read has none).
            // Under ALL, every file column (incl. the projected ones) is accumulated in harvestAllColumns
            // (acc.allCols) from the raw record — a strict superset that shadows acc.cols at emit — so the
            // projected accumulator is skipped, mirroring NDJSON.
            long[] offsets = acceptedRowStartBytes;
            boolean aligned = stripeHarvester.forEachRun(offsets, offsets == null ? -1 : offsets.length, n, (ordinal, acc, from, to) -> {
                if (acc.cols == null && statsColumnScope == StripeColumnScope.PROJECTED && projectedAttrs.length > 0) {
                    acc.cols = ColumnStatsAccumulator.forProjectedAttributes(projectedAttrs);
                }
                if (acc.cols != null) {
                    if (from == 0 && to == n) {
                        for (int b = 0; b < columnCount; b++) {
                            acc.cols.acceptBlockAt(b, page.getBlock(b));
                        }
                    } else {
                        int[] positions = new int[to - from];
                        for (int p = 0; p < positions.length; p++) {
                            positions[p] = from + p;
                        }
                        for (int b = 0; b < columnCount; b++) {
                            var filtered = page.getBlock(b).filter(false, positions);
                            try {
                                acc.cols.acceptBlockAt(b, filtered);
                            } finally {
                                filtered.close();
                            }
                        }
                    }
                }
                acc.rows += (to - from);
            });
            if (aligned == false) {
                stripeCaptureDisabled = true; // alignment lost — safe miss
            }
        }

        /**
         * Emits one stripe-addressed fragment for every stripe this chunk's byte range
         * {@code [splitStartByte, chunkAbsEnd)} overlaps — including stripes with no records (a stripe whose
         * grid line falls inside a record of the lower stripe, so its first record lands in the next chunk;
         * this chunk still owns that stripe's left edge and must anchor it). Cover anchors are pure
         * byte-range-overlap predicates: anchored-at-start iff this chunk covers the stripe's left grid line
         * ({@code splitStartByte <= ordinal*B}), complete-on-the-right iff it covers the right grid line
         * ({@code chunkAbsEnd >= (ordinal+1)*B}) or this is the file-final chunk. Record attribution is by
         * each record's own start, so per-stripe row counts are scan-invariant; the coordinator folds
         * fragments per ordinal by interval-cover, and misaligned sibling tilings collapse to one answer.
         */
        private void emitPerStripe() {
            // chunkAbsEnd is in the same (decompressed) coordinate as the per-record offsets: compression is
            // a delegating layer (DecompressingStorageObject), so byteCounter counts decompressed bytes. The
            // byte-range cover loop itself lives in the shared StripeStatsHarvester.
            long chunkBytes = byteCounter != null ? byteCounter.getBytesRead() : -1L;
            // Byte-exactness tripwire for the tracked-Jackson path. The tracker INFERS byte widths from
            // decoded chars, which assumes well-formed UTF-8: a malformed sequence the decoder replaced with
            // U+FFFD is counted at the replacement's width, skewing every subsequent record offset -- and
            // differently-chunked scans of the same file would then attribute boundary records to DIFFERENT
            // stripes (a chunk starting after the bad bytes restarts byte-exact), interleaving into a wrong
            // warm count under a "complete" cover. This emit only runs after a clean full drain, where the
            // inferred end must equal the actual bytes consumed; a mismatch means the offsets are not
            // byte-exact -- safe-miss the whole chunk rather than commit mis-attributed stripes. Data
            // condition, not a bug: no assert.
            // Both offset sources INFER byte widths from decoded chars: the bulk Jackson path via
            // bulkByteTracker, the record-reader path (bracket / _rowPosition projected) via
            // CsvLogicalRecordReader's own per-char accounting (recordReader.bytesRead()). On the bulk path
            // the tracker read the body (recordReader only consumed the header); on the record-reader path the
            // recordReader read everything, so its bytesRead() is the whole chunk's inferred end. Either way,
            // after this clean full drain the inferred end must equal the actual bytes the CountingInputStream
            // saw; a mismatch means malformed UTF-8 skewed the offsets -- safe-miss.
            long inferredEndOffset = bulkByteTracker != null
                ? bulkByteTracker.inferredEndOffset()
                : splitStartByte + recordReader.bytesRead();
            if (chunkBytes >= 0 && inferredEndOffset != splitStartByte + chunkBytes) {
                stripeCaptureDisabled = true;
                return;
            }
            stripeHarvester.emit(sourceLocation, splitStartByte, chunkBytes, pinnedMtimeMillis, computeConfigFingerprint(), schema);
        }

        /**
         * Bulk Jackson iterator that also tracks per-record byte offsets via {@link ByteOffsetTrackingReader},
         * so canonical-stripe attribution works on the fast path without dropping onto the per-record reader.
         * Live only when {@link #jacksonGrammarApplies()} (trim on, or escaped mode) and the encoding is
         * UTF-8; under no-trim the read routes through the per-record recordReader path instead (which
         * supplies byte-exact offsets for any encoding), so this tracked bulk path stays idle there.
         */
        @SuppressWarnings({ "rawtypes", "unchecked" })
        private Iterator<List<?>> newTrackedJacksonBulkIterator() throws IOException {
            // The header row and any schema-inference sample were already consumed from `reader` through
            // recordReader, so the tracker's first character sits at splitStartByte + recordReader.bytesRead(),
            // not at splitStartByte. Basing it at splitStartByte would skew every first-split row offset by the
            // header's byte length — and asymmetrically, since non-first splits skip no header — so the same
            // file read with different chunkings would attribute boundary records to different stripes and the
            // sibling tilings would fold to disagreeing per-stripe counts (a silently wrong cached aggregate).
            bulkByteTracker = new ByteOffsetTrackingReader(reader, splitStartByte + recordReader.bytesRead());
            bulkRows = sharedCsvMapper.readerFor(List.class).with(newCsvSchema()).readValues(bulkByteTracker);
            return (Iterator) bulkRows;
        }

        @Override
        protected void closeInternal() throws IOException {
            if (closed == false) {
                closed = true;
                if (prefetchedRowsBytes > 0) {
                    blockFactory.breaker().addWithoutBreaking(-prefetchedRowsBytes);
                    prefetchedRowsBytes = 0;
                    prefetchedRows = null;
                }
                // Close the reader/stream even if a stats publish throws — the publish is best-effort
                // caching, the close is not.
                try {
                    if (modeOrdinal != ErrorPolicy.Mode.FAIL_FAST.ordinal() && errorCount > 0) {
                        logger.info(
                            "CSV parsing completed with [{}] errors out of [{}] rows (policy: {})",
                            errorCount,
                            totalRowCount,
                            errorPolicy.mode()
                        );
                    }
                    // A DROPPED row (SKIP_ROW, or a structural malformed row -- e.g. an unescaped-delimiter
                    // extra column -- even under NULL_FIELD) does NOT make the cached stats wrong: which rows
                    // survive is a deterministic function of the file bytes and the error policy (pinned by the
                    // cache fingerprint -- error_mode/max_errors/max_error_ratio are all format-affecting; chunk
                    // boundaries are quote-aware, so re-execution drops the identical rows), so every statistic
                    // (row count AND extrema) over the survivors equals what re-running this query computes. So
                    // commit normally. NULL_FIELD field null-fill likewise preserves the row and caches fully.
                    // FAIL_FAST aborts before EOF, so naturallyExhausted gates it out. NONE scope suppresses all
                    // publishing. (A scan cut short mid-way -- LIMIT, cancellation, a chunk exceeding its error
                    // budget -- leaves naturallyExhausted false or an uncovered stripe, so it safe-misses rather
                    // than serving; the coordinator's whole-file poison covers the non-clean-close case.)
                    if (cacheableObject != null
                        && naturallyExhausted
                        && pinnedMtimeMillis >= 0
                        && schema != null
                        && recordCapDropped == false
                        && statsColumnScope != StripeColumnScope.NONE) {
                        if (statsStripeSize > 0) {
                            // Chunk-parallel read: only per-stripe fragments are cacheable. Emit one
                            // stripe-addressed fragment per stripe this chunk touched (the coordinator
                            // interval-covers and folds them) if capture stayed aligned and non-empty;
                            // otherwise safe-miss. A whole-chunk publish here would be an un-addressable
                            // PARTIAL the coordinator just discards — wasted work. Mirrors NDJSON.
                            if (stripeCaptureDisabled == false && stripeHarvester.isEmpty() == false) {
                                emitPerStripe();
                            }
                        } else {
                            // Non-chunk whole-file read. PROJECTED/COUNT commit columnStats (projected / none).
                            // ALL additionally commits allFileColumnStats (every file column) → strict superset
                            // of PROJECTED's set.
                            Map<String, ExternalStats.ColumnStats> cols = StripeStatsHarvester.mergeColumnStats(
                                columnStats,
                                allFileColumnStats
                            );
                            OptionalLong bytesRead = byteCounter == null
                                ? OptionalLong.empty()
                                : OptionalLong.of(byteCounter.getBytesRead());
                            String fingerprint = computeConfigFingerprint();
                            ExternalStats.Stats statsRecord = new ExternalStats.Stats(rowsEmittedForCache, bytesRead, cols);
                            // Whole-file publishes carry no partial marker; per-chunk publishes carry one, and
                            // the ParallelParsingCoordinator publishes a finalize marker at clean whole-file
                            // completion so the coordinator's reconciler only commits the merge then.
                            publishToCaptureSink(
                                sourceLocation,
                                pinnedMtimeMillis,
                                fingerprint,
                                statsRecord,
                                schema,
                                sizeInBytesFromLength()
                            );
                        }
                    }
                } finally {
                    IOUtils.close(reader, stream);
                }
            }
        }

        /** Returns the file's length-derived size when known by the cacheable storage object, else empty. */
        private OptionalLong sizeInBytesFromLength() {
            if (cacheableObject == null) {
                return OptionalLong.empty();
            }
            try {
                return OptionalLong.of(cacheableObject.length());
            } catch (IOException | UnsupportedOperationException e) {
                return OptionalLong.empty();
            }
        }

        private void publishToCaptureSink(
            String filePath,
            long mtimeMillis,
            String fingerprint,
            ExternalStats.Stats stats,
            List<Attribute> resolvedSchema,
            OptionalLong lengthSize
        ) {
            SourceStatistics sourceStats = TextFormatStats.build(Optional.of(stats), lengthSize, resolvedSchema);
            Map<String, Object> base = new HashMap<>();
            base.put(ExternalStats.MTIME_MILLIS_KEY, mtimeMillis);
            base.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
            if (chunkMode) {
                base.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
            }
            Map<String, Object> flat = SourceStatisticsSerializer.embedStatistics(base, sourceStats);
            ExternalStatsCapture.record(filePath, flat);
        }

        private Page readNextBatch() throws IOException {
            // Reset per batch; only the direct path sets it true (in convertDirectBatchToPage) so that
            // the sample/boxed page paths still capture stats via the block walk in captureBlockStats.
            lastBatchAccumulatedStats = false;
            boolean useDirectBlock = directBlockPlain || directBlockQuoted;
            if (schema == null) {
                if (preResolvedSchema != null) {
                    schema = preResolvedSchema;
                } else if (options.headerRow() == false) {
                    schema = inferSchemaHeaderlessFromBatchReader();
                    if (schema == null) {
                        return null;
                    }
                } else {
                    String headerLine = null;
                    String record;
                    while ((record = recordReader.readRecord(false)) != null) {
                        String trimmed = record.trim();
                        if (trimmed.isEmpty() || (hasCommentFilter && trimmed.startsWith(options.commentPrefix()))) {
                            continue;
                        }
                        headerLine = trimmed;
                        break;
                    }
                    if (headerLine == null) {
                        return null;
                    }
                    counters.markHeaderDetected();
                    schema = parseSchema(headerLine);
                    if (schema == null) {
                        schema = inferSchemaFromBatchReader(headerLine);
                    }
                    if (schema == null) {
                        return null;
                    }
                }
                initProjection();

                boolean useBracketAwareParsing = bracketMultiValues && options.delimiter() == ',';
                if (useBracketAwareParsing == false && useDirectBlock == false && csvIterator == null) {
                    // Hot data path: Jackson reads directly from the BufferedReader and tokenizes records in its
                    // own bulk char buffer. The per-record byte cap is enforced upstream by the wrapping
                    // CsvRecordCappingInputStream, so we don't need CsvLogicalRecordReader's per-char loop here.
                    //
                    // Two exceptions route through the recordReader-backed per-record iterator instead of the
                    // Jackson bulk path. read() suppresses the byte-level cap wrap on both to match (see
                    // useRecordReaderPath):
                    // 1. _rowPosition projected (rowPositionSlot >= 0, i.e. _id / _file.record_ref): each
                    // record must advance CsvLogicalRecordReader's byte accounting so the offset
                    // (splitStartByte + bytesRead - lastRecordBytes) stays exact; the Jackson bulk path
                    // bypasses recordReader and would pin every data row at the header boundary.
                    // 2. Jackson's grammar does not apply (no-trim, non-escaped-mode — see
                    // jacksonGrammarApplies): under no-trim Jackson mis-splits padded-quoted fields and
                    // eats first-column leading whitespace, so the record path tokenizes with the house
                    // splitRecordFields (parseRecord) to agree with the direct walkers. Stripe capture
                    // still composes: recordReader supplies byte-exact per-record offsets (the
                    // bulkByteTracker == null branch below), validated by the emit-time tripwire, so
                    // capture is NOT disabled here even for non-UTF-8 — recordReader counts bytes per
                    // options.encoding().
                    if (rowPositionSlot >= 0 || jacksonGrammarApplies() == false) {
                        // parseRecord already applied decodeFieldValue on both of its branches. That holds for the
                        // no-trim reroute arm too, where the decode is the identity (QUOTED / PLAIN only), so the
                        // contract is DECODED here even though only escaped mode can observe the difference.
                        routeCsvIterator(newCsvIterator(recordReader), true);
                    } else if (statsStripeSize > 0 && StandardCharsets.UTF_8.equals(options.encoding())) {
                        // Stripe capture on the bulk path: wrap the reader so each row's char offset maps to a
                        // file-global byte offset (record-canonical stripe attribution) without leaving the fast
                        // Jackson path or re-decoding. Non-UTF-8 falls through and stripe capture safe-misses.
                        routeCsvIterator(newTrackedJacksonBulkIterator(), false);
                    } else {
                        // Plain Jackson bulk path: neither a byte tracker (set only on the UTF-8 stripe path
                        // above) nor the record-reader path (rowPositionSlot >= 0) is active, so recordReader is
                        // never advanced. Any per-row offset would be derived from its frozen byte accounting and
                        // collapse every row onto one stripe — a wrong warm count/min/max. If stripe capture would
                        // otherwise be attempted here (e.g. ALL scope on a non-UTF-8 input), disable it so the read
                        // safe-misses and a warm aggregate re-scans rather than serving fabricated per-stripe stats.
                        if (statsStripeSize > 0) {
                            stripeCaptureDisabled = true;
                        }
                        routeCsvIterator(newJacksonBulkIterator(reader), false);
                    }
                }
            }
            // Direct-block fast path (elastic/elasticsearch#152300): drain the schema-sample's prefetched rows
            // through the shared String[] conversion so the row sequence (parity) is identical regardless of
            // path, then run the direct-block batch loop. Stripe capture composes on this path: nextRecord()
            // commits byte-exact per-record accounting, so convertDirectBatchToPage attributes each record to its
            // canonical stripe by its own file-global byte start (COUNT/PROJECTED); ALL scope, which needs the
            // raw String[] for every file column, routes to the non-direct harvest path (directEligible excludes
            // it). The emit-time byte-exactness tripwire safe-misses on any skew — never a wrong value.
            if (useDirectBlock && prefetchedRows != null) {
                List<String[]> rows = new ArrayList<>(prefetchedRows);
                // Stripe capture: the sampled rows carry their own file-global byte offsets (captured at
                // sampling time, parallel to prefetchedRows). Hand them to convertRowsToPage via rowStartBytes so
                // the prefetched rows are stripe-attributed exactly like the direct loop's records, then null the
                // parallel arrays so the next batch (the direct loop) re-derives offsets from the record reader.
                if (statsStripeSize > 0 && cacheableObject != null && stripeCaptureDisabled == false) {
                    rowStartBytes = prefetchedRowStartBytes;
                }
                prefetchedRows = null;
                prefetchedRowStartBytes = null;
                blockFactory.breaker().addWithoutBreaking(-prefetchedRowsBytes);
                prefetchedRowsBytes = 0;
                Page page = convertRowsToPage(rows);
                if (page != null) {
                    return page;
                }
                // No accepted rows in the sample under a lenient policy: fall through to the direct loop.
            }
            if (useDirectBlock) {
                while (true) {
                    Page page = convertDirectBatchToPage(batchSize);
                    if (directBatchRecordsRead == 0) {
                        return null;
                    }
                    if (page != null || modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                        return page;
                    }
                }
            }
            boolean bracketAware = csvIterator == null && bracketMultiValues && options.delimiter() == ',';
            // ALL scope must observe every file column, including unprojected ones; the fused bracket
            // conversion ({@link #splitAndConvertProjected}) walks a line once and materialises ONLY the
            // projected fields, so it cannot feed the all-column accumulator. Route ALL through the
            // full-split {@code readRowsBracketAware} path instead (one String[] of every field per row) —
            // the opt-in cost of the broad mode. Narrower scopes keep the fused fast path.
            boolean useFusedBracketPath = bracketAware && statsColumnScope != StripeColumnScope.ALL;
            // Capture per-row offsets when _rowPosition is projected (record-reader path), when the bulk
            // path is tracking byte offsets for stripe capture, or — for ANY scope, not just ALL — whenever
            // stripe capture is active and a record-advancing path can supply real offsets; otherwise it is
            // dead work. Without this last disjunct the bracket-aware path (multi_value_syntax: brackets) with
            // scope PROJECTED/COUNT and no _rowPosition would harvest NOTHING on every chunked read — a silent
            // permanent warm miss for a whole configuration, diverging from NDJSON and CSV's own bulk path.
            // The disjunct is gated on stripeCaptureDisabled == false: on the plain Jackson bulk path (no byte
            // tracker, no record-reader) recordReader is never advanced, so the only offset we could compute
            // (splitStartByte + recordReader.bytesRead() - lastRecordBytes) is frozen and would collapse every
            // row onto one stripe. That path sets stripeCaptureDisabled = true during schema setup; mirroring
            // the flag here keeps the offset capture (and the downstream harvest) from doing fabricated work
            // once capture is off. Byte exactness on the record-reader/bracket path is still enforced at emit
            // by the inferred-vs-actual tripwire, so a skewed offset safe-misses rather than serving wrong.
            final boolean trackOffsets = rowPositionSlot >= 0
                || bulkByteTracker != null
                || (statsStripeSize > 0 && cacheableObject != null && stripeCaptureDisabled == false);
            while (true) {
                if (useFusedBracketPath && prefetchedRows == null && columnCount > 0) {
                    List<String> lines = new ArrayList<>();
                    List<Long> lineStartBytes = trackOffsets ? new ArrayList<>() : null;
                    readLogicalLinesBracketAware(lines, lineStartBytes, batchSize);
                    if (lines.isEmpty()) {
                        return null;
                    }
                    rowStartBytes = trackOffsets ? toLongArray(lineStartBytes) : null;
                    Page page = convertLinesToPage(lines);
                    if (page != null || modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                        return page;
                    }
                } else {
                    List<String[]> rows = new ArrayList<>();
                    List<Long> rowStartBytesList = trackOffsets ? new ArrayList<>() : null;
                    if (prefetchedRows != null) {
                        // Replay the schema-sample rows with the offsets captured during sampling.
                        rows.addAll(prefetchedRows);
                        if (trackOffsets && prefetchedRowStartBytes != null) {
                            for (long offset : prefetchedRowStartBytes) {
                                rowStartBytesList.add(offset);
                            }
                        }
                        prefetchedRows = null;
                        prefetchedRowStartBytes = null;
                        blockFactory.breaker().addWithoutBreaking(-prefetchedRowsBytes);
                        prefetchedRowsBytes = 0;
                    }
                    if (bracketAware) {
                        readRowsBracketAware(rows, rowStartBytesList, batchSize - rows.size());
                    } else {
                        while (rows.size() < batchSize) {
                            try {
                                if (csvIterator.hasNext() == false) {
                                    break;
                                }
                                // Bulk path: read the row's start byte from the parser's char offset BEFORE
                                // next() advances it. Record-reader path: capture AFTER next(), when bytesRead /
                                // lastRecordBytes reflect the just-returned record (cumulative across any
                                // blank/comment lines this next() consumed on the way to it).
                                long rowStartByte = 0L;
                                if (bulkByteTracker != null) {
                                    // The parser's current-token char offset is the row's first char (its
                                    // location reports char offsets for CSV; byte offsets are always -1). If it
                                    // is unavailable (-1) or non-monotonic, we cannot map this record to a byte
                                    // offset, so disable stripe capture for the file — a safe miss; the query
                                    // still runs and a warm query simply re-scans rather than serving stale stats.
                                    long charOff = bulkRows.getParser().currentTokenLocation().getCharOffset();
                                    if (charOff < 0 || charOff < bulkLastCharOffset) {
                                        stripeCaptureDisabled = true;
                                    } else {
                                        bulkLastCharOffset = charOff;
                                        rowStartByte = bulkByteTracker.byteOffsetAtChar(charOff);
                                    }
                                }
                                List<?> rowList = csvIterator.next();
                                if (bulkByteTracker == null && trackOffsets) {
                                    rowStartByte = splitStartByte + recordReader.bytesRead() - recordReader.lastRecordBytes();
                                }
                                String[] row = new String[rowList.size()];
                                for (int i = 0; i < rowList.size(); i++) {
                                    Object val = rowList.get(i);
                                    // decodeFieldValue must run exactly once per field, and the two record sources
                                    // carry opposite contracts: the Jackson bulk iterators deliver RAW values
                                    // (newCsvSchema withholds the escape char in the no-quote modes, so the
                                    // backslash reaches us untouched), while CsvRecordIterator.parseRecord already
                                    // decoded. This seam therefore decodes only the raw arm. Decoding both would
                                    // silently corrupt escaped mode — the only dialect where decodeFieldValue is
                                    // non-identity (it un-escapes \t \n \\ and maps a whole-field \N to null).
                                    String value = val != null ? val.toString() : null;
                                    row[i] = csvIteratorDeliversDecoded ? value : decodeFieldValue(value);
                                }
                                if (hasCommentFilter && row.length > 0 && row[0] != null) {
                                    String trimmedFirstCell = row[0].trim();
                                    if (trimmedFirstCell.startsWith(options.commentPrefix())) {
                                        continue;
                                    }
                                }
                                rows.add(row);
                                if (trackOffsets) {
                                    rowStartBytesList.add(rowStartByte);
                                }
                            } catch (RuntimeException e) {
                                totalRowCount++;
                                if (isRecordCapDrop(e)) {
                                    // An over-max_record_size record dropped by the record-reader path
                                    // (rowPositionSlot >= 0): CsvRecordIterator.hasNext launders the typed
                                    // CsvRecordTooLargeException through ExternalFailures.surface, so it arrives
                                    // here as the cause of an unchecked wrapper. Same survivor loss as
                                    // readBracketAwareRecord's typed catch: the cap is a query pragma outside the
                                    // cache fingerprint, so the whole publish must safe-miss rather than cache a
                                    // pragma-dependent count.
                                    recordCapDropped = true;
                                }
                                onRowError("CSV parse error: " + CsvErrorMessages.summarize(e.getMessage()), e, EMPTY_ROW, true);
                            }
                        }
                    }

                    if (rows.isEmpty()) {
                        return null;
                    }

                    rowStartBytes = trackOffsets ? toLongArray(rowStartBytesList) : null;
                    Page page = convertRowsToPage(rows);
                    if (page != null || modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                        return page;
                    }
                }
            }
        }

        /** Copies a {@code List<Long>} accumulator to a primitive {@code long[]} for the per-batch row-offset array. */
        private static long[] toLongArray(List<Long> values) {
            long[] result = new long[values.size()];
            for (int i = 0; i < values.size(); i++) {
                result[i] = values.get(i);
            }
            return result;
        }

        /**
         * Reads CSV rows using bracket-aware parsing. When a cell starts with {@code [} after a comma
         * and ends with {@code ]} before a comma, commas inside are not column delimiters.
         * The cell value is kept as {@code [a,b,c]} so multi-value conversion can parse it.
         * Supports multi-line quoted fields.
         * <p>
         * Per-line {@code splitLineBracketAware} failures are routed through the error policy so a
         * single malformed line does not abort the batch. {@link IOException}s from the underlying
         * reader are propagated since they signal an unrecoverable I/O fault.
         */
        private String readBracketAwareRecord() throws IOException {
            try {
                return recordReader.readRecord(true);
            } catch (CsvRecordTooLargeException e) {
                totalRowCount++;
                // A cap-determined drop: the max_record_size pragma is not fingerprinted, so this survivor
                // loss is not reproducible from the cache key. Mark the scan uncacheable (safe-miss) so a warm
                // query under a different cap re-scans rather than serving this pragma-dependent count.
                recordCapDropped = true;
                onRowError(e.getMessage(), e, EMPTY_ROW, true);
                return "";
            }
        }

        private void readRowsBracketAware(List<String[]> rows, List<Long> rowStartBytesList, int batchSize) throws IOException {
            String record;
            final boolean trackOffsets = rowStartBytesList != null;
            while (rows.size() < batchSize && (record = readBracketAwareRecord()) != null) {
                if (isBlankOrComment(record, options.commentPrefix())) {
                    continue;
                }
                // Snapshot offset BEFORE splitLineBracketAware so a tokenizer throw still leaves
                // the offset list aligned with rows: a thrown row is not added, no offset added.
                long rowStartByte = trackOffsets ? splitStartByte + recordReader.bytesRead() - recordReader.lastRecordBytes() : 0L;
                try {
                    String[] row = splitLineBracketAware(record);
                    rows.add(row);
                    if (trackOffsets) {
                        rowStartBytesList.add(rowStartByte);
                    }
                } catch (MalformedRowException e) {
                    totalRowCount++;
                    onRowError(e.getMessage(), e, EMPTY_ROW, true);
                }
            }
        }

        /**
         * Splits a CSV line by delimiter, treating quoted fields and {@code [..,..,..]} as single cells.
         * Nested brackets ({@code [[37]]}) stay one cell: closing happens when MVC bracket depth returns to zero.
         * Commas inside quotes or brackets are not delimiters. Escaped commas ({@code \,}) are skipped.
         */
        private String[] splitLineBracketAware(String line) {
            return splitCommaDelimiterBracketAwareFields(
                line,
                options.quoteChar(),
                options.escapeChar(),
                sourceIndexBound,
                options.trimSpaces()
            );
        }

        /**
         * The single seam that installs a record source. Binding the iterator and its value contract together —
         * rather than letting a call site set one and forget the other — is what keeps {@code decodeFieldValue}
         * running exactly once per field: a new record source cannot be introduced without declaring whether it
         * already decoded. {@code deliversDecoded} is true only for {@link CsvRecordIterator}, whose
         * {@code parseRecord} decodes at tokenization.
         */
        private void routeCsvIterator(Iterator<List<?>> iterator, boolean deliversDecoded) {
            csvIterator = iterator;
            csvIteratorDeliversDecoded = deliversDecoded;
        }

        /** Drops the sampling iterator so {@link #readNextBatch}'s routing re-selects (and re-declares) a source. */
        private void clearCsvIterator() {
            routeCsvIterator(null, false);
        }

        private List<Attribute> inferSchemaFromBatchReader(String headerLine) throws IOException {
            String[] columnNames = splitFieldsForOptions(headerLine, options);
            if (options.quoting()) {
                // No type annotations on this path, so the fields are bare names — unwrap RFC 4180 quoting.
                unquoteHeaderNames(columnNames, options.quoteChar());
            }
            routeCsvIterator(newCsvIterator(recordReader), true);
            SchemaSample sample = collectSampleRows(
                csvIterator,
                options.commentPrefix(),
                schemaSampleSize,
                blockFactory.breaker(),
                errorPolicy,
                recordReader,
                splitStartByte
            );
            // Drop the per-record sampling iterator so the bulk Jackson path can pick up where the
            // sample left off. Without this reset, inferred-schema reads stay on the slow per-record
            // CsvLogicalRecordReader path for the remainder of the file.
            clearCsvIterator();
            if (sample.rows().isEmpty()) {
                blockFactory.breaker().addWithoutBreaking(-sample.reservedBytes());
                return null;
            }
            prefetchedRows = sample.rows();
            prefetchedRowsBytes = sample.reservedBytes();
            prefetchedRowStartBytes = sample.rowStartBytes();
            if (sample.recordCapDropped()) {
                recordCapDropped = true; // cap-determined survivor loss during sampling — publish must safe-miss
            }
            maybeHintUndecodedNullMarker(sample.rows(), sourceLocation);
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows(), options.datetimeFormatter());
        }

        private List<Attribute> inferSchemaHeaderlessFromBatchReader() throws IOException {
            routeCsvIterator(newCsvIterator(recordReader), true);
            SchemaSample sample = collectSampleRows(
                csvIterator,
                options.commentPrefix(),
                schemaSampleSize,
                blockFactory.breaker(),
                errorPolicy,
                recordReader,
                splitStartByte
            );
            clearCsvIterator();
            if (sample.rows().isEmpty()) {
                blockFactory.breaker().addWithoutBreaking(-sample.reservedBytes());
                return null;
            }
            prefetchedRows = sample.rows();
            prefetchedRowsBytes = sample.reservedBytes();
            prefetchedRowStartBytes = sample.rowStartBytes();
            if (sample.recordCapDropped()) {
                recordCapDropped = true; // cap-determined survivor loss during sampling — publish must safe-miss
            }
            maybeHintUndecodedNullMarker(sample.rows(), sourceLocation);
            return inferSyntheticSchema(sample.rows(), options.columnPrefix(), options.datetimeFormatter());
        }

        /** The raw field index a pinned-schema position reads; the identity under positional binding. */
        private int rawFieldIndex(int schemaPosition) {
            return schemaFieldIndex == null ? schemaPosition : schemaFieldIndex[schemaPosition];
        }

        private void initProjection() {
            int schemaSize = schema.size();
            schemaColumnCount = schemaSize;
            rowWidthLimit = schemaFieldIndex == null ? schemaSize : Integer.MAX_VALUE;
            // Schema position per projected slot, tracked alongside projectedIdx because a declared path makes the
            // two diverge: projectedIdx is the RAW FIELD INDEX to read out of the record, schemaPos names the
            // declaring attribute. Without a declared path schemaFieldIndex is null, rawFieldIndex() is the
            // identity, and the two arrays are equal — today's positional contract, unchanged.
            int[] schemaPos;
            if (projectedColumns == null) {
                // Identity projection — every slot maps 1:1 to a source column; no synthetic kinds.
                columnCount = schemaSize;
                projectedIdx = new int[schemaSize];
                schemaPos = new int[schemaSize];
                syntheticKinds = new SyntheticColumns.Kind[schemaSize];
                for (int i = 0; i < schemaSize; i++) {
                    schemaPos[i] = i;
                    projectedIdx[i] = rawFieldIndex(i);
                }
            } else if (projectedColumns.isEmpty()) {
                columnCount = 0;
                projectedIdx = new int[0];
                schemaPos = new int[0];
                syntheticKinds = new SyntheticColumns.Kind[0];
            } else {
                columnCount = projectedColumns.size();
                projectedIdx = new int[columnCount];
                schemaPos = new int[columnCount];
                Arrays.fill(schemaPos, -1);
                syntheticKinds = new SyntheticColumns.Kind[columnCount];
                for (int c = 0; c < columnCount; c++) {
                    String colName = projectedColumns.get(c);
                    SyntheticColumns.Kind kind = SyntheticColumns.kindOf(colName);
                    if (kind != null) {
                        // No backing CSV source column; the convert paths fill this slot from a
                        // per-kind value source. projectedIdx stays as the kind-agnostic "no
                        // source column here" sentinel; syntheticKinds carries the discriminator.
                        projectedIdx[c] = -1;
                        syntheticKinds[c] = kind;
                        if (kind == SyntheticColumns.Kind.ROW_POSITION) {
                            rowPositionSlot = c;
                        }
                        continue;
                    }
                    int index = -1;
                    for (int i = 0; i < schemaSize; i++) {
                        if (schema.get(i).name().equals(colName)) {
                            index = i;
                            break;
                        }
                    }
                    if (index == -1) {
                        throw new EsqlIllegalArgumentException("Column not found in CSV schema: [{}]", colName);
                    }
                    schemaPos[c] = index;
                    projectedIdx[c] = rawFieldIndex(index);
                }
            }
            projectedTypes = new DataType[columnCount];
            projectedAttrs = new Attribute[columnCount];
            for (int i = 0; i < columnCount; i++) {
                SyntheticColumns.Kind kind = syntheticKinds[i];
                if (kind != null) {
                    // Registry-driven: the Kind carries its own attribute shape, so a new member
                    // works here without a per-kind dispatch arm.
                    projectedTypes[i] = kind.dataType();
                    projectedAttrs[i] = SyntheticColumns.newAttribute(kind);
                    continue;
                }
                Attribute attr = schema.get(schemaPos[i]);
                projectedAttrs[i] = attr;
                projectedTypes[i] = attr.dataType();
            }
            // Resolve declared per-column date formatters ONCE (never per row). declaredDateFormats is physical-keyed
            // and projectedAttrs carry physical (post-rename) names, so they line up by name. A column with no declared
            // format keeps a null slot and follows today's ISO/file-level path unchanged.
            if (declaredDateFormats.isEmpty() == false) {
                declaredFormatters = new DateFormatter[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Attribute attr = projectedAttrs[i];
                    String pattern = attr != null ? declaredDateFormats.get(attr.name()) : null;
                    if (pattern != null) {
                        declaredFormatters[i] = DateFormatter.forPattern(pattern);
                    }
                }
            }
            int[] stringSlots = new int[columnCount];
            int stringColumns = 0;
            for (int i = 0; i < columnCount; i++) {
                if (DataType.isString(projectedTypes[i])) {
                    stringSlots[stringColumns++] = i;
                }
            }
            byteHintColumns = Arrays.copyOf(stringSlots, stringColumns);
            columnToByteHintSlot = new int[columnCount];
            Arrays.fill(columnToByteHintSlot, -1);
            for (int slot = 0; slot < byteHintColumns.length; slot++) {
                columnToByteHintSlot[byteHintColumns[slot]] = slot;
            }
            rowBuffer = new Object[columnCount];

            // A declared path may bind a raw index at or beyond the schema's own size (declare 5 columns of a
            // 105-column file with path: "col100"), so size by the widest bound index rather than by schemaSize.
            sourceIndexBound = schemaSize;
            for (int i = 0; i < columnCount; i++) {
                if (projectedIdx[i] >= sourceIndexBound) {
                    sourceIndexBound = projectedIdx[i] + 1;
                }
            }
            projectedFieldSet = new BitSet(sourceIndexBound);
            sourceToBufferIndex = new int[sourceIndexBound];
            Arrays.fill(sourceToBufferIndex, -1);
            for (int i = 0; i < columnCount; i++) {
                if (projectedIdx[i] < 0) {
                    continue;
                }
                projectedFieldSet.set(projectedIdx[i]);
                sourceToBufferIndex[projectedIdx[i]] = i;
            }
            if ((directBlockPlain || directBlockQuoted) && columnCount > 0) {
                keywordScratch = new BytesRef[columnCount];
                directElements = new ElementType[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    directElements[i] = DeclaredTypeCoercions.elementTypeFor(projectedTypes[i]);
                }
                stageLong = new long[columnCount];
                stageInt = new int[columnCount];
                stageDouble = new double[columnCount];
                stageBool = new boolean[columnCount];
                stageRef = new BytesRef[columnCount];
                stageNull = new boolean[columnCount];
                // For cacheable reads, fold the cache's per-column null/min/max stats into the parse
                // loop (see appendStagedRow) instead of re-walking every built block. Build the
                // accumulator eagerly here so the staging loop can feed it; captureBlockStats then skips
                // direct-produced pages (lastBatchAccumulatedStats) to avoid double counting.
                if (cacheableObject != null) {
                    columnStats = ColumnStatsAccumulator.forProjectedAttributes(projectedAttrs);
                    accumulateDirectStats = columnStats.isEmpty() == false;
                }
            }
        }

        private Page convertRowsToPage(List<String[]> rows) {
            int schemaSize = schema.size();
            // COUNT(*) fast path: no columns projected, so skip builder allocation and type conversion
            // and emit a row-count-only Page. We still apply the column-count validation that the
            // regular path uses so structural errors are routed through the policy consistently.
            if (columnCount == 0) {
                int acceptedRows = 0;
                SurvivorOffsets survivors = SurvivorOffsets.of(rowStartBytes, rows.size());
                for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
                    String[] row = rows.get(rowIdx);
                    totalRowCount++;
                    if (row.length > rowWidthLimit) {
                        onRowError(
                            "CSV row has [" + row.length + "] columns but schema defines [" + schemaSize + "] columns",
                            null,
                            row,
                            true
                        );
                        continue;
                    }
                    // ALL scope: COUNT(*) projects zero columns, so the page carries no blocks — harvest
                    // every file column's min/max/null straight from the raw record here. rowIdx indexes
                    // rowStartBytes (built parallel to rows) so the stripe attribution is exact.
                    harvestAllColumns(row, rowIdx);
                    survivors.accept(rowIdx);
                    acceptedRows++;
                }
                acceptedRowStartBytes = survivors.finish();
                return acceptedRows == 0 ? null : new Page(acceptedRows);
            }
            BlockUtils.BuilderWrapper[] builders = new BlockUtils.BuilderWrapper[columnCount];
            try {
                // Only KEYWORD/TEXT get a byte-size hint: their stored bytes are the source string's UTF-8 bytes
                // verbatim, so the column's byte size is knowable here and lets BytesRefArray size its buffer once.
                // Fixed-width types are already sized by the position count; IP and VERSION map to BYTES_REF but
                // store encoded bytes unrelated to the source length, so they stay unhinted. byteHintColumns is
                // precomputed to the string slots only, so the hint pass is skipped entirely for numeric
                // projections and never walks non-string columns.
                long[] byteHints = new long[columnCount];
                if (byteHintColumns.length > 0) {
                    for (String[] row : rows) {
                        for (int slot : byteHintColumns) {
                            int si = projectedIdx[slot];
                            if (si >= 0 && si < row.length && row[si] != null) {
                                byteHints[slot] += UnicodeUtil.calcUTF16toUTF8Length(row[si], 0, row[si].length());
                            }
                        }
                    }
                }
                for (int i = 0; i < columnCount; i++) {
                    builders[i] = BlockUtils.wrapperFor(
                        blockFactory,
                        DeclaredTypeCoercions.elementTypeFor(projectedTypes[i]),
                        rows.size(),
                        byteHints[i]
                    );
                }
                int acceptedRows = 0;
                SurvivorOffsets survivors = SurvivorOffsets.of(rowStartBytes, rows.size());
                for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
                    String[] row = rows.get(rowIdx);
                    totalRowCount++;
                    if (row.length > rowWidthLimit) {
                        onRowError(
                            "CSV row has [" + row.length + "] columns but schema defines [" + schemaSize + "] columns",
                            null,
                            row,
                            true
                        );
                        continue;
                    }
                    if (convertRowInPlace(row, rowIdx)) {
                        for (int i = 0; i < columnCount; i++) {
                            builders[i].append().accept(rowBuffer[i]);
                        }
                        // ALL scope: harvest every file column (incl. unprojected) from the raw record.
                        harvestAllColumns(row, rowIdx);
                        survivors.accept(rowIdx);
                        acceptedRows++;
                    }
                }
                acceptedRowStartBytes = survivors.finish();
                if (acceptedRows == 0) {
                    return null;
                }
                Block[] blocks = new Block[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    blocks[i] = builders[i].builder().build();
                }
                return new Page(acceptedRows, blocks);
            } finally {
                Releasables.closeExpectNoException(builders);
            }
        }

        private boolean convertRowInPlace(String[] row, int rowIdx) {
            int mode = this.modeOrdinal;
            // Emit the file-global byte offset captured at record-read time so the value is stable
            // for the same physical record regardless of which split surfaced it.
            if (rowPositionSlot >= 0) {
                rowBuffer[rowPositionSlot] = rowStartBytes[rowIdx];
            }
            for (int i = 0; i < columnCount; i++) {
                if (i == rowPositionSlot) {
                    continue;
                }
                int si = projectedIdx[i];
                String value = si >= 0 && si < row.length ? row[si] : null;
                Object result = tryConvertValue(value, projectedTypes[i], i);
                if (lastFieldError != null) {
                    if (mode == ErrorPolicy.Mode.NULL_FIELD.ordinal()) {
                        rowBuffer[i] = null;
                        onFieldError(lastFieldError, value, projectedAttrs[i]);
                        lastFieldError = null;
                    } else {
                        String err = lastFieldError;
                        lastFieldError = null;
                        onRowError(err, null, row, false);
                        return false;
                    }
                } else {
                    rowBuffer[i] = result;
                }
            }
            return true;
        }

        /**
         * ALL-scope side-pass: harvest min/max/null for EVERY file column from the raw parsed record,
         * independently of projection. The output page (built by the projected paths) only carries the
         * query's columns, so this reads the raw {@code String[]} — which always holds every field — and
         * type-converts each field against the FULL file schema, feeding the per-column accumulator. On the
         * stripe path the row is attributed to its own stripe (by {@link #rowStartBytes}); off the stripe
         * path it folds into the single whole-file accumulator. A convert failure for an unprojected column
         * contributes a null (never poisons the row or the projected error policy — {@link #lastFieldError}
         * is saved and restored). No-op unless scope is ALL and stats capture is live.
         */
        private void harvestAllColumns(String[] row, int rowIdx) {
            if (statsColumnScope != StripeColumnScope.ALL || cacheableObject == null || schema == null) {
                return;
            }
            ColumnStatsAccumulator acc;
            if (statsStripeSize > 0) {
                if (rowStartBytes == null || stripeCaptureDisabled || rowIdx >= rowStartBytes.length) {
                    return; // stripe attribution unavailable -> safe miss (a warm aggregate re-scans)
                }
                long ordinal = stripeHarvester.ordinalOf(rowStartBytes[rowIdx]);
                StripeStatsHarvester.StripeAccum stripe = stripeHarvester.getOrCreate(ordinal);
                if (stripe.allCols == null) {
                    stripe.allCols = ColumnStatsAccumulator.forSchema(schema);
                }
                acc = stripe.allCols;
            } else {
                if (allFileColumnStats == null) {
                    allFileColumnStats = ColumnStatsAccumulator.forSchema(schema);
                }
                acc = allFileColumnStats;
            }
            if (acc.isEmpty()) {
                return;
            }
            int n = schemaColumnCount;
            String savedError = lastFieldError;
            for (int si = 0; si < n; si++) {
                // Read the raw field this schema position BINDS (a declared path makes the two differ), so the value
                // is attributed to the column it belongs to rather than the column sitting at its position.
                int fi = rawFieldIndex(si);
                String value = fi >= 0 && fi < row.length ? row[fi] : null;
                // Stats-harvest accumulator over the full schema (S4 seam): declaredFormatters is projected-aligned, so
                // the schema index si would misindex it. Pass -1 so this path uses the default datetime parse (no declared
                // formatter). Full S4 fix (decline harvesting declared-format/retyped columns outright) lands separately.
                Object converted = tryConvertValue(value, schema.get(si).dataType(), -1);
                if (lastFieldError != null) {
                    lastFieldError = null; // an unparseable file column contributes a null; never poisons the harvest
                    converted = null;
                }
                acc.acceptValueAt(si, converted);
            }
            lastFieldError = savedError;
        }

        /**
         * Collects logical lines from the bracket-aware CSV reader, handling multi-line quoted
         * fields. Blank/comment lines are skipped. Collected lines are not yet split or
         * type-converted — that work is deferred to {@link #splitAndConvertProjected}.
         */
        private void readLogicalLinesBracketAware(List<String> lines, List<Long> lineStartBytes, int batchSize) throws IOException {
            String record;
            final boolean trackOffsets = lineStartBytes != null;
            while (lines.size() < batchSize && (record = readBracketAwareRecord()) != null) {
                if (isBlankOrComment(record, options.commentPrefix())) {
                    continue;
                }
                lines.add(record);
                if (trackOffsets) {
                    lineStartBytes.add(splitStartByte + recordReader.bytesRead() - recordReader.lastRecordBytes());
                }
            }
        }

        /**
         * Converts raw logical CSV lines to a {@link Page} using the fused
         * {@link #splitAndConvertProjected} path: splitting, projection filtering, and typed
         * conversion happen in a single character walk per line.
         */
        private Page convertLinesToPage(List<String> lines) {
            BlockUtils.BuilderWrapper[] builders = new BlockUtils.BuilderWrapper[columnCount];
            try {
                for (int i = 0; i < columnCount; i++) {
                    builders[i] = BlockUtils.wrapperFor(
                        blockFactory,
                        DeclaredTypeCoercions.elementTypeFor(projectedTypes[i]),
                        lines.size()
                    );
                }
                int acceptedRows = 0;
                // Same survivor-offset rebuild as convertRowsToPage (via the shared SurvivorOffsets): a dropped
                // line must not desync rowStartBytes from the emitted page, or accumulateStripes safe-misses or
                // mis-attributes. This bracket-aware path drops rows too, so it feeds the same primitive.
                SurvivorOffsets survivors = SurvivorOffsets.of(rowStartBytes, lines.size());
                for (int lineIdx = 0; lineIdx < lines.size(); lineIdx++) {
                    String line = lines.get(lineIdx);
                    totalRowCount++;
                    try {
                        if (splitAndConvertProjected(line, lineIdx)) {
                            for (int i = 0; i < columnCount; i++) {
                                builders[i].append().accept(rowBuffer[i]);
                            }
                            survivors.accept(lineIdx);
                            acceptedRows++;
                        }
                    } catch (MalformedRowException e) {
                        onRowError(e.getMessage(), e, line, true);
                    }
                }
                acceptedRowStartBytes = survivors.finish();
                if (acceptedRows == 0) {
                    return null;
                }
                Block[] blocks = new Block[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    blocks[i] = builders[i].builder().build();
                }
                return new Page(acceptedRows, blocks);
            } finally {
                Releasables.closeExpectNoException(builders);
            }
        }

        /**
         * Advances {@link #recordReader} to the next direct-path data record, returning a status that
         * tells the batch loop whether to consume, skip, or stop. Blank and comment records are
         * {@link #DIRECT_SKIP}ped (Jackson-compatible, first-cell-bounded comment detection on the raw
         * {@code char[]}, no row String); a per-record size-cap violation is routed through the error
         * policy and also skipped, mirroring the recovery contract of the String path. The record, when
         * {@link #DIRECT_DATA}, is exposed via {@code recordReader.recordBuffer()/recordLength()}.
         */
        private int advanceDirectRecord() throws IOException {
            try {
                if (recordReader.nextRecord() == false) {
                    return DIRECT_EOF;
                }
            } catch (CsvRecordTooLargeException e) {
                totalRowCount++;
                onRowError(e.getMessage(), e, EMPTY_ROW, true);
                // Cap-determined survivor loss on the direct path — the harvested stats now under-count, so the
                // whole-file/stripe publish must safe-miss (matches the bracket, Jackson-bulk, and sampling
                // catch sites; the publish gate at closeInternal checks recordCapDropped == false).
                recordCapDropped = true;
                return DIRECT_SKIP;
            }
            char[] recBuf = recordReader.recordBuffer();
            int recLen = recordReader.recordLength();
            if (isBlankOrCommentFirstCell(recBuf, 0, recLen, options.commentPrefix(), options.delimiter())) {
                return DIRECT_SKIP;
            }
            // The raw first-cell comment check above is exact unless decoding could move the first
            // cell's leading bytes: a field-leading quote (the quote is stripped) or, with escapes
            // active, an escape that rewrites the prefix region. On the quoted path those records are
            // re-checked against the decoded, trimmed first cell so a quoted/escaped comment (for
            // example {@code "//x",a}) is skipped exactly as the Jackson bulk filter skips it.
            if (directBlockQuoted
                && hasCommentFilter
                && firstCellMayDecodeToComment(recBuf, recLen)
                && decodedFirstCellIsComment(recBuf, recLen)) {
                return DIRECT_SKIP;
            }
            return DIRECT_DATA;
        }

        /**
         * Cheap guard for {@link #decodedFirstCellIsComment}: returns {@code true} only when decoding
         * the first cell could change whether it starts with the comment prefix, i.e. the first
         * non-whitespace char is the quote (a field-leading quote is stripped on decode) or, when
         * escapes are active, an escape occurs in the first cell (it can rewrite the leading bytes).
         * For the common first cell (a plain id/keyword with no leading quote or escape) the raw check
         * in {@link #advanceDirectRecord} is exact and the decode is skipped.
         */
        private boolean firstCellMayDecodeToComment(char[] buf, int to) {
            final char delim = options.delimiter();
            int p = 0;
            while (p < to && buf[p] <= ' ' && buf[p] != delim) {
                p++;
            }
            if (p >= to) {
                return false;
            }
            if (buf[p] == options.quoteChar()) {
                return true;
            }
            if (options.escaping()) {
                final char esc = options.escapeChar();
                for (int k = p; k < to && buf[k] != delim; k++) {
                    if (buf[k] == esc) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Whether the record's first cell, decoded and trimmed as Jackson parses it, starts with the
         * comment prefix. Mirrors the Jackson bulk filter ({@code row[0].trim().startsWith(prefix)}) by
         * decoding the first cell with the same quoted/escaped rules as {@link #splitAndConvertQuoted}
         * into the reusable {@link #quotedBuf}, then trimming (chars {@code <= ' '}) before the prefix
         * test. Only invoked on the quoted path for first cells {@link #firstCellMayDecodeToComment}
         * flags, so it is off the common hot path.
         */
        private boolean decodedFirstCellIsComment(char[] buf, int to) {
            final String prefix = options.commentPrefix();
            final char delim = options.delimiter();
            final char quote = options.quoteChar();
            final char esc = options.escapeChar();
            final boolean escapeAware = options.escaping();
            StringBuilder cell = resetQuotedBuf();
            int p = 0;
            while (p < to && buf[p] <= ' ' && buf[p] != delim) {
                p++;
            }
            if (p < to && buf[p] == quote) {
                int q = p + 1;
                while (q < to) {
                    char c = buf[q];
                    if (c == quote) {
                        if (q + 1 < to && buf[q + 1] == quote) {
                            cell.append(quote);
                            q += 2;
                            continue;
                        }
                        break;
                    }
                    if (escapeAware && c == esc) {
                        if (q + 1 < to) {
                            cell.append(decodeQuotedEscapeChar(buf[q + 1]));
                            q += 2;
                        } else {
                            q++;
                        }
                        continue;
                    }
                    cell.append(c);
                    q++;
                }
            } else {
                int j = p;
                while (j < to && buf[j] != delim) {
                    char c = buf[j];
                    if (escapeAware && c == esc) {
                        if (j + 1 < to) {
                            cell.append(decodeQuotedEscapeChar(buf[j + 1]));
                            j += 2;
                        } else {
                            j++;
                        }
                        continue;
                    }
                    cell.append(c);
                    j++;
                }
            }
            int s = 0;
            int e = cell.length();
            while (s < e && cell.charAt(s) <= ' ') {
                s++;
            }
            while (e > s && cell.charAt(e - 1) <= ' ') {
                e--;
            }
            if (e - s < prefix.length()) {
                return false;
            }
            for (int k = 0; k < prefix.length(); k++) {
                if (cell.charAt(s + k) != prefix.charAt(k)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Reads and converts one direct-block batch into a {@link Page}, fusing record reading and field
         * parsing so each record is walked straight out of {@link #recordReader}'s reusable {@code char[]}
         * with no per-row {@link String}. Walkers ({@link #splitAndConvertPlain} / {@link #splitAndConvertQuoted})
         * write typed values into the staging slots, flushed to the block builders without boxing (see
         * {@link #appendStagedRow}). COUNT(*) (no projected columns) takes the row-count-only fast path.
         *
         * <p>{@link #directBatchRecordsRead} is set to the number of data records consumed this batch so
         * the caller can distinguish end of stream (zero) from an all-skipped lenient batch.
         *
         * @return the page, or {@code null} when no rows were accepted (end of stream or all rows dropped)
         */
        private Page convertDirectBatchToPage(int batchSize) throws IOException {
            directBatchRecordsRead = 0;
            lastBatchAccumulatedStats = accumulateDirectStats;
            // Stripe capture on the direct path: record each data record's file-global byte start (the same
            // splitStartByte + bytesRead - lastRecordBytes axis the record-reader path uses, now byte-exact on
            // the bulk reader once nextRecord() commits byte accounting), then hand the SURVIVING rows' offsets to
            // captureBlockStats->accumulateStripes via rowStartBytes/acceptedRowStartBytes. The emit-time
            // byte-exactness tripwire safe-misses on any skew, so a misattribution never serves a wrong value.
            final boolean captureStripeOffsets = statsStripeSize > 0 && cacheableObject != null && stripeCaptureDisabled == false;
            final long[] directOffsets = captureStripeOffsets ? new long[batchSize] : null;
            final boolean[] directSurvived = captureStripeOffsets ? new boolean[batchSize] : null;
            if (columnCount == 0) {
                int acceptedRows = 0;
                while (directBatchRecordsRead < batchSize) {
                    int status = advanceDirectRecord();
                    if (status == DIRECT_EOF) {
                        break;
                    }
                    if (status == DIRECT_SKIP) {
                        continue;
                    }
                    int dataIdx = directBatchRecordsRead;
                    directBatchRecordsRead++;
                    totalRowCount++;
                    if (captureStripeOffsets) {
                        directOffsets[dataIdx] = splitStartByte + recordReader.bytesRead() - recordReader.lastRecordBytes();
                    }
                    if (splitAndConvertDirect(recordReader.recordBuffer(), 0, recordReader.recordLength())) {
                        if (captureStripeOffsets) {
                            directSurvived[dataIdx] = true;
                        }
                        acceptedRows++;
                    }
                }
                if (captureStripeOffsets) {
                    setDirectStripeOffsets(directOffsets, directSurvived, directBatchRecordsRead);
                }
                return acceptedRows == 0 ? null : new Page(acceptedRows);
            }
            final boolean useByteHint = byteHintColumns.length > 0;
            BlockUtils.BuilderWrapper[] builders = new BlockUtils.BuilderWrapper[columnCount];
            try {
                // Non-byte-hinted columns are sized by position count and filled as the batch is parsed.
                // Byte-hinted (KEYWORD/TEXT) columns are left null here: the direct path streams records, so
                // a string column's total byte size is unknown until the whole batch is parsed. Their values
                // are retained while parsing, then replayed into a builder sized with the exact byte total so
                // the BytesRefArray byte storage is allocated once instead of regrowing, matching the
                // byte-hinted Jackson path in convertRowsToPage (see buildRetainedByteHintColumns).
                for (int i = 0; i < columnCount; i++) {
                    if (useByteHint && columnToByteHintSlot[i] >= 0) {
                        continue;
                    }
                    builders[i] = BlockUtils.wrapperFor(blockFactory, DeclaredTypeCoercions.elementTypeFor(projectedTypes[i]), batchSize);
                }
                if (useByteHint) {
                    resetByteHintRetain();
                }
                int acceptedRows = 0;
                while (directBatchRecordsRead < batchSize) {
                    int status = advanceDirectRecord();
                    if (status == DIRECT_EOF) {
                        break;
                    }
                    if (status == DIRECT_SKIP) {
                        continue;
                    }
                    int dataIdx = directBatchRecordsRead;
                    directBatchRecordsRead++;
                    totalRowCount++;
                    if (captureStripeOffsets) {
                        directOffsets[dataIdx] = splitStartByte + recordReader.bytesRead() - recordReader.lastRecordBytes();
                    }
                    if (splitAndConvertDirect(recordReader.recordBuffer(), 0, recordReader.recordLength())) {
                        if (useByteHint) {
                            appendStagedRowDeferringByteHint(builders);
                        } else {
                            appendStagedRow(builders);
                        }
                        if (captureStripeOffsets) {
                            directSurvived[dataIdx] = true;
                        }
                        acceptedRows++;
                    }
                }
                if (captureStripeOffsets) {
                    setDirectStripeOffsets(directOffsets, directSurvived, directBatchRecordsRead);
                }
                if (acceptedRows == 0) {
                    return null;
                }
                if (useByteHint) {
                    buildRetainedByteHintColumns(builders, acceptedRows);
                }
                Block[] blocks = new Block[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    blocks[i] = builders[i].builder().build();
                }
                return new Page(acceptedRows, blocks);
            } finally {
                Releasables.closeExpectNoException(builders);
            }
        }

        /**
         * Publishes the direct path's per-record stripe offsets: {@link #rowStartBytes} (every data record, in
         * read order) gates the {@link #captureBlockStats} -> {@link #accumulateStripes} branch, and
         * {@link #acceptedRowStartBytes} (page-aligned survivors, via the shared {@link SurvivorOffsets}) is what
         * accumulateStripes attributes by — exactly as {@link #convertRowsToPage} builds them on the non-direct
         * path. A record dropped by the error policy is absent from both the page and the survivor offsets.
         */
        private void setDirectStripeOffsets(long[] offsets, boolean[] survived, int dataRecords) {
            rowStartBytes = Arrays.copyOf(offsets, dataRecords);
            SurvivorOffsets survivors = SurvivorOffsets.of(rowStartBytes, dataRecords);
            for (int i = 0; i < dataRecords; i++) {
                if (survived[i]) {
                    survivors.accept(i);
                }
            }
            acceptedRowStartBytes = survivors.finish();
        }

        /** Resets the per-batch byte-hint retain buffers, allocating them on first use and reusing them after. */
        private void resetByteHintRetain() {
            if (kwRetainData == null) {
                int slots = byteHintColumns.length;
                kwRetainData = new byte[slots][];
                kwRetainDataLen = new int[slots];
                kwRetainValueLen = new int[slots][];
            }
            kwRetainRows = 0;
            Arrays.fill(kwRetainDataLen, 0);
        }

        /**
         * Like {@link #appendStagedRow} but retains byte-hinted (KEYWORD/TEXT) columns into the per-batch
         * buffers instead of appending them, so their builders can be sized up-front once the batch's exact
         * byte total is known (see {@link #buildRetainedByteHintColumns}). Non-hinted columns append
         * immediately, exactly as on the {@link #appendStagedRow} path. Column stats, when enabled, are fed
         * here for retained columns so the result is identical regardless of which path runs.
         */
        private void appendStagedRowDeferringByteHint(BlockUtils.BuilderWrapper[] builders) {
            final boolean stats = accumulateDirectStats;
            final int row = kwRetainRows;
            for (int i = 0; i < columnCount; i++) {
                int slot = columnToByteHintSlot[i];
                if (slot < 0) {
                    appendStagedColumn(builders, i);
                    continue;
                }
                int[] valueLen = kwRetainValueLen[slot];
                if (valueLen == null || row >= valueLen.length) {
                    int newLen = valueLen == null ? Math.max(16, row + 1) : Math.max(row + 1, valueLen.length * 2);
                    valueLen = valueLen == null ? new int[newLen] : Arrays.copyOf(valueLen, newLen);
                    kwRetainValueLen[slot] = valueLen;
                }
                if (stageNull[i]) {
                    valueLen[row] = -1;
                    if (stats) {
                        columnStats.acceptNullAt(i);
                    }
                } else {
                    BytesRef v = stageRef[i];
                    byte[] data = kwRetainData[slot];
                    int used = kwRetainDataLen[slot];
                    int need = used + v.length;
                    if (data == null || need > data.length) {
                        int newCap = data == null ? Math.max(64, need) : Math.max(need, data.length * 2);
                        data = data == null ? new byte[newCap] : Arrays.copyOf(data, newCap);
                        kwRetainData[slot] = data;
                    }
                    System.arraycopy(v.bytes, v.offset, data, used, v.length);
                    valueLen[row] = v.length;
                    kwRetainDataLen[slot] = need;
                    if (stats) {
                        columnStats.acceptBytesRefAt(i, v);
                    }
                }
            }
            kwRetainRows++;
        }

        /**
         * Creates the byte-hinted column builders deferred during the batch parse and replays their retained
         * values in row order. Each builder is sized with the exact retained byte total so its byte storage
         * is allocated once. The created wrappers are stored back into {@code builders} so the caller's
         * assembly loop builds them and its {@code finally} releases them on any failure.
         */
        private void buildRetainedByteHintColumns(BlockUtils.BuilderWrapper[] builders, int acceptedRows) {
            for (int slot = 0; slot < byteHintColumns.length; slot++) {
                int col = byteHintColumns[slot];
                BlockUtils.BuilderWrapper wrapper = BlockUtils.wrapperFor(
                    blockFactory,
                    ElementType.BYTES_REF,
                    acceptedRows,
                    kwRetainDataLen[slot]
                );
                builders[col] = wrapper;
                BytesRefBlock.Builder builder = (BytesRefBlock.Builder) wrapper.builder();
                byte[] data = kwRetainData[slot];
                int[] valueLen = kwRetainValueLen[slot];
                BytesRef scratch = new BytesRef();
                int offset = 0;
                for (int row = 0; row < acceptedRows; row++) {
                    int len = valueLen[row];
                    if (len < 0) {
                        builder.appendNull();
                    } else {
                        // data is non-null whenever any row contributed bytes (len >= 0 below implies a
                        // non-null value was retained, so the column's byte buffer was allocated).
                        scratch.bytes = data;
                        scratch.offset = offset;
                        scratch.length = len;
                        builder.appendBytesRef(scratch);
                        offset += len;
                    }
                }
            }
        }

        /**
         * Flushes the typed staging slots of one accepted row into the block builders. Each primitive
         * element type appends straight from its typed array (no boxing).
         */
        private void appendStagedRow(BlockUtils.BuilderWrapper[] builders) {
            for (int i = 0; i < columnCount; i++) {
                appendStagedColumn(builders, i);
            }
        }

        /** Flushes one column's staged value into its builder (no boxing); feeds column stats when enabled. */
        private void appendStagedColumn(BlockUtils.BuilderWrapper[] builders, int i) {
            final boolean stats = accumulateDirectStats;
            Block.Builder builder = builders[i].builder();
            if (stageNull[i]) {
                builder.appendNull();
                if (stats) {
                    columnStats.acceptNullAt(i);
                }
                return;
            }
            switch (directElements[i]) {
                case LONG -> {
                    ((LongBlock.Builder) builder).appendLong(stageLong[i]);
                    if (stats) {
                        columnStats.acceptLongAt(i, stageLong[i]);
                    }
                }
                case INT -> {
                    ((IntBlock.Builder) builder).appendInt(stageInt[i]);
                    if (stats) {
                        columnStats.acceptIntAt(i, stageInt[i]);
                    }
                }
                case DOUBLE -> {
                    ((DoubleBlock.Builder) builder).appendDouble(stageDouble[i]);
                    if (stats) {
                        columnStats.acceptDoubleAt(i, stageDouble[i]);
                    }
                }
                case BOOLEAN -> {
                    ((BooleanBlock.Builder) builder).appendBoolean(stageBool[i]);
                    if (stats) {
                        columnStats.acceptBooleanAt(i, stageBool[i]);
                    }
                }
                case BYTES_REF -> {
                    ((BytesRefBlock.Builder) builder).appendBytesRef(stageRef[i]);
                    if (stats) {
                        columnStats.acceptBytesRefAt(i, stageRef[i]);
                    }
                }
                default -> throw new IllegalStateException("Unexpected element type in direct-block staging: " + directElements[i]);
            }
        }

        private void stageNullValue(int bufIdx) {
            stageNull[bufIdx] = true;
        }

        private void stageLongValue(int bufIdx, long value) {
            stageLong[bufIdx] = value;
            stageNull[bufIdx] = false;
        }

        private void stageIntValue(int bufIdx, int value) {
            stageInt[bufIdx] = value;
            stageNull[bufIdx] = false;
        }

        private void stageDoubleValue(int bufIdx, double value) {
            stageDouble[bufIdx] = value;
            stageNull[bufIdx] = false;
        }

        private void stageRefValue(int bufIdx, BytesRef value) {
            stageRef[bufIdx] = value;
            stageNull[bufIdx] = false;
        }

        /**
         * Stages the result of a (cold) typed string conversion for the direct path, dispatching the
         * boxed value into the matching typed slot so {@link #appendStagedRow} can append it without a
         * second boxing. Mirrors {@link #emitConvertedStringField} but targets the direct path's typed
         * staging instead of the shared {@code rowBuffer}.
         *
         * @return {@code true} if the field was accepted, {@code false} if a row-level error was raised
         */
        private boolean emitConvertedStageField(String value, int bufIdx, DataType dt) {
            // bufIdx is the projected-column slot, so a declared per-column date format is honored on the
            // direct-to-block path exactly as on the Jackson path (both funnel through tryConvertValue).
            Object result = tryConvertValue(value, dt, bufIdx);
            if (lastFieldError != null) {
                if (modeOrdinal == ErrorPolicy.Mode.NULL_FIELD.ordinal()) {
                    stageNullValue(bufIdx);
                    onFieldError(lastFieldError, value, projectedAttrs[bufIdx]);
                    lastFieldError = null;
                    return true;
                } else {
                    String err = lastFieldError;
                    lastFieldError = null;
                    onRowError(err, null, directRawLine(), false);
                    return false;
                }
            }
            stageConvertedValue(bufIdx, result);
            return true;
        }

        /** Routes a converted value into the typed staging slot matching the column's element type. */
        private void stageConvertedValue(int bufIdx, Object result) {
            if (result == null) {
                stageNullValue(bufIdx);
                return;
            }
            switch (directElements[bufIdx]) {
                case LONG -> stageLongValue(bufIdx, (Long) result);
                case INT -> stageIntValue(bufIdx, (Integer) result);
                case DOUBLE -> stageDoubleValue(bufIdx, (Double) result);
                case BOOLEAN -> {
                    stageBool[bufIdx] = (Boolean) result;
                    stageNull[bufIdx] = false;
                }
                case BYTES_REF -> stageRefValue(bufIdx, (BytesRef) result);
                default -> throw new IllegalStateException("Unexpected element type in direct-block staging: " + directElements[bufIdx]);
            }
        }

        /**
         * Routes an over-long field through the error policy with byte-for-byte the same message Jackson's
         * {@code StreamReadConstraints.maxStringLength} violation produces (see {@link #read}, which wires
         * {@code maxFieldSize} into that constraint). The fault is structural and unparsed, so it carries
         * the {@code EMPTY_ROW} excerpt ({@code <unparsed>}) exactly as the Jackson bulk path does.
         *
         * @param valueLen the field's value length in characters (trimmed for unquoted fields, decoded for
         *                 quoted fields), matching the length Jackson reports
         * @return always {@code false}, so callers can {@code return rejectFieldTooLarge(len)} to drop the row
         */
        private boolean rejectFieldTooLarge(int valueLen) {
            onRowError("CSV parse error: " + fieldSizeExceededDetail(valueLen, maxFieldChars), null, EMPTY_ROW, true);
            return false;
        }

        /**
         * Dispatches to the plain or quoted walker, routing an unclosed-quote fault through the policy.
         * The record is a {@code [from, to)} range into the reader's reusable {@code char[]}; it is
         * captured into {@link #directRecBuf}/{@link #directRecFrom}/{@link #directRecTo} so error paths
         * can lazily build the raw row String without allocating one when the row is accepted.
         */
        private boolean splitAndConvertDirect(char[] buf, int from, int to) {
            directRecBuf = buf;
            directRecFrom = from;
            directRecTo = to;
            if (directBlockQuoted) {
                try {
                    return splitAndConvertQuoted(buf, from, to);
                } catch (MalformedRowException e) {
                    onRowError("CSV parse error: " + CsvErrorMessages.summarize(e.getMessage()), e, EMPTY_ROW, true);
                    return false;
                }
            }
            return splitAndConvertPlain(buf, from, to);
        }

        /** Lazily materializes the current direct-path record as a String, for cold error/warning paths only. */
        private String directRawLine() {
            return new String(directRecBuf, directRecFrom, directRecTo - directRecFrom);
        }

        /**
         * Direct-to-block field splitting and typed conversion for the plain (unquoted, unescaped,
         * non-bracket) path. Walks the record range {@code [from, to)} once, splitting on the delimiter,
         * and converts each projected field straight from its character range into the typed staging
         * slots, with no per-cell {@link String} for integers, longs, doubles, or keyword/text values.
         *
         * @return {@code true} if the row was accepted, {@code false} if rejected by the error policy
         */
        private boolean splitAndConvertPlain(char[] buf, int from, int to) {
            final char delim = options.delimiter();
            int fieldIndex = 0;
            int fieldStart = from;
            for (int i = from; i <= to; i++) {
                if (i == to || buf[i] == delim) {
                    if (fieldIndex < sourceIndexBound && projectedFieldSet.get(fieldIndex)) {
                        int bufIdx = sourceToBufferIndex[fieldIndex];
                        if (emitPlainField(buf, fieldStart, i, bufIdx, projectedTypes[bufIdx]) == false) {
                            return false;
                        }
                    } else if (checkUnprojectedFieldCap(buf, fieldStart, i) == false) {
                        // Jackson tokenizes every field, so a too-long non-projected (or beyond-schema)
                        // field trips maxStringLength even though we never materialize its value.
                        return false;
                    }
                    fieldIndex++;
                    fieldStart = i + 1;
                }
            }
            int totalFields = fieldIndex;
            if (totalFields > rowWidthLimit) {
                onRowError(
                    "CSV row has [" + totalFields + "] columns but schema defines [" + schemaColumnCount + "] columns",
                    null,
                    directRawLine(),
                    true
                );
                return false;
            }
            // Null-fill projected columns whose source index falls past the row's trailing edge.
            for (int c = 0; c < columnCount; c++) {
                if (projectedIdx[c] < 0 || projectedIdx[c] >= totalFields) {
                    stageNullValue(c);
                }
            }
            return true;
        }

        /**
         * Stages a present-but-empty field on the direct-to-block path: the empty string on
         * {@code KEYWORD}/{@code TEXT} columns, {@code null} on every other type (which has no empty
         * representation). Mirrors {@link CsvFormatReader#presentEmptyValue} and the {@link #tryConvertValue}
         * empty branch so the direct decoders agree with the Jackson path. A MISSING field (row shorter than
         * the schema) is always {@code null} and is handled by the trailing null-fill, not this method.
         */
        private void stagePresentEmptyValue(int bufIdx, DataType dt) {
            if (DataType.isString(dt)) {
                stageRefValue(bufIdx, EMPTY_STRING);
            } else {
                stageNullValue(bufIdx);
            }
        }

        /**
         * Converts the character range {@code [start, end)} of {@code line} for the given target type
         * and stores the result in the typed staging slot {@code bufIdx} (see {@link #appendStagedRow}).
         * The hot numeric, double, and keyword types are parsed directly from the character range; the
         * remaining types fall back to the shared {@link #emitConvertedStageField} path on a (cold)
         * substring so error handling and value semantics stay identical to the Jackson path.
         *
         * @return {@code true} if the field was accepted, {@code false} if a row-level error was
         *         raised (SKIP_ROW / FAIL_FAST)
         */
        private boolean emitPlainField(char[] buf, int start, int end, int bufIdx, DataType dt) {
            // Whitespace mirrors the Jackson path: a typed column is always trimmed (it parses, like
            // tryConvertValue); a string column keeps its bytes unless trim_spaces is set. The cap
            // (Jackson's maxStringLength) governs the value AS TOKENIZED — trimmed only when trim_spaces is
            // set, regardless of ES type — so a typed no-trim field caps the RAW token, then trims to parse.
            // This keeps the cap decision identical to the Jackson arm and the unprojected cap check, so
            // projection can never change whether a row survives. On the hot no-trim string path both
            // branches skip the loops, so it never costs more than the old unconditional trim.
            final boolean trimSpaces = options.trimSpaces();
            if (trimSpaces) {
                while (start < end && buf[start] <= ' ') {
                    start++;
                }
                while (end > start && buf[end - 1] <= ' ') {
                    end--;
                }
                if (end - start > maxFieldChars) {
                    return rejectFieldTooLarge(end - start);
                }
            } else {
                if (end - start > maxFieldChars) {
                    return rejectFieldTooLarge(end - start);
                }
                if (DataType.isString(dt) == false) {
                    // Mirror tryConvertValue's raw-first null-marker check: a whitespace-bearing null_value
                    // (e.g. " 0 ") must match the UNTRIMMED value, else a typed column trims it away and misses
                    // it while the house arm (which compares the raw field) nulls it — a silent divergence.
                    if (hasCustomNullValue && end - start == nullValueStr.length() && regionEquals(buf, start, nullValueStr)) {
                        stageNullValue(bufIdx);
                        return true;
                    }
                    while (start < end && buf[start] <= ' ') {
                        start++;
                    }
                    while (end > start && buf[end - 1] <= ' ') {
                        end--;
                    }
                }
            }
            // maxFieldChars was already enforced against end-start in both trim branches above; trimming
            // only shrinks the range, so len here is always within the cap and needs no re-check.
            int len = end - start;
            // Null classification mirrors tryConvertValue: a present-but-empty field is the empty string
            // on string columns and null on other types; the literal "null" (any case) is a null marker
            // only for non-string columns, since KEYWORD/TEXT must be able to hold the string "null"; the
            // configured null marker always becomes null.
            if (len == 0) {
                stagePresentEmptyValue(bufIdx, dt);
                return true;
            }
            if (DataType.isString(dt) == false && len == 4 && regionEqualsIgnoreCase(buf, start, "null")) {
                stageNullValue(bufIdx);
                return true;
            }
            if (hasCustomNullValue && len == nullValueStr.length() && regionEquals(buf, start, nullValueStr)) {
                stageNullValue(bufIdx);
                return true;
            }
            switch (dt) {
                case LONG -> {
                    int p = start;
                    boolean neg = buf[p] == '-';
                    if (neg) p++;
                    long acc = parseUnsignedDecimal(buf, p, end);
                    if (acc >= 0) {
                        stageLongValue(bufIdx, neg ? -acc : acc);
                        return true;
                    }
                    return emitConvertedStageField(new String(buf, start, len), bufIdx, dt);
                }
                case INTEGER -> {
                    int p = start;
                    boolean neg = buf[p] == '-';
                    if (neg) p++;
                    long acc = parseUnsignedDecimal(buf, p, end);
                    if (acc >= 0) {
                        long val = neg ? -acc : acc;
                        if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                            stageIntValue(bufIdx, (int) val);
                            return true;
                        }
                    }
                    return emitConvertedStageField(new String(buf, start, len), bufIdx, dt);
                }
                case KEYWORD, TEXT -> {
                    stageRefValue(bufIdx, keywordRef(bufIdx, buf, start, len));
                    return true;
                }
                case DOUBLE -> {
                    // Parse the double straight from the record's char range, skipping the substring.
                    // jackson-core's fast parser is bit-identical to Double.parseDouble on accepted
                    // inputs; on rejection we fall back to the shared conversion path, which itself
                    // uses Double.parseDouble, so the accepted/rejected boundary matches the baseline.
                    try {
                        stageDoubleValue(bufIdx, NumberInput.parseDouble(buf, start, len, true));
                        return true;
                    } catch (NumberFormatException e) {
                        return emitConvertedStageField(new String(buf, start, len), bufIdx, dt);
                    }
                }
                // UNSIGNED_LONG must stay on this cold path: the LONG fast arm above accumulates a raw signed
                // value and can neither sign-flip nor represent anything above Long.MAX_VALUE, so routing
                // unsigned_long through it would silently corrupt exactly the (2^63, 2^64) values it exists for.
                case UNSIGNED_LONG, DATETIME, DATE_NANOS, BOOLEAN, IP, VERSION, NULL -> {
                    return emitConvertedStageField(new String(buf, start, len), bufIdx, dt);
                }
                default -> throw new IllegalArgumentException("Unsupported data type: " + dt);
            }
        }

        /**
         * Parses unsigned decimal digits from {@code buf[p, end)} into a non-negative long. Returns
         * the parsed value on success, or {@code Long.MIN_VALUE} (as a sentinel) if the range is empty,
         * contains a non-digit, or the value overflows {@code Long.MAX_VALUE}. Callers pass {@code p}
         * already advanced past any leading sign character.
         */
        private static long parseUnsignedDecimal(char[] buf, int p, int end) {
            if (p == end) {
                return Long.MIN_VALUE;
            }
            long acc = 0;
            for (; p < end; p++) {
                char c = buf[p];
                if (c < '0' || c > '9') {
                    return Long.MIN_VALUE;
                }
                long nv = acc * 10 + (c - '0');
                if (acc != 0 && nv / 10 != acc) {
                    return Long.MIN_VALUE;
                }
                acc = nv;
            }
            return acc;
        }

        /**
         * Case-insensitive variant of {@link CsvFormatReader#regionEquals}, used for the literal {@code null} marker.
         * Mirrors {@link String#regionMatches(boolean, int, String, int, int)} with {@code ignoreCase=true}
         * (upper- then lower-case fold) so classification matches the Jackson conversion path exactly.
         */
        private static boolean regionEqualsIgnoreCase(char[] buf, int start, String s) {
            for (int k = 0; k < s.length(); k++) {
                char a = buf[start + k];
                char b = s.charAt(k);
                if (a == b) {
                    continue;
                }
                char ua = Character.toUpperCase(a);
                char ub = Character.toUpperCase(b);
                if (ua == ub || Character.toLowerCase(ua) == Character.toLowerCase(ub)) {
                    continue;
                }
                return false;
            }
            return true;
        }

        /**
         * Enforces the per-field length cap for a non-projected unquoted field {@code line[start, end)}.
         * The raw span is an upper bound on the trimmed value length Jackson counts, so the trim work is
         * only paid when the span itself already exceeds the cap.
         *
         * @return {@code true} if within the cap, {@code false} if it was rejected (and the row dropped)
         */
        private boolean checkUnprojectedFieldCap(char[] buf, int start, int end) {
            if (end - start <= maxFieldChars) {
                return true;
            }
            // Only whitespace trimming can bring an over-cap field back under, and only when trim_spaces is
            // set (otherwise the raw bytes are the stored value, so the raw length is what the cap governs).
            if (options.trimSpaces()) {
                while (start < end && buf[start] <= ' ') {
                    start++;
                }
                while (end > start && buf[end - 1] <= ' ') {
                    end--;
                }
            }
            if (end - start > maxFieldChars) {
                return rejectFieldTooLarge(end - start);
            }
            return true;
        }

        /**
         * Encodes {@code buf[start, start+len)} as UTF-8 into the reusable per-column scratch
         * {@link BytesRef} and returns it. The caller stages the reference in a typed staging slot and
         * the page builder copies the bytes synchronously on append, so reusing one scratch per column
         * across rows is safe (a distinct scratch per column avoids in-row aliasing between two
         * keyword columns).
         */
        private BytesRef keywordRef(int bufIdx, char[] buf, int start, int len) {
            BytesRef ref = keywordScratch[bufIdx];
            if (ref == null) {
                ref = new BytesRef();
                keywordScratch[bufIdx] = ref;
            }
            int maxBytes = UnicodeUtil.maxUTF8Length(len);
            if (ref.bytes.length < maxBytes) {
                ref.bytes = new byte[maxBytes];
            }
            ref.offset = 0;
            ref.length = UnicodeUtil.UTF16toUTF8(buf, start, len, ref.bytes);
            return ref;
        }

        /**
         * Direct-to-block field splitting and typed conversion for the quoted (RFC 4180) path,
         * including optional backslash escapes. Matches Jackson's {@code TRIM_SPACES} <em>whitespace and
         * quote</em> grammar (field boundaries, outer-whitespace trimming, {@code ""} quote doubling)
         * <em>and</em> its escape decoding: {@link #decodeQuotedEscapeChar} reproduces Jackson's quoted-escape
         * rule ({@code \t \n \r \0} control, every other {@code \c} literal). Under no-trim this walker
         * (and its string-domain twin {@link #splitRecordFields}) IS the grammar and Jackson is not used,
         * because Jackson's no-trim tokenization diverges (padded quotes mis-split, first-column leading
         * whitespace is eaten). A field-leading quote (after optional outer whitespace)
         * opens a quoted region where {@code ""} is a literal quote and, when escaping is on, {@code \}+char
         * is decoded with Jackson's quoted-escape rule; quoted content (including inner whitespace and embedded newlines) is preserved
         * verbatim, while unquoted fields are trimmed only for typed columns (a keyword keeps its bytes
         * unless trim_spaces). Non-whitespace after a closing quote is a row error, and an empty quoted field
         * ({@code ""}) is a present-but-empty field (the empty string on string columns, null otherwise). Simple
         * unquoted fields (no escape) take the same char-range fast path as
         * {@link #splitAndConvertPlain}; quoted or escaped fields are assembled into a reused buffer.
         *
         * @return {@code true} if the row was accepted, {@code false} if rejected by the error policy
         * @throws MalformedRowException if a quoted field is never closed before end of record
         */
        private boolean splitAndConvertQuoted(char[] buf, int from, int to) {
            final char delim = options.delimiter();
            final char quote = options.quoteChar();
            final char esc = options.escapeChar();
            final boolean escapeAware = options.escaping();
            final int len = to;

            int fieldIndex = 0;
            int i = from;
            while (true) {
                boolean projected = fieldIndex < sourceIndexBound && projectedFieldSet.get(fieldIndex);
                int bufIdx = projected ? sourceToBufferIndex[fieldIndex] : -1;
                DataType dt = projected ? projectedTypes[bufIdx] : null;

                // Skip leading outer whitespace to decide quoted vs unquoted. The delimiter is never
                // skipped even when it is itself a whitespace byte (e.g. a TAB in TSV), so an empty
                // field before a quoted field is not mistaken for the quoted field.
                int p = i;
                while (p < len && buf[p] <= ' ' && buf[p] != delim) {
                    p++;
                }

                int next;
                boolean lastField;
                if (p < len && buf[p] == quote) {
                    StringBuilder value = projected ? resetQuotedBuf() : null;
                    // Decoded value length, tracked even when unprojected (value == null) so the field-size
                    // cap is enforced on every field exactly as Jackson does during tokenization.
                    int decodedLen = 0;
                    int q = p + 1;
                    boolean closed = false;
                    while (q < len) {
                        char c = buf[q];
                        if (c == quote) {
                            if (q + 1 < len && buf[q + 1] == quote) {
                                if (value != null) {
                                    value.append(quote);
                                }
                                decodedLen++;
                                q += 2;
                                continue;
                            }
                            closed = true;
                            q++;
                            break;
                        }
                        if (escapeAware && c == esc) {
                            if (q + 1 < len) {
                                if (value != null) {
                                    value.append(decodeQuotedEscapeChar(buf[q + 1]));
                                }
                                decodedLen++;
                                q += 2;
                            } else {
                                q++; // trailing lone escape: dropped
                            }
                            continue;
                        }
                        if (value != null) {
                            value.append(c);
                        }
                        decodedLen++;
                        q++;
                    }
                    if (closed == false) {
                        throw MalformedRowException.unclosedQuotedField(directRawLine(), p - from);
                    }
                    // Jackson checks maxStringLength on the aggregated value right after the closing quote,
                    // before inspecting any trailing content, so the cap precedes the content-after-quote check.
                    if (decodedLen > maxFieldChars) {
                        return rejectFieldTooLarge(decodedLen);
                    }
                    // After the closing quote only trailing whitespace may precede the delimiter.
                    // The delimiter itself is never consumed here even when it is a whitespace byte
                    // (e.g. a TAB in TSV), otherwise the field boundary would be lost.
                    int r = q;
                    while (r < len && buf[r] <= ' ' && buf[r] != delim) {
                        r++;
                    }
                    if (r < len && buf[r] != delim) {
                        onRowError("CSV parse error: CSV row has unexpected content after a closing quote", null, EMPTY_ROW, true);
                        return false;
                    }
                    if (projected) {
                        if (value.length() == 0) {
                            // Empty quoted field ("") is a present-but-empty field: empty string on
                            // string columns, null otherwise (matches the fused/split bracket routes).
                            stagePresentEmptyValue(bufIdx, dt);
                        } else if (emitConvertedStageField(value.toString(), bufIdx, dt) == false) {
                            return false;
                        }
                    }
                    lastField = r >= len;
                    next = r + 1;
                } else {
                    // Unquoted field: scan to the next unescaped delimiter, noting whether it has an escape.
                    int j = i;
                    boolean hasEsc = false;
                    while (j < len) {
                        char c = buf[j];
                        if (escapeAware && c == esc) {
                            hasEsc = true;
                            j += 2; // skip the escaped char (even if it is a delimiter)
                            continue;
                        }
                        if (c == delim) {
                            break;
                        }
                        j++;
                    }
                    int fieldEnd = Math.min(j, len);
                    if (projected) {
                        if (hasEsc) {
                            if (emitUnquotedEscapedField(buf, i, fieldEnd, bufIdx, dt) == false) {
                                return false;
                            }
                        } else if (emitPlainField(buf, i, fieldEnd, bufIdx, dt) == false) {
                            return false;
                        }
                    } else {
                        // Non-projected field is still tokenized by Jackson, so it counts against the cap.
                        boolean within = hasEsc
                            ? checkUnprojectedEscapedFieldCap(buf, i, fieldEnd)
                            : checkUnprojectedFieldCap(buf, i, fieldEnd);
                        if (within == false) {
                            return false;
                        }
                    }
                    lastField = j >= len;
                    next = fieldEnd + 1;
                }

                fieldIndex++;
                if (lastField) {
                    break;
                }
                i = next;
            }

            int totalFields = fieldIndex;
            if (totalFields > rowWidthLimit) {
                onRowError(
                    "CSV row has [" + totalFields + "] columns but schema defines [" + schemaColumnCount + "] columns",
                    null,
                    directRawLine(),
                    true
                );
                return false;
            }
            for (int c = 0; c < columnCount; c++) {
                if (projectedIdx[c] < 0 || projectedIdx[c] >= totalFields) {
                    stageNullValue(c);
                }
            }
            return true;
        }

        /**
         * Converts an unquoted field that contains the escape character: decodes {@code \}-escapes via
         * {@link #decodeQuotedEscapeChar} (Jackson's quoted-escape semantics) into the reused buffer.
         * Whitespace trimming is gated on {@code trim_spaces} only (leading raw, then decoded-trailing),
         * mirroring the house peer {@link #emitUnquotedEscapedSplitField}; the typed trim is deferred to
         * {@code tryConvertValue}. Under the no-trim default this method trims nothing and IS the house
         * grammar (Jackson is not used). A trailing lone escape (no following char) is dropped.
         *
         * <p>When trimming (i.e. {@code trim_spaces}), the order raw-leading, decode, decoded-trailing
         * matches Jackson's: it skips raw leading whitespace before its escape-decode loop, then trims
         * trailing whitespace from the collected (already-decoded) value. Trimming raw trailing whitespace
         * before decoding would differ when the raw span ends with {@code \ }+whitespace, since the escape
         * decodes that pair to whitespace which is then removed, whereas a raw trim stops at {@code \} and
         * leaves a lone escape (dropped).
         */
        private boolean emitUnquotedEscapedField(char[] buf, int start, int end, int bufIdx, DataType dt) {
            // Only trim_spaces trims here (mirrors the house peer emitUnquotedEscapedSplitField); the typed
            // trim is deferred to tryConvertValue below, so a whitespace-bearing null_value survives to its
            // raw-marker check instead of being stripped first.
            final boolean trimSpaces = options.trimSpaces();
            if (trimSpaces) {
                while (start < end && buf[start] <= ' ') {
                    start++;
                }
            }
            if (start == end) {
                // Whitespace-only field: present-but-empty (empty string on string columns, null otherwise).
                stagePresentEmptyValue(bufIdx, dt);
                return true;
            }
            final char esc = options.escapeChar();
            StringBuilder value = resetQuotedBuf();
            for (int k = start; k < end; k++) {
                char c = buf[k];
                if (c == esc) {
                    if (k + 1 < end) {
                        value.append(decodeQuotedEscapeChar(buf[++k]));
                    }
                    // else: trailing lone escape, dropped
                } else {
                    value.append(c);
                }
            }
            // Trim trailing whitespace from the decoded value (matches Jackson's TRIM_SPACES: trim the
            // collected decoded chars, not the raw input) — gated the same way as the leading trim.
            int trimEnd = value.length();
            if (trimSpaces) {
                while (trimEnd > 0 && value.charAt(trimEnd - 1) <= ' ') {
                    trimEnd--;
                }
            }
            if (trimEnd == 0) {
                // Decoded to only whitespace: present-but-empty (empty string on string columns, null otherwise).
                stagePresentEmptyValue(bufIdx, dt);
                return true;
            }
            // Cap on the tokenized value (full decoded length under no-trim, trimmed under trim_spaces).
            if (trimEnd > maxFieldChars) {
                return rejectFieldTooLarge(trimEnd);
            }
            return emitConvertedStageField(trimEnd == value.length() ? value.toString() : value.substring(0, trimEnd), bufIdx, dt);
        }

        /**
         * Enforces the field-size cap for a non-projected unquoted field that contains escapes. Escapes
         * only shrink the value, so the raw span is an upper bound on the trimmed decoded length; the
         * decode walk is only paid when the raw span already exceeds the cap. The cap governs the
         * tokenized value — trimmed only under {@code trim_spaces} (raw-leading, decode, decoded-trailing),
         * matching {@link #emitUnquotedEscapedField} — so projection cannot change whether a row survives.
         *
         * @return {@code true} if within the cap, {@code false} if it was rejected (and the row dropped)
         */
        private boolean checkUnprojectedEscapedFieldCap(char[] buf, int start, int end) {
            if (end - start <= maxFieldChars) {
                return true;
            }
            // Whitespace only counts against the cap when trim_spaces is off (the raw bytes are stored).
            final boolean trim = options.trimSpaces();
            // Trim raw leading whitespace (matches Jackson's skip-leading-ws before the decode loop).
            if (trim) {
                while (start < end && buf[start] <= ' ') {
                    start++;
                }
            }
            if (start == end) {
                return true;
            }
            final char esc = options.escapeChar();
            // Count decoded chars, tracking the trailing whitespace run so we can trim after decoding
            // (matching Jackson's TRIM_SPACES order: trim the decoded value, not the raw input).
            int decodedLen = 0;
            int trailingWs = 0;
            for (int k = start; k < end; k++) {
                char decoded;
                if (buf[k] == esc) {
                    if (k + 1 < end) {
                        decoded = decodeQuotedEscapeChar(buf[++k]);
                    } else {
                        continue; // trailing lone escape, dropped
                    }
                } else {
                    decoded = buf[k];
                }
                decodedLen++;
                if (decoded <= ' ') {
                    trailingWs++;
                } else {
                    trailingWs = 0;
                }
            }
            int trimmedLen = decodedLen - (trim ? trailingWs : 0);
            if (trimmedLen > maxFieldChars) {
                return rejectFieldTooLarge(trimmedLen);
            }
            return true;
        }

        private StringBuilder resetQuotedBuf() {
            if (quotedBuf == null) {
                quotedBuf = new StringBuilder(64);
            } else {
                quotedBuf.setLength(0);
            }
            return quotedBuf;
        }

        /**
         * Fused field-splitting and typed conversion for the bracket-aware CSV path. Walks the
         * line character-by-character, maintaining the same quote/bracket/escape state machine as
         * {@link CsvFormatReader#splitCommaDelimiterBracketAwareFields}, but skips
         * {@link StringBuilder} accumulation for non-projected fields and inlines integer/long
         * parsing to avoid a second character walk via {@code Long.parseLong}.
         *
         * @return {@code true} if the row was accepted, {@code false} if it was rejected
         */
        private boolean splitAndConvertProjected(String line, int lineIdx) {
            // Emit the file-global byte offset captured at line-read time; the _rowPosition slot
            // maps to no source field, so the field walk below never overwrites it.
            if (rowPositionSlot >= 0) {
                rowBuffer[rowPositionSlot] = rowStartBytes[lineIdx];
            }
            final char delim = ',';
            final char quote = options.quoteChar();
            final char esc = options.escapeChar();

            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;
            int bracketDepth = 0;
            // Remember where the parser entered the unclosed state so error messages can anchor on
            // the actual fault site instead of head/tail-truncating a long line and hiding it.
            // Mirrors the offset tracking in splitCommaDelimiterBracketAwareFields.
            int quoteOpenAt = -1;
            int bracketOpenAt = -1;
            int fieldIndex = 0;
            boolean fieldHasNonWhitespace = false;
            boolean trailingFieldHasContent = false;

            boolean isProjected = fieldIndex < sourceIndexBound && projectedFieldSet.get(fieldIndex);
            int bufIdx = isProjected ? sourceToBufferIndex[fieldIndex] : -1;
            DataType dt = isProjected ? projectedTypes[bufIdx] : null;
            boolean tryNumeric = isProjected && (dt == DataType.INTEGER || dt == DataType.LONG);

            long numAcc = 0;
            boolean negative = false;
            boolean numericValid = tryNumeric;
            boolean numStarted = false;

            int i = 0;
            while (i < line.length()) {
                char c = line.charAt(i);

                if (inQuotes) {
                    trailingFieldHasContent = true;
                    if (c == quote) {
                        if (i + 1 < line.length() && line.charAt(i + 1) == quote) {
                            if (isProjected) current.append(quote);
                            numericValid = false;
                            i += 2;
                            continue;
                        }
                        inQuotes = false;
                        // Trailing whitespace between the closing quote and the delimiter/EOL is not part of
                        // the value (matches the direct quoted walker and splitCommaDelimiterBracketAwareFields);
                        // skip it so no-trim bracket mode agrees. Non-whitespace is still concatenated.
                        int j = i + 1;
                        while (j < line.length() && line.charAt(j) <= ' ') {
                            j++;
                        }
                        if (j == line.length() || line.charAt(j) == delim) {
                            i = j;
                            continue;
                        }
                    } else if (c == esc && i + 1 < line.length() && line.charAt(i + 1) == delim) {
                        if (isProjected) current.append(delim);
                        numericValid = false;
                        i += 2;
                        continue;
                    } else {
                        if (isProjected) current.append(c);
                        numericValid = false;
                    }
                    i++;
                } else if (bracketDepth > 0) {
                    trailingFieldHasContent = true;
                    if (isProjected) current.append(c);
                    numericValid = false;
                    if (c == '[') {
                        bracketDepth++;
                    } else if (c == ']') {
                        bracketDepth--;
                    }
                    i++;
                } else if (c == quote && fieldHasNonWhitespace == false) {
                    // Drop any whitespace-only prefix accumulated before the opening quote so ` "y"` yields
                    // `y`, matching the direct quoted walker and staying aligned with
                    // splitCommaDelimiterBracketAwareFields. Behavior-neutral under trim_spaces.
                    if (isProjected) {
                        current.setLength(0);
                    }
                    trailingFieldHasContent = true;
                    inQuotes = true;
                    quoteOpenAt = i;
                    numericValid = false;
                    i++;
                } else if (c == '[' && fieldHasNonWhitespace == false) {
                    trailingFieldHasContent = true;
                    if (hasMvcBracketClose(line, i)) {
                        bracketDepth = 1;
                        bracketOpenAt = i;
                    }
                    if (isProjected) current.append(c);
                    numericValid = false;
                    i++;
                } else if (c == delim) {
                    if (i > 0 && line.charAt(i - 1) == esc) {
                        trailingFieldHasContent = true;
                        if (isProjected) current.append(c);
                        numericValid = false;
                    } else {
                        if (isProjected) {
                            if (current.length() > 0) {
                                if (emitConvertedField(current, bufIdx, dt, numericValid, numAcc, negative, numStarted, line) == false) {
                                    return false;
                                }
                            } else {
                                // Present-but-empty field (a delimiter closed it): empty string on
                                // string columns, null otherwise.
                                rowBuffer[bufIdx] = presentEmptyValue(dt);
                            }
                            current.setLength(0);
                        }
                        fieldIndex++;
                        fieldHasNonWhitespace = false;
                        trailingFieldHasContent = false;
                        isProjected = fieldIndex < sourceIndexBound && projectedFieldSet.get(fieldIndex);
                        bufIdx = isProjected ? sourceToBufferIndex[fieldIndex] : -1;
                        dt = isProjected ? projectedTypes[bufIdx] : null;
                        tryNumeric = isProjected && (dt == DataType.INTEGER || dt == DataType.LONG);
                        numAcc = 0;
                        negative = false;
                        numericValid = tryNumeric;
                        numStarted = false;
                    }
                    i++;
                } else {
                    trailingFieldHasContent = true;
                    if (Character.isWhitespace(c) == false) {
                        fieldHasNonWhitespace = true;
                    }
                    if (isProjected) {
                        current.append(c);
                        if (numericValid) {
                            if (c >= '0' && c <= '9') {
                                long newAcc = numAcc * 10 + (c - '0');
                                if (numAcc != 0 && newAcc / 10 != numAcc) {
                                    numericValid = false;
                                } else {
                                    numAcc = newAcc;
                                    numStarted = true;
                                }
                            } else if (c == '-' && numStarted == false && negative == false) {
                                negative = true;
                            } else if (c <= ' ') {
                                if (numStarted) {
                                    numericValid = false;
                                }
                            } else {
                                numericValid = false;
                            }
                        }
                    }
                    i++;
                }
            }

            if (inQuotes) {
                throw MalformedRowException.unclosedQuotedField(line, quoteOpenAt);
            }
            if (bracketDepth > 0) {
                throw MalformedRowException.unclosedBracketCell(line, bracketOpenAt);
            }

            // An unquoted trailing empty field (a row-ending delimiter, e.g. `a,b,`) leaves
            // trailingFieldHasContent false. It is still a PRESENT empty field when it falls inside the
            // schema, so count it and fill it like any other present-empty field. Beyond the schema a
            // lone trailing delimiter on a full-width row is not an extra column and does not error.
            boolean presentTrailingEmpty = isPresentTrailingEmpty(fieldIndex, sourceIndexBound);
            int totalFields = (trailingFieldHasContent || presentTrailingEmpty) ? fieldIndex + 1 : fieldIndex;
            if (totalFields > rowWidthLimit) {
                onRowError(
                    "CSV row has [" + totalFields + "] columns but schema defines [" + schemaColumnCount + "] columns",
                    null,
                    line,
                    true
                );
                return false;
            }

            if (isProjected) {
                if (trailingFieldHasContent) {
                    if (current.length() > 0) {
                        if (emitConvertedField(current, bufIdx, dt, numericValid, numAcc, negative, numStarted, line) == false) {
                            return false;
                        }
                    } else {
                        // Present-but-empty trailing field with the content flag set (e.g. a quoted
                        // empty `,""`): empty string on string columns, null otherwise.
                        rowBuffer[bufIdx] = presentEmptyValue(dt);
                    }
                } else if (presentTrailingEmpty) {
                    rowBuffer[bufIdx] = presentEmptyValue(dt);
                }
            }

            for (int c = 0; c < columnCount; c++) {
                int si = projectedIdx[c];
                if (si >= totalFields) {
                    rowBuffer[c] = null;
                }
            }

            return true;
        }

        /**
         * Emits a converted field value into {@link #rowBuffer}. For INTEGER/LONG fields that
         * were successfully parsed inline (all digits, no overflow), the numeric value is used
         * directly; otherwise falls back to the standard string conversion path.
         *
         * @param rawLine the raw CSV line, kept for error reporting
         * @return {@code true} if the field was accepted, {@code false} if a row-level error
         *         was raised (SKIP_ROW / FAIL_FAST)
         */
        private boolean emitConvertedField(
            StringBuilder current,
            int bufIdx,
            DataType dt,
            boolean numericValid,
            long numAcc,
            boolean negative,
            boolean numStarted,
            String rawLine
        ) {
            if (numericValid && numStarted) {
                long val = negative ? -numAcc : numAcc;
                if (dt == DataType.INTEGER) {
                    if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                        rowBuffer[bufIdx] = (int) val;
                        return true;
                    }
                } else {
                    rowBuffer[bufIdx] = val;
                    return true;
                }
            }
            return emitConvertedStringField(emitField(current, options.trimSpaces()), bufIdx, dt, rawLine);
        }

        /**
         * Converts a string field value (trimmed only when {@code trim_spaces} is set) and stores it in
         * {@link #rowBuffer}, routing parse errors through the error policy.
         */
        private boolean emitConvertedStringField(String value, int bufIdx, DataType dt, String rawLine) {
            Object result = tryConvertValue(value, dt, bufIdx);
            if (lastFieldError != null) {
                if (modeOrdinal == ErrorPolicy.Mode.NULL_FIELD.ordinal()) {
                    rowBuffer[bufIdx] = null;
                    onFieldError(lastFieldError, value, projectedAttrs[bufIdx]);
                    lastFieldError = null;
                    return true;
                } else {
                    String err = lastFieldError;
                    lastFieldError = null;
                    onRowError(err, null, rawLine, false);
                    return false;
                }
            }
            rowBuffer[bufIdx] = result;
            return true;
        }

        private Object tryConvertValue(String value, DataType dataType, int columnIndex) {
            if (value == null) {
                // A field the parser already resolved to null: a missing field (row shorter than the
                // schema), or a Jackson-emitted null (custom null_value token / escaped \N). Null on every type.
                return null;
            }
            if (hasCustomNullValue && value.equals(nullValueStr)) {
                return null;
            }
            if (value.isEmpty()) {
                // Present-but-empty cell: empty string on string columns, null otherwise.
                return presentEmptyValue(dataType);
            }
            if (DataType.isString(dataType) == false && value.equalsIgnoreCase("null")) {
                // The literal "null" (any case) is a null marker only for non-string columns; KEYWORD/TEXT
                // hold the string "null" verbatim.
                return null;
            }
            if (bracketMultiValues) {
                // Bracket syntax is structural: a padded " [1,2] " is the same multi-value cell, so the
                // detection probe is whitespace-insensitive regardless of the trim_spaces setting.
                String probe = value.trim();
                if (probe.startsWith("[") && probe.endsWith("]")) {
                    return tryConvertMultiValue(probe, dataType, columnIndex);
                }
            }
            if (DataType.isString(dataType) == false) {
                // Typed parses mirror CsvSchemaInferrer, which trims before type detection: a value the
                // sampler classified as INTEGER (etc.) must convert as that type regardless of surrounding
                // whitespace or quoting. A now-empty or "null" cell is null (as the sampler treats it —
                // null/empty are compatible with every type); a padded custom null_value is null too, to
                // match the top-of-method sentinel check. KEYWORD/TEXT keep their bytes verbatim — that
                // fidelity is the trim_spaces axis, not this one.
                value = value.trim();
                if (value.isEmpty() || value.equalsIgnoreCase("null") || (hasCustomNullValue && value.equals(nullValueStr))) {
                    return null;
                }
            }
            return switch (dataType) {
                case INTEGER -> tryParseInt(value);
                case LONG -> tryParseLong(value);
                case UNSIGNED_LONG -> tryParseUnsignedLong(value);
                case DOUBLE -> tryParseDouble(value);
                case KEYWORD, TEXT -> new BytesRef(value);
                case BOOLEAN -> tryParseBoolean(value);
                case DATETIME -> tryParseDatetime(value, columnIndex);
                case DATE_NANOS -> tryParseDateNanos(value, columnIndex);
                case IP -> tryParseIp(value);
                case VERSION -> tryParseVersion(value);
                case NULL -> null;
                default -> {
                    lastFieldError = "Unsupported data type: " + dataType;
                    yield null;
                }
            };
        }

        private Object tryConvertMultiValue(String value, DataType dataType, int columnIndex) {
            // Element extraction honors trim_spaces exactly like the scalar path (emitField): with trimming
            // off, per-element whitespace in a string multi-value ([ a , b ] on a keyword column) is kept.
            String inner = value.substring(1, value.length() - 1);
            String content = options.trimSpaces() ? inner.trim() : inner;
            if (content.isEmpty()) {
                return null;
            }
            List<String> parts = splitBracketContent(content);
            List<Object> result = new ArrayList<>(parts.size());
            for (String part : parts) {
                Object elem = parseElement(part, dataType, columnIndex);
                if (lastFieldError != null) {
                    return null;
                }
                if (elem != null) {
                    result.add(elem);
                }
            }
            return result.isEmpty() ? null : result;
        }

        private List<String> splitBracketContent(String content) {
            List<String> result = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            char esc = options.escapeChar();
            char quote = options.quoteChar();
            boolean trimSpaces = options.trimSpaces();
            boolean inQuotes = false;
            int i = 0;
            while (i < content.length()) {
                char c = content.charAt(i);
                if (c == quote) {
                    if (inQuotes) {
                        if (i + 1 < content.length() && content.charAt(i + 1) == quote) {
                            current.append(quote);
                            i += 2;
                            continue;
                        }
                        inQuotes = false;
                    } else {
                        inQuotes = true;
                    }
                    i++;
                } else if (c == ',' && inQuotes == false) {
                    result.add(trimSpaces ? current.toString().trim() : current.toString());
                    current = new StringBuilder();
                    i++;
                } else if (c == esc && inQuotes == false && i + 1 < content.length() && content.charAt(i + 1) == ',') {
                    current.append(',');
                    i += 2;
                } else {
                    current.append(c);
                    i++;
                }
            }
            result.add(trimSpaces ? current.toString().trim() : current.toString());
            return result;
        }

        private Object parseElement(String value, DataType dataType, int columnIndex) {
            if (value == null) {
                return null;
            }
            if (hasCustomNullValue && value.equals(nullValueStr)) {
                return null;
            }
            if (value.isEmpty()) {
                // Present-but-empty element (e.g. the middle of [a,,c]): same per-type rule as a
                // scalar present-empty cell — empty string on string columns, null otherwise.
                return presentEmptyValue(dataType);
            }
            if (DataType.isString(dataType) == false && value.equalsIgnoreCase("null")) {
                // Same string-type gate as tryConvertValue: a bracket element that is "null" stays the
                // literal string on KEYWORD/TEXT.
                return null;
            }
            value = unquoteElement(value);
            if (value.isEmpty()) {
                // Present-but-empty quoted element (e.g. [a,"",c]).
                return presentEmptyValue(dataType);
            }
            if (DataType.isString(dataType) == false) {
                // Same typed-parse leniency as tryConvertValue: a quoted, padded numeric element (e.g.
                // [" 5 ", 6]) converts by its inferred/declared type; padded null-sentinels become null.
                value = value.trim();
                if (value.isEmpty() || value.equalsIgnoreCase("null") || (hasCustomNullValue && value.equals(nullValueStr))) {
                    return null;
                }
            }
            return switch (dataType) {
                case INTEGER -> tryParseInt(value);
                case LONG -> tryParseLong(value);
                case UNSIGNED_LONG -> tryParseUnsignedLong(value);
                case DOUBLE -> tryParseDouble(value);
                case KEYWORD, TEXT -> new BytesRef(value);
                case BOOLEAN -> tryParseBoolean(value);
                case DATETIME -> tryParseDatetime(value, columnIndex);
                case DATE_NANOS -> tryParseDateNanos(value, columnIndex);
                case IP -> tryParseIp(value);
                case VERSION -> tryParseVersion(value);
                case NULL -> null;
                default -> {
                    lastFieldError = "Unsupported data type: " + dataType;
                    yield null;
                }
            };
        }

        /**
         * Unquotes an element that is wrapped in the configured quote character.
         * Removes leading/trailing quotes and replaces {@code ""} with {@code "} in the inner content.
         */
        private String unquoteElement(String value) {
            char quote = options.quoteChar();
            if (value.length() >= 2 && value.charAt(0) == quote && value.charAt(value.length() - 1) == quote) {
                String inner = value.substring(1, value.length() - 1);
                return inner.replace(String.valueOf(quote) + quote, String.valueOf(quote));
            }
            return value;
        }

        // The numeric parsers reuse the ES|QL :: cast engine (EsqlDataTypeConverter.stringToInt/
        // stringToLong/stringToDouble) so a declared CSV read is value-identical to ::integer/::long/
        // ::double and to the columnar readers: a fractional/scientific token parses and ROUNDS for the
        // whole-number targets (e.g. "1.9" -> 2), where the former Integer/Long.parseLong rejected it as
        // a policy error. The direct-block fast path still parses clean integers straight from the char
        // buffer and only falls back here for non-integer tokens, so the hot path is unchanged. A failure
        // stays a per-field policy error (lastFieldError + null), counted against the read's error budget.
        private Object tryParseInt(String value) {
            try {
                return EsqlDataTypeConverter.stringToInt(value);
            } catch (NumberFormatException | InvalidArgumentException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [INTEGER]";
                return null;
            }
        }

        private Object tryParseLong(String value) {
            try {
                return EsqlDataTypeConverter.stringToLong(value);
            } catch (NumberFormatException | InvalidArgumentException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [LONG]";
                return null;
            }
        }

        private Object tryParseDouble(String value) {
            // Double is NOT routed through the :: engine: it has no truncate-vs-round divergence, and
            // stringToDouble (StringUtils.parseDouble) rejects NaN/Infinity that the direct-block fast path
            // (NumberInput.parseDouble) accepts - routing here would break the direct-block parity contract.
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [DOUBLE]";
                return null;
            }
        }

        private Object tryParseUnsignedLong(String value) {
            try {
                // Delegate to the single unsigned_long authority (DeclaredTypeCoercions.coerceToUnsignedLong), the
                // same scalar the Parquet and ORC declared-read paths use. It parses, range-checks [0, 2^64-1] and
                // returns the sign-flip block encoding, so a declared unsigned_long reads bit-identically whatever
                // the file format. Fractional and scientific tokens truncate toward zero, matching ::unsigned_long
                // and deliberately unlike the round-half behavior of the long/integer coercers.
                return DeclaredTypeCoercions.coerceToUnsignedLong(value);
            } catch (IllegalArgumentException e) {
                // coerceToUnsignedLong signals every bad token with an IllegalArgumentException (its range guard, the
                // ArithmeticException remap, and the NumberFormatException subclass from BigDecimal); unlike
                // strictParseBoolean it never throws InvalidArgumentException, so one catch clause covers it.
                lastFieldError = "Failed to parse CSV value [" + value + "] as [UNSIGNED_LONG]";
                return null;
            }
        }

        private Object tryParseBoolean(String value) {
            try {
                // Delegate to the single strict-boolean authority (DeclaredTypeCoercions.strictParseBoolean)
                // so the CSV declared-boolean rule cannot drift from the columnar and NDJSON readers - they
                // all accept exactly true/false case-insensitively and reject everything else.
                return DeclaredTypeCoercions.strictParseBoolean(value);
            } catch (IllegalArgumentException | InvalidArgumentException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [BOOLEAN]";
                return null;
            }
        }

        private Object tryParseDatetime(String value, int columnIndex) {
            // A column with a declared `format` parses strictly with that ES DateFormatter, overriding the numeric
            // epoch shortcut, the file-level datetime_format, and the ISO fast path — for THIS column only. The parse
            // goes through the shared DeclaredTypeCoercions.parseDatetimeMillis, the SAME string->datetime conversion
            // the columnar readers use for their string->date coercion, so identical bytes + declared format produce
            // the identical instant regardless of source format. Other columns keep today's behavior.
            if (columnIndex >= 0
                && declaredFormatters != null
                && columnIndex < declaredFormatters.length
                && declaredFormatters[columnIndex] != null) {
                try {
                    return DeclaredTypeCoercions.parseDatetimeMillis(value, declaredFormatters[columnIndex]);
                } catch (Exception e) {
                    lastFieldError = "Failed to parse CSV datetime value [" + value + "]";
                    return null;
                }
            }
            // The file-level datetime_format runs the same parse as the declared branch above, with the file-wide
            // formatter. It outranks the numeric-epoch shortcut whenever it actually matches the cell, so an
            // all-digit pattern (yyyyMMdd, basic_date, epoch_second) can win; tryParse asks that question without
            // paying an exception on a miss. A numeric cell the pattern does NOT match stays epoch millis, which
            // keeps an epoch column readable in a file whose other datetime columns use the pattern -- CSV's
            // stand-in for the JSON number token that bypasses NDJSON's string formatter. Catch Exception like the
            // declared branch: a single bad cell nulls out under the error policy, it never aborts the batch.
            if (datetimeFormatter != null) {
                if (looksNumeric(value) == false || datetimeFormatter.tryParse(value) != null) {
                    try {
                        return DeclaredTypeCoercions.parseDatetimeMillis(value, datetimeFormatter);
                    } catch (Exception e) {
                        lastFieldError = "Failed to parse CSV datetime value [" + value + "]";
                        return null;
                    }
                }
                Long epoch = parseEpoch(value);
                if (epoch == null) {
                    lastFieldError = "Failed to parse CSV datetime value [" + value + "]";
                }
                return epoch;
            }
            if (looksNumeric(value)) {
                Long epoch = parseEpoch(value);
                if (epoch != null) {
                    return epoch;
                }
                // Overflowed a long; fall through to the ISO stages, which will report the failure.
            }
            // Stage 1: ES's hand-rolled ISO-8601 parser (T-separator, date-only, zones, fractions)
            // avoids the DateTimeFormatter Parsed-HashMap allocation that dominates DateUtils.asDateTime.
            // tryParse returns null on mismatch so we don't pay an exception per missed input.
            // Iso8601Parser only checks loose bounds (month <= 12, day <= 31) and defers month-length
            // validation to LocalDate.of(...) inside DateFormatters.from(...). That call can throw two
            // distinct unchecked exceptions: a DateTimeException for calendar-invalid inputs like
            // 2021-02-30T10:00:00, and an IllegalArgumentException for the fallthrough branch in
            // DateFormatters.from when the parsed accessor cannot be converted to a zoned date-time.
            // Catch both and fall through so the slow Stage 3 path handles the input instead of
            // propagating an uncaught exception and aborting the batch.
            TemporalAccessor parsed = ISO_DATETIME_FAST_FORMATTER.tryParse(value);
            if (parsed != null) {
                try {
                    return DateFormatters.from(parsed).toInstant().toEpochMilli();
                } catch (DateTimeException | IllegalArgumentException e) {
                    // fall through to Stage 2 / 3
                }
            }
            // Stage 2: hot path for `YYYY-MM-DD HH:MM:SS[.fff]` (space-separated, no zone), which the
            // ISO parser does not accept. Returns FAST_PATH_MISS on any mismatch.
            long spaceMillis = tryParseSpaceSeparatedDatetimeMillis(value);
            if (spaceMillis != FAST_PATH_MISS) {
                return spaceMillis;
            }
            // Stage 3: full DateUtils.asDateTime fallback for the long tail (space-separated with zone,
            // lowercase `t`, non-millisecond fractional precision, etc.). Same exception-based path as
            // before — only reached when neither fast path matched.
            try {
                return DateUtils.asDateTime(value).toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                lastFieldError = "Failed to parse CSV datetime value [" + value + "]";
                return null;
            }
        }

        private Object tryParseIp(String value) {
            try {
                return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(value)));
            } catch (IllegalArgumentException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [IP]";
                return null;
            }
        }

        private Object tryParseDateNanos(String value, int columnIndex) {
            // Mirrors tryParseDatetime exactly, one rail down: a column with a declared `format` parses strictly with
            // that ES DateFormatter, overriding the epoch shortcut and the file-level datetime_format, for THIS column
            // only. Every rail goes through EsqlDataTypeConverter.dateNanosToLong — the SAME string -> date_nanos
            // conversion the columnar declared coercion (DeclaredTypeCoercions.scalarCoercer, which threads the
            // declared format) and the NDJSON decode arm use — so identical bytes with an identical declared format
            // yield the same instant across every format. A bare numeric cell is epoch NANOS: the declared type names
            // the numeric unit (datetime = millis, date_nanos = nanos; see DeclaredTypeCoercions).
            if (columnIndex >= 0
                && declaredFormatters != null
                && columnIndex < declaredFormatters.length
                && declaredFormatters[columnIndex] != null) {
                try {
                    return EsqlDataTypeConverter.dateNanosToLong(value, declaredFormatters[columnIndex]);
                } catch (Exception e) {
                    lastFieldError = "Failed to parse CSV date_nanos value [" + value + "]";
                    return null;
                }
            }
            // See tryParseDatetime: the file-level pattern outranks the epoch shortcut when it matches the cell, and a
            // numeric cell it does not match stays epoch nanos.
            if (datetimeFormatter != null) {
                if (looksNumeric(value) == false || datetimeFormatter.tryParse(value) != null) {
                    try {
                        return EsqlDataTypeConverter.dateNanosToLong(value, datetimeFormatter);
                    } catch (Exception e) {
                        lastFieldError = "Failed to parse CSV date_nanos value [" + value + "]";
                        return null;
                    }
                }
                Long epoch = parseEpoch(value);
                if (epoch == null || epoch < 0) {
                    // A negative epoch has no date_nanos representation (the TO_DATE_NANOS range rule), so it fails the
                    // cell through the error policy rather than ever emitting a negative nanos long.
                    lastFieldError = "Failed to parse CSV date_nanos value [" + value + "]";
                    return null;
                }
                return epoch;
            }
            if (looksNumeric(value)) {
                Long epoch = parseEpoch(value);
                if (epoch != null && epoch >= 0) {
                    return epoch;
                }
                if (epoch != null) {
                    // A negative epoch is not a representable date_nanos; fail the cell rather than emit it.
                    lastFieldError = "Failed to parse CSV date_nanos value [" + value + "]";
                    return null;
                }
                // Overflowed a long; fall through to the ISO fallback, which will report the failure.
            }
            try {
                return EsqlDataTypeConverter.dateNanosToLong(value);
            } catch (IllegalArgumentException e) {
                lastFieldError = "Failed to parse CSV date_nanos value [" + value + "]";
                return null;
            }
        }

        private Object tryParseVersion(String value) {
            try {
                return EsqlDataTypeConverter.stringToVersion(value);
            } catch (IllegalArgumentException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [VERSION]";
                return null;
            }
        }

        /**
         * Single point of truth for what happens to a row that failed structural parsing or
         * row-shape validation.
         *
         * @param structural {@code true} for tokeniser errors (Jackson, bracket parser) and
         *                   row-shape mismatches (column count) where {@code skip_row} is the
         *                   natural escape hatch; {@code false} for field-type errors where
         *                   {@code null_field} is the better suggestion. Only affects the hint
         *                   appended to the FAIL_FAST {@link ParsingException}.
         */
        private void onRowError(String message, Exception cause, String[] row, boolean structural) {
            onRowErrorImpl(message, cause, CsvErrorMessages.summarizeRow(row), structural);
        }

        /**
         * Overload for the fused bracket-aware path where the row has not been split into a
         * {@code String[]}. Uses the raw CSV line for the error excerpt instead.
         */
        private void onRowError(String message, Exception cause, String rawLine, boolean structural) {
            onRowErrorImpl(message, cause, CsvErrorMessages.summarize(rawLine), structural);
        }

        private void onRowErrorImpl(String message, Exception cause, String rowExcerpt, boolean structural) {
            if (modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                String hint = structural
                    ? "; set error_mode=skip_row (or null_field) to skip and warn instead of failing"
                    : "; set error_mode=null_field to null-fill the bad field instead of failing";
                throw new ParsingException(
                    cause,
                    Source.EMPTY,
                    "{}",
                    "CSV parse error at row [" + totalRowCount + "]: " + message + "; row: " + rowExcerpt + hint
                );
            }
            errorCount++;
            skipWarnings.add("Row [" + totalRowCount + "] error: " + message);
            if (logErrors) {
                logger.warn(
                    "Skipping malformed CSV row [{}] (error {}/{}): {}",
                    totalRowCount,
                    errorCount,
                    errorPolicy.maxErrors(),
                    message
                );
            }
            checkBudget(message, cause);
        }

        private void onFieldError(String message, String value, Attribute attr) {
            errorCount++;
            String summarizedValue = CsvErrorMessages.summarize(value);
            skipWarnings.add("Row [" + totalRowCount + "] field [" + attr.name() + "] value [" + summarizedValue + "]: " + message);
            if (logErrors) {
                logger.warn(
                    "Null-filling unparseable field [{}] value [{}] in row [{}] (error {}/{}): {}",
                    attr.name(),
                    summarizedValue,
                    totalRowCount,
                    errorCount,
                    errorPolicy.maxErrors(),
                    message
                );
            }
            checkBudget(message, null);
        }

        private void checkBudget(String message, Exception cause) {
            if (errorPolicy.isBudgetExceeded(errorCount, totalRowCount)) {
                // Surface the budget-exceeded condition as a warning too so clients see which row tripped it
                // even when the request itself fails.
                skipWarnings.add(
                    "CSV error budget exceeded at row ["
                        + totalRowCount
                        + "]: ["
                        + errorCount
                        + "] errors, maximum ["
                        + errorPolicy.maxErrors()
                        + "] or ratio ["
                        + errorPolicy.maxErrorRatio()
                        + "]"
                );
                // Budget exceeded is a client-data problem (the file has too many bad rows for the
                // user-configured tolerance), not a server bug — surface as HTTP 400.
                throw new ParsingException(
                    cause,
                    Source.EMPTY,
                    "CSV error budget exceeded: [{}] errors in [{}] rows, maximum allowed is [{}] errors or [{}] ratio",
                    errorCount,
                    totalRowCount,
                    errorPolicy.maxErrors(),
                    errorPolicy.maxErrorRatio()
                );
            }
        }

        /**
         * Reads a {@link #looksNumeric} cell as a raw epoch value. Returns {@code null} when the digits overflow a
         * {@code long}; each caller decides what that means. Under a file-level format it is a hard field error —
         * nothing else will parse the cell. Without one it falls through to the ISO stages, which report the failure.
         */
        private static Long parseEpoch(String value) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private static boolean looksNumeric(String value) {
            if (value == null || value.isEmpty()) {
                return false;
            }
            int start = (value.charAt(0) == '-') ? 1 : 0;
            if (start >= value.length()) {
                return false;
            }
            for (int i = start; i < value.length(); i++) {
                if (value.charAt(i) < '0' || value.charAt(i) > '9') {
                    return false;
                }
            }
            return true;
        }
    }
}
