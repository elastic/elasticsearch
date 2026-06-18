/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.datasources.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.TextAggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.cache.ColumnStatsAccumulator;
import org.elasticsearch.xpack.esql.datasources.cache.CountingInputStream;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.cache.TextFormatStats;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.BufferingPageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
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
import java.util.regex.Pattern;

/**
 * CSV/TSV format reader for external datasources.
 *
 * <h2>File format</h2>
 * <ul>
 *   <li>First non-comment line: schema — {@code column:type} pairs separated by the delimiter
 *   <li>Subsequent lines: data rows
 *   <li>Empty/missing values → {@code null}
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
 *   <tr><td>{@code quote}</td><td>{@code "}</td><td>Quoting character</td></tr>
 *   <tr><td>{@code escape}</td><td>{@code \}</td><td>Escape character inside quoted fields</td></tr>
 *   <tr><td>{@code comment}</td><td>{@code //}</td><td>Line comment prefix</td></tr>
 *   <tr><td>{@code null_value}</td><td>(empty)</td><td>String representation of null</td></tr>
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
 * </table>
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
 * {@snippet lang="esql" :
 *   EXTERNAL "s3://bucket/data.tsv" WITH {"delimiter": "\t", "error_mode": "skip_row", "max_errors": 100}
 * }
 * {@snippet lang="esql" :
 *   EXTERNAL "s3://bucket/employees.csv" WITH {"multi_value_syntax": "brackets"}
 * }
 * {@snippet lang="esql" :
 *   EXTERNAL "s3://bucket/data.csv" WITH {"multi_value_syntax": "brackets", "error_mode": "skip_row"}
 * }
 * {@snippet lang="esql" :
 *   EXTERNAL "https://datasets.example.com/headerless.csv.gz" WITH {"header_row": false}
 * }
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
    static final String CONFIG_SCHEMA_SAMPLE_SIZE = "schema_sample_size";

    /** Keys recognised by {@link #withConfigTrackingConsumedKeys(Map)}. */
    static final Set<String> RECOGNIZED_KEYS = Set.of(
        CONFIG_DELIMITER,
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
     * so a user request like {@code WITH {"error_mode": "skip_row"}} also applies to schema
     * sampling — matching ClickHouse's {@code input_format_allow_errors_*} semantics.
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

    public CsvFormatReader(BlockFactory blockFactory) {
        this(
            blockFactory,
            CsvFormatOptions.DEFAULT,
            "csv",
            List.of(".csv", ".tsv"),
            null,
            CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE,
            ErrorPolicy.STRICT,
            ""
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
            ""
        );
    }

    public CsvFormatReader(BlockFactory blockFactory, CsvFormatOptions options, String format, List<String> extensions) {
        this(blockFactory, options, format, extensions, null, CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE, ErrorPolicy.STRICT, "");
    }

    private CsvFormatReader(
        BlockFactory blockFactory,
        CsvFormatOptions options,
        String format,
        List<String> extensions,
        List<Attribute> resolvedSchema,
        int schemaSampleSize,
        ErrorPolicy effectivePolicy,
        String canonicalConfig
    ) {
        this.blockFactory = blockFactory;
        this.options = options;
        this.format = format;
        this.extensions = extensions;
        this.resolvedSchema = resolvedSchema;
        this.schemaSampleSize = schemaSampleSize;
        this.effectivePolicy = effectivePolicy;
        this.canonicalConfig = canonicalConfig;
        this.counters = new CsvReaderCounters(format);
        this.sharedCsvMapper = createMapper(options);
    }

    private static CsvMapper createMapper(CsvFormatOptions opts) {
        CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.TRIM_SPACES);
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
        char delimiter = parseChar(config.get(CONFIG_DELIMITER), baseline.delimiter());
        char quoteChar = parseChar(config.get(CONFIG_QUOTE), baseline.quoteChar());
        char escapeChar = parseChar(config.get(CONFIG_ESCAPE), baseline.escapeChar());
        String commentPrefix = parseString(config.get(CONFIG_COMMENT), baseline.commentPrefix());
        String nullValue = parseString(config.get(CONFIG_NULL_VALUE), baseline.nullValue());
        Charset encoding = parseEncoding(config.get(CONFIG_ENCODING), baseline.encoding());
        DateTimeFormatter datetimeFormatter = parseDatetimeFormat(config.get(CONFIG_DATETIME_FORMAT), baseline.datetimeFormatter());
        int maxFieldSize = parseInt(config.get(CONFIG_MAX_FIELD_SIZE), baseline.maxFieldSize());
        CsvFormatOptions.MultiValueSyntax multiValueSyntax = parseMultiValueSyntax(
            config.get(CONFIG_MULTI_VALUE_SYNTAX),
            baseline.multiValueSyntax()
        );
        boolean headerRow = parseBooleanOption(CONFIG_HEADER_ROW, config.get(CONFIG_HEADER_ROW), baseline.headerRow());
        String columnPrefix = parseString(config.get(CONFIG_COLUMN_PREFIX), baseline.columnPrefix());

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
            columnPrefix
        );
        return merged.equals(baseline) ? null : merged;
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

    private static DateTimeFormatter parseDatetimeFormat(Object value, DateTimeFormatter baseline) {
        if (value == null || value.toString().isEmpty()) {
            return baseline;
        }
        try {
            return DateTimeFormatter.ofPattern(value.toString(), Locale.ROOT);
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
            canonicalConfig
        );
    }

    @Override
    public CsvFormatReader withSchema(List<Attribute> schema) {
        return new CsvFormatReader(blockFactory, options, format, extensions, schema, schemaSampleSize, effectivePolicy, canonicalConfig);
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
            canon
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
                options.encoding()
            );
            if (options.headerRow() == false) {
                return inferSchemaWithSyntheticNames(recordReader);
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
            List<Attribute> inferred = inferSchemaFromSample(headerLine, recordReader);
            checkUniqueAttributeNames(inferred);
            return inferred;
        }
    }

    private List<Attribute> inferSchemaFromSample(String headerLine, CsvLogicalRecordReader recordReader) throws IOException {
        String[] columnNames = splitFieldsForOptions(headerLine, options);
        Iterator<List<?>> csvIterator = newCsvIterator(recordReader);
        CircuitBreaker breaker = blockFactory.breaker();
        SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, breaker, effectivePolicy);
        try {
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows());
        } finally {
            breaker.addWithoutBreaking(-sample.reservedBytes());
        }
    }

    private List<Attribute> inferSchemaWithSyntheticNames(CsvLogicalRecordReader recordReader) throws IOException {
        Iterator<List<?>> csvIterator = newCsvIterator(recordReader);
        CircuitBreaker breaker = blockFactory.breaker();
        SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, breaker, effectivePolicy);
        try {
            if (sample.rows().isEmpty()) {
                throw new IOException("CSV file has no data rows");
            }
            return inferSyntheticSchema(sample.rows(), options.columnPrefix());
        } finally {
            breaker.addWithoutBreaking(-sample.reservedBytes());
        }
    }

    /**
     * Build a schema for a headerless CSV: count the widest sample row, synthesize names from
     * {@code prefix}, and run type inference. Pure on its inputs — does not touch the circuit
     * breaker. Both call sites must guarantee {@code sampleRows} is non-empty (and surface the
     * user-facing "CSV file has no data rows" {@link IOException} themselves); the assertion is
     * just a programmer-error guard.
     */
    static List<Attribute> inferSyntheticSchema(List<String[]> sampleRows, String prefix) {
        assert sampleRows.isEmpty() == false : "sampleRows must be non-empty for synthetic schema inference";
        int columnCount = 0;
        for (String[] row : sampleRows) {
            if (row.length > columnCount) {
                columnCount = row.length;
            }
        }
        String[] columnNames = synthesizeColumnNames(columnCount, prefix);
        return CsvSchemaInferrer.inferSchema(columnNames, sampleRows);
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
                "CSV header has duplicate column names {}; if the file has no header row, "
                    + "set [\"header_row\": false] in the WITH options",
                rendered.toString()
            );
        }
    }

    /**
     * Per-record iterator used for schema reading, sampling, and bootstrap paths where each record is
     * materialized into a {@link String} before Jackson parses it. Cost is bounded by
     * {@link #schemaSampleSize} on inference paths; the bulk read loop swaps over to
     * {@link #newJacksonBulkIterator} after schema resolution so the per-row hot path stays on
     * Jackson's direct bulk char-buffer tokenization.
     */
    private Iterator<List<?>> newCsvIterator(CsvLogicalRecordReader recordReader) throws IOException {
        return new CsvRecordIterator(recordReader, newCsvSchema());
    }

    /**
     * Bulk-path iterator: hands the raw {@link Reader} straight to Jackson's {@link CsvParser} so the
     * per-row hot loop tokenizes characters in Jackson's internal char buffer instead of re-materializing
     * each logical record into a {@link StringBuilder} for a follow-up Jackson parse. The byte-level
     * {@code max_record_size} cap is enforced upstream by {@link CsvRecordCappingInputStream}, so this
     * path no longer needs the per-char accounting that {@link CsvLogicalRecordReader#readRecord} added.
     * Used after schema resolution / sampling, where every subsequent record flows through this iterator.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Iterator<List<?>> newJacksonBulkIterator(Reader reader) throws IOException {
        return (Iterator) sharedCsvMapper.readerFor(List.class).with(newCsvSchema()).readValues(reader);
    }

    private CsvSchema newCsvSchema() {
        return CsvSchema.emptySchema()
            .withColumnSeparator(options.delimiter())
            .withQuoteChar(options.quoteChar())
            .withEscapeChar(options.escapeChar())
            .withNullValue(options.nullValue());
    }

    /**
     * Holds sample rows collected for schema inference together with the number of bytes
     * reserved on the circuit breaker. Callers must release {@link #reservedBytes} via
     * {@link CircuitBreaker#addWithoutBreaking(long)} when the rows are no longer needed.
     */
    record SchemaSample(List<String[]> rows, long reservedBytes) {}

    /** Hard cap on consecutive parse failures during schema sampling, applied INDEPENDENTLY of
     *  the user's {@link ErrorPolicy}. Jackson's stream-based CSV parser cannot guarantee
     *  resync after a malformed record (the tokeniser may have consumed bytes mid-field), so
     *  even a generous {@code max_errors} budget cannot make progress past a permanently
     *  confused parser state. ClickHouse / DuckDB don't need this guard because their C++
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
     * <p>Aligning sampling with the runtime policy matches ClickHouse's
     * {@code input_format_allow_errors_*} semantics (one budget covering both phases) and
     * means a {@code WITH {"error_mode": "skip_row"}} request is honoured at planning time
     * too — not just once data starts flowing.
     */
    static SchemaSample collectSampleRows(
        Iterator<List<?>> csvIterator,
        String commentPrefix,
        int sampleSize,
        CircuitBreaker breaker,
        ErrorPolicy policy
    ) {
        List<String[]> sampleRows = new ArrayList<>();
        long reservedBytes = 0;
        boolean success = false;
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
                    sampleRows.add(row);
                    consecutiveFailures = 0;
                } catch (RuntimeException e) {
                    totalRowCount++;
                    errorCount++;
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
            return new SchemaSample(sampleRows, reservedBytes);
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
                + "; set error_mode to skip_row (or null_field) in WITH options to skip and warn instead of failing"
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
        InputStream capped = useBracketAware ? stream : new CsvRecordCappingInputStream(stream, context.maxRecordBytes());
        BufferedReader reader = new BufferedReader(new InputStreamReader(capped, options.encoding()), READER_BUFFER_SIZE);
        CsvLogicalRecordReader recordReader = new CsvLogicalRecordReader(
            reader,
            options.quoteChar(),
            options.delimiter(),
            context.maxRecordBytes(),
            options.encoding()
        );
        // Falls back to effectivePolicy (resolved from WITH options in withConfig) so a user
        // request like WITH {"error_mode": "skip_row"} also applies to the data path when no
        // upstream caller has built a FormatReadContext with an explicit policy. The planner
        // path always sets context.errorPolicy() explicitly.
        ErrorPolicy effective = context.errorPolicy() != null ? context.errorPolicy() : effectivePolicy;
        List<Attribute> effectiveSchema;
        List<Attribute> readSchema = context.readSchema();
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
                skipHeaderLine(recordReader);
            }
            effectiveSchema = readSchema;
        } else if (context.firstSplit()) {
            // resolvedSchema from withSchema(...) is the projected output, not the file's column
            // layout — using it as positional schema would mis-align columns. Only trust it when
            // recordAligned=true (streaming-parallel pre-bound the FULL file schema from chunk 0).
            if (context.recordAligned() && resolvedSchema != null) {
                if (options.headerRow()) {
                    skipHeaderLine(recordReader);
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
            effective,
            object.path().toString(),
            cacheable ? object : null,
            cacheable ? stream : null,
            pinnedMtimeMillis,
            chunkMode,
            counters
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
    public void close() throws IOException {}

    /**
     * Consumes one header line from {@code reader}, skipping over leading empty lines and
     * comment lines. Used by {@link #read} when a schema is already bound but the input split
     * still starts with the file header.
     */
    private void skipHeaderLine(CsvLogicalRecordReader recordReader) throws IOException {
        String record;
        while ((record = recordReader.readRecord(false)) != null) {
            String trimmed = record.trim();
            if (trimmed.isEmpty() || (options.commentPrefix().isEmpty() == false && trimmed.startsWith(options.commentPrefix()))) {
                continue;
            }
            return;
        }
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
            String name = parts[0].trim();
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
     * Converts accumulated field content to a trimmed string, with a fast path that skips
     * {@link String#trim()} when the first and last characters are already non-whitespace
     * (the common case for clean CSV data).
     */
    static String emitField(StringBuilder current) {
        String s = current.toString();
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
     * Splits a <strong>header</strong> line into fields using the same comma/bracket/quote awareness as the data
     * splitter, but <em>preserves the original substring</em> of each field — including any surrounding quote
     * characters. Header fields like {@code "host:port"} need quotes intact so {@link #hasTypeAnnotations} can tell
     * a quoted name from a {@code name:type} annotation.
     */
    private static String[] splitFieldsForOptions(String line, CsvFormatOptions options) {
        if (options.multiValueSyntax() == CsvFormatOptions.MultiValueSyntax.BRACKETS && options.delimiter() == ',') {
            return splitHeaderCommaDelimiterBracketAware(line, options.quoteChar(), options.escapeChar());
        }
        return line.split(Pattern.quote(Character.toString(options.delimiter())));
    }

    /**
     * Header-only variant of {@link #splitCommaDelimiterBracketAwareFields}: tracks the same state machine but
     * emits the raw substring between commas instead of accumulating into a {@link StringBuilder} that strips
     * quotes. Used by schema discovery / inference.
     */
    private static String[] splitHeaderCommaDelimiterBracketAware(String line, char quote, char esc) {
        final char delim = ',';
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
            if (c == '[' && fieldHasNonWhitespace == false && hasMvcBracketClose(line, i)) {
                bracketDepth = 1;
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
     * Bracket- and quote-aware comma split; must stay aligned with {@link CsvBatchIterator#splitLineBracketAware}.
     */
    private static String[] splitCommaDelimiterBracketAwareFields(String line, char quote, char esc) {
        final char delim = ',';
        List<String> entries = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
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
                current.append(c);
                if (c == '[') {
                    bracketDepth++;
                } else if (c == ']') {
                    bracketDepth--;
                }
                i++;
            } else if (c == quote && (current.length() == 0 || isWhitespaceOnlyFieldPrefix(current))) {
                inQuotes = true;
                quoteOpenAt = i;
                i++;
            } else if (c == '[' && (current.length() == 0 || isWhitespaceOnlyFieldPrefix(current))) {
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
                    entries.add(emitField(current));
                    current = new StringBuilder();
                }
                i++;
            } else {
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
            entries.add(emitField(current));
        }
        return entries.toArray(String[]::new);
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
            try (CsvParser parser = sharedCsvMapper.getFactory().createParser(record)) {
                parser.setSchema(csvSchema);
                List<String> row = new ArrayList<>();
                JsonToken token;
                while ((token = parser.nextToken()) != null) {
                    if (token == JsonToken.VALUE_NULL) {
                        row.add(null);
                    } else if (token.isScalarValue()) {
                        row.add(parser.getValueAsString());
                    } else if (token != JsonToken.START_ARRAY && token != JsonToken.END_ARRAY) {
                        throw new IOException("Unexpected CSV token [" + token + "] while parsing record");
                    }
                }
                return row.isEmpty() ? null : row;
            }
        }
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
        private final DateTimeFormatter datetimeFormatter;
        private final boolean bracketMultiValues;
        private final String sourceLocation;
        private final SkipWarnings skipWarnings;
        private List<Attribute> schema;
        private int[] projectedIdx;
        private DataType[] projectedTypes;
        private Attribute[] projectedAttrs;
        private int columnCount;
        /** Total number of columns in the file schema (not just projected). */
        private int schemaColumnCount;
        /** Which source-column indices are projected (for skipping non-projected fields during splitting). */
        private BitSet projectedFieldSet;
        /** Maps a source-column index to its slot in {@link #rowBuffer} / {@link #projectedTypes}; -1 if not projected. */
        private int[] sourceToBufferIndex;
        private Object[] rowBuffer;
        private Iterator<List<?>> csvIterator;
        private List<String[]> prefetchedRows;
        private long prefetchedRowsBytes;
        // Inner close flag: gates hasNext() short-circuit after close and the one-shot teardown below. The base
        // BufferingPageIterator separately gates close()/closeInternal() re-entry; this one also stops iteration.
        private boolean closed = false;
        private long errorCount = 0;
        /**
         * Subset of {@link #errorCount} counting only row-dropping events (SKIP_ROW and structural
         * failures). Field-level NULL_FIELD events leave the row intact and increment
         * {@code errorCount} but NOT this counter. The chunk-poison gate keys on this so a
         * benign field-warning under NULL_FIELD does not discard a file's cache merge.
         */
        private long rowsSkipped = 0;
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

        /** Pinned at iterator open; closes the open-vs-close mtime-race window for the cache key. */
        private final long pinnedMtimeMillis;
        /** True for parallel-parsing chunks — close-time publish carries the partial-chunk marker. */
        private final boolean chunkMode;

        // Reader-level counters shared across this iterator and any sibling segments.
        private final CsvReaderCounters counters;

        CsvBatchIterator(
            BufferedReader reader,
            CsvLogicalRecordReader recordReader,
            InputStream stream,
            List<String> projectedColumns,
            int batchSize,
            List<Attribute> preResolvedSchema,
            ErrorPolicy errorPolicy,
            String sourceLocation,
            StorageObject cacheableObject,
            CountingInputStream byteCounter,
            long pinnedMtimeMillis,
            boolean chunkMode,
            CsvReaderCounters counters
        ) {
            this.reader = reader;
            this.recordReader = recordReader;
            this.stream = stream;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.preResolvedSchema = preResolvedSchema;
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
            this.skipWarnings = SkipWarnings.of(
                errorPolicy,
                "CSV read from ["
                    + sourceLocation
                    + "] encountered parse errors handled per policy (policy: "
                    + errorPolicy.modeName()
                    + "); affected rows/fields are listed below"
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
            if (cacheableObject == null || columnCount == 0 || projectedAttrs == null) {
                return;
            }
            if (columnStats == null) {
                columnStats = ColumnStatsAccumulator.forProjectedAttributes(projectedAttrs);
            }
            for (int i = 0; i < columnCount; i++) {
                columnStats.acceptBlockAt(i, page.getBlock(i));
            }
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
                if (modeOrdinal != ErrorPolicy.Mode.FAIL_FAST.ordinal() && errorCount > 0) {
                    logger.info(
                        "CSV parsing completed with [{}] errors out of [{}] rows (policy: {})",
                        errorCount,
                        totalRowCount,
                        errorPolicy.mode()
                    );
                }
                // The captured row count is accurate as long as no rows were DROPPED. NULL_FIELD
                // null-fills a malformed field but preserves the row, so rowsEmittedForCache still
                // matches the file's true row count even though errorCount > 0 — that read is safe to
                // cache. SKIP_ROW drops rows (rowsSkipped > 0), making the count policy-dependent:
                // suppress the whole-file write, and poison parallel chunks so the coordinator
                // discards the file rather than committing an under-counted sum. (FAIL_FAST aborts
                // before EOF, so naturallyExhausted already gates it out.)
                if (cacheableObject != null && naturallyExhausted && pinnedMtimeMillis >= 0 && schema != null) {
                    if (rowsSkipped == 0) {
                        Map<String, ExternalStats.ColumnStats> cols = columnStats == null ? Map.of() : columnStats.snapshot();
                        OptionalLong bytesRead = byteCounter == null ? OptionalLong.empty() : OptionalLong.of(byteCounter.getBytesRead());
                        String fingerprint = computeConfigFingerprint();
                        ExternalStats.Stats statsRecord = new ExternalStats.Stats(rowsEmittedForCache, bytesRead, cols);
                        // Whole-file publishes carry no partial marker; per-chunk publishes carry one, and
                        // the ParallelParsingCoordinator publishes a finalize marker at clean whole-file
                        // completion so the coordinator's reconciler only commits the merge then.
                        publishToCaptureSink(sourceLocation, pinnedMtimeMillis, fingerprint, statsRecord, schema, sizeInBytesFromLength());
                    } else if (chunkMode) {
                        // rowsSkipped > 0 here: a parallel chunk dropped rows mid-scan, so its partial
                        // would under-count. Poison the file — the reconciler discards every contribution.
                        Map<String, Object> poison = new HashMap<>();
                        poison.put(ExternalStats.MTIME_MILLIS_KEY, pinnedMtimeMillis);
                        poison.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);
                        ExternalStatsCapture.record(sourceLocation, poison);
                    }
                }
                reader.close();
                stream.close();
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
                if (useBracketAwareParsing == false && csvIterator == null) {
                    // Hot data path: Jackson reads directly from the BufferedReader and tokenizes records in its
                    // own bulk char buffer. The per-record byte cap is enforced upstream by the wrapping
                    // CsvRecordCappingInputStream, so we don't need CsvLogicalRecordReader's per-char loop here.
                    csvIterator = newJacksonBulkIterator(reader);
                }
            }
            boolean useFusedBracketPath = csvIterator == null && bracketMultiValues && options.delimiter() == ',';
            while (true) {
                if (useFusedBracketPath && prefetchedRows == null && columnCount > 0) {
                    List<String> lines = new ArrayList<>();
                    readLogicalLinesBracketAware(lines, batchSize);
                    if (lines.isEmpty()) {
                        return null;
                    }
                    Page page = convertLinesToPage(lines);
                    if (page != null || modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                        return page;
                    }
                } else {
                    List<String[]> rows = new ArrayList<>();
                    if (prefetchedRows != null) {
                        rows.addAll(prefetchedRows);
                        prefetchedRows = null;
                        blockFactory.breaker().addWithoutBreaking(-prefetchedRowsBytes);
                        prefetchedRowsBytes = 0;
                    }
                    if (useFusedBracketPath) {
                        readRowsBracketAware(rows, batchSize - rows.size());
                    } else {
                        while (rows.size() < batchSize) {
                            try {
                                if (csvIterator.hasNext() == false) {
                                    break;
                                }
                                List<?> rowList = csvIterator.next();
                                String[] row = new String[rowList.size()];
                                for (int i = 0; i < rowList.size(); i++) {
                                    Object val = rowList.get(i);
                                    row[i] = val != null ? val.toString() : null;
                                }
                                if (hasCommentFilter && row.length > 0 && row[0] != null) {
                                    String trimmedFirstCell = row[0].trim();
                                    if (trimmedFirstCell.startsWith(options.commentPrefix())) {
                                        continue;
                                    }
                                }
                                rows.add(row);
                            } catch (RuntimeException e) {
                                totalRowCount++;
                                onRowError("CSV parse error: " + CsvErrorMessages.summarize(e.getMessage()), e, EMPTY_ROW, true);
                            }
                        }
                    }

                    if (rows.isEmpty()) {
                        return null;
                    }

                    Page page = convertRowsToPage(rows);
                    if (page != null || modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                        return page;
                    }
                }
            }
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
                onRowError(e.getMessage(), e, EMPTY_ROW, true);
                return "";
            }
        }

        private void readRowsBracketAware(List<String[]> rows, int batchSize) throws IOException {
            String record;
            while (rows.size() < batchSize && (record = readBracketAwareRecord()) != null) {
                if (isBlankOrComment(record, options.commentPrefix())) {
                    continue;
                }
                try {
                    String[] row = splitLineBracketAware(record);
                    rows.add(row);
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
            return splitCommaDelimiterBracketAwareFields(line, options.quoteChar(), options.escapeChar());
        }

        private List<Attribute> inferSchemaFromBatchReader(String headerLine) throws IOException {
            String[] columnNames = splitFieldsForOptions(headerLine, options);
            csvIterator = newCsvIterator(recordReader);
            SchemaSample sample = collectSampleRows(
                csvIterator,
                options.commentPrefix(),
                schemaSampleSize,
                blockFactory.breaker(),
                errorPolicy
            );
            // Drop the per-record sampling iterator so the bulk Jackson path can pick up where the
            // sample left off. Without this reset, inferred-schema reads stay on the slow per-record
            // CsvLogicalRecordReader path for the remainder of the file.
            csvIterator = null;
            if (sample.rows().isEmpty()) {
                blockFactory.breaker().addWithoutBreaking(-sample.reservedBytes());
                return null;
            }
            prefetchedRows = sample.rows();
            prefetchedRowsBytes = sample.reservedBytes();
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows());
        }

        private List<Attribute> inferSchemaHeaderlessFromBatchReader() throws IOException {
            csvIterator = newCsvIterator(recordReader);
            SchemaSample sample = collectSampleRows(
                csvIterator,
                options.commentPrefix(),
                schemaSampleSize,
                blockFactory.breaker(),
                errorPolicy
            );
            csvIterator = null;
            if (sample.rows().isEmpty()) {
                blockFactory.breaker().addWithoutBreaking(-sample.reservedBytes());
                return null;
            }
            prefetchedRows = sample.rows();
            prefetchedRowsBytes = sample.reservedBytes();
            return inferSyntheticSchema(sample.rows(), options.columnPrefix());
        }

        private void initProjection() {
            int schemaSize = schema.size();
            schemaColumnCount = schemaSize;
            if (projectedColumns == null) {
                columnCount = schemaSize;
                projectedIdx = new int[schemaSize];
                for (int i = 0; i < schemaSize; i++) {
                    projectedIdx[i] = i;
                }
            } else if (projectedColumns.isEmpty()) {
                columnCount = 0;
                projectedIdx = new int[0];
            } else {
                columnCount = projectedColumns.size();
                projectedIdx = new int[columnCount];
                for (int c = 0; c < columnCount; c++) {
                    String colName = projectedColumns.get(c);
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
                    projectedIdx[c] = index;
                }
            }
            projectedTypes = new DataType[columnCount];
            projectedAttrs = new Attribute[columnCount];
            for (int i = 0; i < columnCount; i++) {
                Attribute attr = schema.get(projectedIdx[i]);
                projectedAttrs[i] = attr;
                projectedTypes[i] = attr.dataType();
            }
            rowBuffer = new Object[columnCount];

            projectedFieldSet = new BitSet(schemaSize);
            sourceToBufferIndex = new int[schemaSize];
            Arrays.fill(sourceToBufferIndex, -1);
            for (int i = 0; i < columnCount; i++) {
                projectedFieldSet.set(projectedIdx[i]);
                sourceToBufferIndex[projectedIdx[i]] = i;
            }
        }

        private Page convertRowsToPage(List<String[]> rows) {
            int schemaSize = schema.size();
            // COUNT(*) fast path: no columns projected, so skip builder allocation and type conversion
            // and emit a row-count-only Page. We still apply the column-count validation that the
            // regular path uses so structural errors are routed through the policy consistently.
            if (columnCount == 0) {
                int acceptedRows = 0;
                for (String[] row : rows) {
                    totalRowCount++;
                    if (row.length > schemaSize) {
                        onRowError(
                            "CSV row has [" + row.length + "] columns but schema defines [" + schemaSize + "] columns",
                            null,
                            row,
                            true
                        );
                        continue;
                    }
                    acceptedRows++;
                }
                return acceptedRows == 0 ? null : new Page(acceptedRows);
            }
            BlockUtils.BuilderWrapper[] builders = new BlockUtils.BuilderWrapper[columnCount];
            try {
                for (int i = 0; i < columnCount; i++) {
                    builders[i] = BlockUtils.wrapperFor(
                        blockFactory,
                        ElementType.fromJava(javaClassForDataType(projectedTypes[i])),
                        rows.size()
                    );
                }
                int acceptedRows = 0;
                for (String[] row : rows) {
                    totalRowCount++;
                    if (row.length > schemaSize) {
                        onRowError(
                            "CSV row has [" + row.length + "] columns but schema defines [" + schemaSize + "] columns",
                            null,
                            row,
                            true
                        );
                        continue;
                    }
                    if (convertRowInPlace(row)) {
                        for (int i = 0; i < columnCount; i++) {
                            builders[i].append().accept(rowBuffer[i]);
                        }
                        acceptedRows++;
                    }
                }
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

        private boolean convertRowInPlace(String[] row) {
            int mode = this.modeOrdinal;
            for (int i = 0; i < columnCount; i++) {
                int si = projectedIdx[i];
                String value = si < row.length ? row[si] : null;
                Object result = tryConvertValue(value, projectedTypes[i]);
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
         * Collects logical lines from the bracket-aware CSV reader, handling multi-line quoted
         * fields. Blank/comment lines are skipped. Collected lines are not yet split or
         * type-converted — that work is deferred to {@link #splitAndConvertProjected}.
         */
        private void readLogicalLinesBracketAware(List<String> lines, int batchSize) throws IOException {
            String record;
            while (lines.size() < batchSize && (record = readBracketAwareRecord()) != null) {
                if (isBlankOrComment(record, options.commentPrefix())) {
                    continue;
                }
                lines.add(record);
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
                        ElementType.fromJava(javaClassForDataType(projectedTypes[i])),
                        lines.size()
                    );
                }
                int acceptedRows = 0;
                for (String line : lines) {
                    totalRowCount++;
                    try {
                        if (splitAndConvertProjected(line)) {
                            for (int i = 0; i < columnCount; i++) {
                                builders[i].append().accept(rowBuffer[i]);
                            }
                            acceptedRows++;
                        }
                    } catch (MalformedRowException e) {
                        onRowError(e.getMessage(), e, line, true);
                    }
                }
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
         * Fused field-splitting and typed conversion for the bracket-aware CSV path. Walks the
         * line character-by-character, maintaining the same quote/bracket/escape state machine as
         * {@link CsvFormatReader#splitCommaDelimiterBracketAwareFields}, but skips
         * {@link StringBuilder} accumulation for non-projected fields and inlines integer/long
         * parsing to avoid a second character walk via {@code Long.parseLong}.
         *
         * @return {@code true} if the row was accepted, {@code false} if it was rejected
         */
        private boolean splitAndConvertProjected(String line) {
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

            boolean isProjected = fieldIndex < schemaColumnCount && projectedFieldSet.get(fieldIndex);
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
                                rowBuffer[bufIdx] = null;
                            }
                            current.setLength(0);
                        }
                        fieldIndex++;
                        fieldHasNonWhitespace = false;
                        trailingFieldHasContent = false;
                        isProjected = fieldIndex < schemaColumnCount && projectedFieldSet.get(fieldIndex);
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

            int totalFields = trailingFieldHasContent ? fieldIndex + 1 : fieldIndex;
            if (totalFields > schemaColumnCount) {
                onRowError(
                    "CSV row has [" + totalFields + "] columns but schema defines [" + schemaColumnCount + "] columns",
                    null,
                    line,
                    true
                );
                return false;
            }

            if (trailingFieldHasContent && isProjected) {
                if (current.length() > 0) {
                    if (emitConvertedField(current, bufIdx, dt, numericValid, numAcc, negative, numStarted, line) == false) {
                        return false;
                    }
                } else {
                    rowBuffer[bufIdx] = null;
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
            return emitConvertedStringField(emitField(current), bufIdx, dt, rawLine);
        }

        /**
         * Converts a trimmed string field value and stores it in {@link #rowBuffer}, routing
         * parse errors through the error policy.
         */
        private boolean emitConvertedStringField(String value, int bufIdx, DataType dt, String rawLine) {
            Object result = tryConvertValue(value, dt);
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

        private Object tryConvertValue(String value, DataType dataType) {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("null")) {
                return null;
            }
            if (hasCustomNullValue && value.equals(nullValueStr)) {
                return null;
            }
            if (bracketMultiValues && value.startsWith("[") && value.endsWith("]")) {
                return tryConvertMultiValue(value, dataType);
            }
            return switch (dataType) {
                case INTEGER -> tryParseInt(value);
                case LONG -> tryParseLong(value);
                case DOUBLE -> tryParseDouble(value);
                case KEYWORD, TEXT -> new BytesRef(value);
                case BOOLEAN -> tryParseBoolean(value);
                case DATETIME -> tryParseDatetime(value);
                case DATE_NANOS -> tryParseDateNanos(value);
                case IP -> tryParseIp(value);
                case VERSION -> tryParseVersion(value);
                case NULL -> null;
                default -> {
                    lastFieldError = "Unsupported data type: " + dataType;
                    yield null;
                }
            };
        }

        private Object tryConvertMultiValue(String value, DataType dataType) {
            String content = value.substring(1, value.length() - 1).trim();
            if (content.isEmpty()) {
                return null;
            }
            List<String> parts = splitBracketContent(content);
            List<Object> result = new ArrayList<>(parts.size());
            for (String part : parts) {
                Object elem = parseElement(part, dataType);
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
                    result.add(current.toString().trim());
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
            result.add(current.toString().trim());
            return result;
        }

        private Object parseElement(String value, DataType dataType) {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("null")) {
                return null;
            }
            if (hasCustomNullValue && value.equals(nullValueStr)) {
                return null;
            }
            value = unquoteElement(value);
            if (value.isEmpty()) {
                return null;
            }
            return switch (dataType) {
                case INTEGER -> tryParseInt(value);
                case LONG -> tryParseLong(value);
                case DOUBLE -> tryParseDouble(value);
                case KEYWORD, TEXT -> new BytesRef(value);
                case BOOLEAN -> tryParseBoolean(value);
                case DATETIME -> tryParseDatetime(value);
                case DATE_NANOS -> tryParseDateNanos(value);
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

        private Object tryParseInt(String value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [INTEGER]";
                return null;
            }
        }

        private Object tryParseLong(String value) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [LONG]";
                return null;
            }
        }

        private Object tryParseDouble(String value) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [DOUBLE]";
                return null;
            }
        }

        private Object tryParseBoolean(String value) {
            try {
                return Booleans.parseBoolean(value.toLowerCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [BOOLEAN]";
                return null;
            }
        }

        private Object tryParseDatetime(String value) {
            if (looksNumeric(value)) {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException e) {}
            }
            if (datetimeFormatter != null) {
                try {
                    return LocalDateTime.parse(value, datetimeFormatter).toInstant(ZoneOffset.UTC).toEpochMilli();
                } catch (DateTimeParseException e) {
                    lastFieldError = "Failed to parse CSV datetime value [" + value + "]";
                    return null;
                }
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

        private Object tryParseDateNanos(String value) {
            if (looksNumeric(value)) {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException e) {}
            }
            if (datetimeFormatter != null) {
                try {
                    Instant instant = LocalDateTime.parse(value, datetimeFormatter).toInstant(ZoneOffset.UTC);
                    return org.elasticsearch.common.time.DateUtils.toLong(instant);
                } catch (DateTimeParseException | ArithmeticException e) {
                    lastFieldError = "Failed to parse CSV date_nanos value [" + value + "]";
                    return null;
                }
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
                    ? "; set error_mode to skip_row (or null_field) in WITH options to skip and warn instead of failing"
                    : "; set error_mode to null_field in WITH options to null-fill the bad field instead of failing";
                throw new ParsingException(
                    cause,
                    Source.EMPTY,
                    "{}",
                    "CSV parse error at row [" + totalRowCount + "]: " + message + "; row: " + rowExcerpt + hint
                );
            }
            errorCount++;
            rowsSkipped++;
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

        private Class<?> javaClassForDataType(DataType dataType) {
            return switch (dataType) {
                case INTEGER -> Integer.class;
                case LONG, DATETIME, DATE_NANOS -> Long.class;
                case DOUBLE -> Double.class;
                case KEYWORD, TEXT, IP, VERSION -> BytesRef.class;
                case BOOLEAN -> Boolean.class;
                case NULL -> Void.class;
                default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
            };
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
