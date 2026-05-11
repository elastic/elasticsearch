/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.network.InetAddresses;
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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
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
 *   <tr><td>{@code multi_value_syntax}</td><td>{@code brackets}</td><td>Multi-value field syntax</td></tr>
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
    /**
     * ErrorPolicy used by the planning-time {@link #metadata} call (which has no per-query
     * {@link FormatReadContext}). Resolved from the {@code WITH} options in {@link #withConfig}
     * so a user request like {@code WITH {"error_mode": "skip_row"}} also applies to schema
     * sampling — matching ClickHouse's {@code input_format_allow_errors_*} semantics.
     * Defaults to {@link #defaultErrorPolicy()} (FAIL_FAST), so unset implies "fail at planning
     * if the file cannot be sampled cleanly", consistent with the rest of the system.
     */
    private final ErrorPolicy effectivePolicy;

    public CsvFormatReader(BlockFactory blockFactory) {
        this(
            blockFactory,
            CsvFormatOptions.DEFAULT,
            "csv",
            List.of(".csv", ".tsv"),
            null,
            CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE,
            ErrorPolicy.STRICT
        );
    }

    public CsvFormatReader(BlockFactory blockFactory, String format, List<String> extensions) {
        this(blockFactory, CsvFormatOptions.DEFAULT, format, extensions, null, CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE, ErrorPolicy.STRICT);
    }

    public CsvFormatReader(BlockFactory blockFactory, CsvFormatOptions options, String format, List<String> extensions) {
        this(blockFactory, options, format, extensions, null, CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE, ErrorPolicy.STRICT);
    }

    private CsvFormatReader(
        BlockFactory blockFactory,
        CsvFormatOptions options,
        String format,
        List<String> extensions,
        List<Attribute> resolvedSchema,
        int schemaSampleSize,
        ErrorPolicy effectivePolicy
    ) {
        this.blockFactory = blockFactory;
        this.options = options;
        this.format = format;
        this.extensions = extensions;
        this.resolvedSchema = resolvedSchema;
        this.schemaSampleSize = schemaSampleSize;
        this.effectivePolicy = effectivePolicy;
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
        return new CsvFormatReader(blockFactory, newOptions, format, extensions, resolvedSchema, schemaSampleSize, effectivePolicy);
    }

    @Override
    public CsvFormatReader withSchema(List<Attribute> schema) {
        return new CsvFormatReader(blockFactory, options, format, extensions, schema, schemaSampleSize, effectivePolicy);
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
        if (newSampleSize != result.schemaSampleSize || resolvedPolicy != result.effectivePolicy) {
            result = new CsvFormatReader(
                result.blockFactory,
                result.options,
                result.format,
                result.extensions,
                result.resolvedSchema,
                newSampleSize,
                resolvedPolicy
            );
        }
        return Configured.fromKnownSubset(result, config, RECOGNIZED_KEYS);
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        List<Attribute> schema = readSchema(object);
        StoragePath objectPath = object.path();
        return new SimpleSourceMetadata(schema, formatName(), objectPath.toString());
    }

    private List<Attribute> readSchema(StorageObject object) throws IOException {
        try (
            InputStream stream = object.newStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, options.encoding()), READER_BUFFER_SIZE)
        ) {
            if (options.headerRow() == false) {
                return inferSchemaWithSyntheticNames(reader);
            }
            String headerLine = null;
            while ((headerLine = reader.readLine()) != null) {
                headerLine = headerLine.trim();
                if (headerLine.isEmpty()
                    || (options.commentPrefix().isEmpty() == false && headerLine.startsWith(options.commentPrefix()))) {
                    continue;
                }
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
            List<Attribute> inferred = inferSchemaFromSample(headerLine, reader);
            checkUniqueAttributeNames(inferred);
            return inferred;
        }
    }

    private List<Attribute> inferSchemaFromSample(String headerLine, BufferedReader reader) throws IOException {
        String[] columnNames = splitFieldsForOptions(headerLine, options);
        Iterator<List<?>> csvIterator = newCsvIterator(reader);
        CircuitBreaker breaker = blockFactory.breaker();
        SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, breaker, effectivePolicy);
        try {
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows());
        } finally {
            breaker.addWithoutBreaking(-sample.reservedBytes());
        }
    }

    private List<Attribute> inferSchemaWithSyntheticNames(BufferedReader reader) throws IOException {
        Iterator<List<?>> csvIterator = newCsvIterator(reader);
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

    private Iterator<List<?>> newCsvIterator(Reader reader) throws IOException {
        CsvSchema csvSchema = CsvSchema.emptySchema()
            .withColumnSeparator(options.delimiter())
            .withQuoteChar(options.quoteChar())
            .withEscapeChar(options.escapeChar())
            .withNullValue(options.nullValue());
        return sharedCsvMapper.readerFor(List.class).with(csvSchema).readValues(reader);
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
        InputStream stream = object.newStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, options.encoding()), READER_BUFFER_SIZE);
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
                skipHeaderLine(reader);
            }
            effectiveSchema = readSchema;
        } else if (context.firstSplit()) {
            // resolvedSchema from withSchema(...) is the projected output, not the file's column
            // layout — using it as positional schema would mis-align columns. Only trust it when
            // recordAligned=true (streaming-parallel pre-bound the FULL file schema from chunk 0).
            if (context.recordAligned() && resolvedSchema != null) {
                if (options.headerRow()) {
                    skipHeaderLine(reader);
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
            reader.readLine();
            effectiveSchema = resolvedSchema;
        }
        // Falls back to effectivePolicy (resolved from WITH options in withConfig) so a user
        // request like WITH {"error_mode": "skip_row"} also applies to the data path when no
        // upstream caller has built a FormatReadContext with an explicit policy. The planner
        // path always sets context.errorPolicy() explicitly.
        ErrorPolicy effective = context.errorPolicy() != null ? context.errorPolicy() : effectivePolicy;
        return new CsvBatchIterator(
            reader,
            stream,
            context.projectedColumns(),
            context.batchSize(),
            effectiveSchema,
            effective,
            object.path().toString()
        );
    }

    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        if (options.multiValueSyntax() != CsvFormatOptions.MultiValueSyntax.BRACKETS || options.delimiter() != ',') {
            return findNextRecordBoundaryQuotedFieldsOnly(stream);
        }
        BufferedInputStream bis = stream instanceof BufferedInputStream b ? b : new BufferedInputStream(stream);
        int markLimit = recordBoundaryMarkLimit();
        long maxMvcSuffixBytes = Math.max(0L, markLimit - 1L);
        return findNextRecordBoundaryBracketCommaMvc(bis, markLimit, maxMvcSuffixBytes);
    }

    /**
     * Upper bound for {@link BufferedInputStream#mark(int)} while probing bracket MVC cells during record-boundary
     * scans. Matches {@link CsvFormatOptions#maxFieldSize()} so an unclosed bracket cell cannot invalidate the mark
     * before we reset and treat {@code [} as a literal byte.
     */
    private int recordBoundaryMarkLimit() {
        int maxField = options.maxFieldSize();
        if (maxField <= 0) {
            return Math.min(64 * 1024 * 1024, Integer.MAX_VALUE - 8);
        }
        return Math.min(maxField + 1024, Integer.MAX_VALUE - 8);
    }

    /**
     * Bytes consumed after an opening {@code [} until bracket depth returns to zero, or {@code -1} if EOF was reached
     * first or the scan exceeded {@link CsvFormatOptions#maxFieldSize()} (unclosed cell).
     */
    private long consumeBracketMvcSuffixBytes(BufferedInputStream in, long maxSuffixBytes) throws IOException {
        int depth = 1;
        long bytes = 0;
        while (depth > 0) {
            if (bytes >= maxSuffixBytes) {
                return -1;
            }
            int ib = in.read();
            if (ib == -1) {
                return -1;
            }
            bytes++;
            byte b = (byte) ib;
            if (b == '[') {
                depth++;
            } else if (b == ']') {
                depth--;
            }
        }
        return bytes;
    }

    private static boolean isAsciiCsvFieldLeadingWhitespace(int ib) {
        return ib == ' ' || ib == '\t' || ib == '\f';
    }

    /**
     * Record boundary scan for comma-delimited CSV with bracket MVC. Newlines inside {@code [..]} or quoted fields
     * must not end the record. Quote opening follows RFC 4180 — only at field start, optionally preceded by whitespace
     * — so stray {@code "} chars in unquoted cells do not trigger multi-line gluing or pathological segment splits.
     */
    private long findNextRecordBoundaryBracketCommaMvc(BufferedInputStream bis, int markLimit, long maxMvcSuffixBytes) throws IOException {
        long consumed = 0;
        boolean inQuotes = false;
        boolean fieldHasNonWhitespace = false;
        byte quoteAsByte = (byte) options.quoteChar();
        byte escAsByte = (byte) options.escapeChar();
        byte delimAsByte = (byte) options.delimiter();

        while (true) {
            int ib = bis.read();
            if (ib == -1) {
                return -1;
            }
            consumed++;
            byte b = (byte) ib;

            if (inQuotes) {
                if (b == quoteAsByte) {
                    bis.mark(2);
                    int ib2 = bis.read();
                    if (ib2 == -1) {
                        inQuotes = false;
                        continue;
                    }
                    if ((byte) ib2 == quoteAsByte) {
                        consumed++;
                        continue;
                    }
                    bis.reset();
                    inQuotes = false;
                } else if (b == escAsByte) {
                    bis.mark(2);
                    int ib2 = bis.read();
                    if (ib2 != -1 && (byte) ib2 == delimAsByte) {
                        consumed++;
                        continue;
                    }
                    bis.reset();
                }
                continue;
            }

            if (b == '\n') {
                return consumed;
            }
            if (b == delimAsByte) {
                fieldHasNonWhitespace = false;
                continue;
            }
            if (b == quoteAsByte && fieldHasNonWhitespace == false) {
                inQuotes = true;
                continue;
            }
            if (b == '[' && fieldHasNonWhitespace == false) {
                bis.mark(markLimit);
                long suffix = consumeBracketMvcSuffixBytes(bis, maxMvcSuffixBytes);
                if (suffix >= 0) {
                    consumed += suffix;
                    fieldHasNonWhitespace = true;
                    continue;
                }
                bis.reset();
                fieldHasNonWhitespace = true;
                continue;
            }
            if (isAsciiCsvFieldLeadingWhitespace(ib & 0xff) == false) {
                fieldHasNonWhitespace = true;
            }
        }
    }

    private long findNextRecordBoundaryQuotedFieldsOnly(InputStream stream) throws IOException {
        long consumed = 0;
        boolean inQuotes = false;
        byte quoteAsByte = (byte) options.quoteChar();
        byte[] buf = new byte[8192];
        int bytesRead;
        while ((bytesRead = stream.read(buf, 0, buf.length)) > 0) {
            for (int i = 0; i < bytesRead; i++) {
                consumed++;
                byte b = buf[i];
                if (b == quoteAsByte) {
                    if (inQuotes) {
                        if (i + 1 < bytesRead) {
                            if (buf[i + 1] == quoteAsByte) {
                                i++;
                                consumed++;
                                continue;
                            }
                            inQuotes = false;
                            if (buf[i + 1] == '\n') {
                                consumed++;
                                return consumed;
                            }
                            continue;
                        }
                        int next = stream.read();
                        if (next == -1) {
                            return -1;
                        }
                        consumed++;
                        if (next == quoteAsByte) {
                            continue;
                        }
                        inQuotes = false;
                        if (next == '\n') {
                            return consumed;
                        }
                        continue;
                    } else {
                        inQuotes = true;
                    }
                } else if (b == '\n' && inQuotes == false) {
                    return consumed;
                }
            }
        }
        return -1;
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
    public void close() throws IOException {}

    /**
     * Consumes one header line from {@code reader}, skipping over leading empty lines and
     * comment lines. Used by {@link #read} when a schema is already bound but the input split
     * still starts with the file header.
     */
    private void skipHeaderLine(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            String trimmed = line.trim();
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
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, name, dataType));
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
                i++;
            } else if (c == '[' && (current.length() == 0 || isWhitespaceOnlyFieldPrefix(current))) {
                if (hasMvcBracketClose(line, i)) {
                    bracketDepth = 1;
                }
                current.append(c);
                i++;
            } else if (c == delim) {
                if (i > 0 && line.charAt(i - 1) == esc) {
                    current.append(c);
                } else {
                    entries.add(current.toString().trim());
                    current = new StringBuilder();
                }
                i++;
            } else {
                current.append(c);
                i++;
            }
        }
        if (inQuotes) {
            throw new MalformedRowException("Unclosed quoted field in line [" + CsvErrorMessages.summarize(line) + "]");
        }
        if (bracketDepth > 0) {
            throw new MalformedRowException("Unclosed bracket cell in line [" + CsvErrorMessages.summarize(line) + "]");
        }
        if (current.length() > 0) {
            entries.add(current.toString().trim());
        }
        return entries.toArray(String[]::new);
    }

    private class CsvBatchIterator implements CloseableIterator<Page> {
        private final BufferedReader reader;
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
        private Object[] rowBuffer;
        private Iterator<List<?>> csvIterator;
        private List<String[]> prefetchedRows;
        private long prefetchedRowsBytes;
        private Page nextPage;
        private boolean closed = false;
        private long errorCount = 0;
        private long totalRowCount = 0;
        private String lastFieldError;

        CsvBatchIterator(
            BufferedReader reader,
            InputStream stream,
            List<String> projectedColumns,
            int batchSize,
            List<Attribute> preResolvedSchema,
            ErrorPolicy errorPolicy,
            String sourceLocation
        ) {
            this.reader = reader;
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
            try {
                nextPage = readNextBatch();
                return nextPage != null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read CSV batch", e);
            }
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            Page result = nextPage;
            nextPage = null;
            return result;
        }

        @Override
        public void close() throws IOException {
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
                reader.close();
                stream.close();
            }
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
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || (hasCommentFilter && line.startsWith(options.commentPrefix()))) {
                            continue;
                        }
                        headerLine = line;
                        break;
                    }
                    if (headerLine == null) {
                        return null;
                    }
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
                    CsvSchema csvSchema = CsvSchema.emptySchema()
                        .withColumnSeparator(options.delimiter())
                        .withQuoteChar(options.quoteChar())
                        .withEscapeChar(options.escapeChar())
                        .withNullValue(options.nullValue());
                    csvIterator = sharedCsvMapper.readerFor(List.class).with(csvSchema).readValues(reader);
                }
            }
            while (true) {
                List<String[]> rows = new ArrayList<>();
                if (prefetchedRows != null) {
                    rows.addAll(prefetchedRows);
                    prefetchedRows = null;
                    blockFactory.breaker().addWithoutBreaking(-prefetchedRowsBytes);
                    prefetchedRowsBytes = 0;
                }
                if (csvIterator == null && bracketMultiValues && options.delimiter() == ',') {
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
                            // Jackson's MappingIterator wraps both structural parse errors and any
                            // underlying IOException as RuntimeException since hasNext()/next() do
                            // not declare checked exceptions. Routing them through the error policy
                            // lets SKIP_ROW/NULL_FIELD recover from malformed rows (e.g. unclosed
                            // quote) without killing the query; FAIL_FAST re-throws inside
                            // onRowError. Jackson typically resynchronizes at the next record
                            // boundary so we keep going. Errors (OOM etc.) propagate.
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
        private void readRowsBracketAware(List<String[]> rows, int batchSize) throws IOException {
            String line;
            while (rows.size() < batchSize && (line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || (hasCommentFilter && line.startsWith(options.commentPrefix()))) {
                    continue;
                }
                StringBuilder logicalLine = new StringBuilder(line);
                while (hasUnclosedQuote(logicalLine.toString(), options.quoteChar())) {
                    String next = reader.readLine();
                    if (next == null) {
                        break;
                    }
                    logicalLine.append('\n').append(next);
                }
                try {
                    String[] row = splitLineBracketAware(logicalLine.toString());
                    rows.add(row);
                } catch (MalformedRowException e) {
                    totalRowCount++;
                    onRowError(e.getMessage(), e, EMPTY_ROW, true);
                }
            }
        }

        /**
         * RFC-4180-style detection for whether {@code s} ends inside a quoted field. A {@code "} only
         * opens quoting at field start (after {@code ,} or line-start, optionally preceded by whitespace);
         * stray {@code "} inside an unquoted cell or inside a {@code [..]} MVC cell is a literal byte, not a
         * quote toggle. Without this guard, real-world rows with embedded {@code "} characters cause
         * {@link #readRowsBracketAware} to merge adjacent physical lines until the {@code "} count is even,
         * yielding "row has [N] columns but schema defines [M]" failures and pathological multi-line scans.
         */
        private static boolean hasUnclosedQuote(String s, char quote) {
            boolean inQuotes = false;
            int bracketDepth = 0;
            boolean fieldHasNonWhitespace = false;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (inQuotes) {
                    if (c == quote) {
                        if (i + 1 < s.length() && s.charAt(i + 1) == quote) {
                            i++;
                            continue;
                        }
                        inQuotes = false;
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
                if (c == ',') {
                    fieldHasNonWhitespace = false;
                    continue;
                }
                if (c == quote && fieldHasNonWhitespace == false) {
                    inQuotes = true;
                    continue;
                }
                if (c == '[' && fieldHasNonWhitespace == false) {
                    // Intentionally enters bracket mode without calling hasMvcBracketClose: this
                    // method only determines whether a quote is unclosed for multi-line gluing.
                    // An unclosed bracket here is harmless (it suppresses quote detection for the
                    // remainder of the line) while a false-negative hasMvcBracketClose would cause
                    // spurious multi-line gluing on rows with stray quotes inside bracket cells.
                    bracketDepth = 1;
                    continue;
                }
                if (Character.isWhitespace(c) == false) {
                    fieldHasNonWhitespace = true;
                }
            }
            return inQuotes;
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
            csvIterator = newCsvIterator(reader);
            SchemaSample sample = collectSampleRows(
                csvIterator,
                options.commentPrefix(),
                schemaSampleSize,
                blockFactory.breaker(),
                errorPolicy
            );
            if (sample.rows().isEmpty()) {
                blockFactory.breaker().addWithoutBreaking(-sample.reservedBytes());
                return null;
            }
            prefetchedRows = sample.rows();
            prefetchedRowsBytes = sample.reservedBytes();
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows());
        }

        private List<Attribute> inferSchemaHeaderlessFromBatchReader() throws IOException {
            csvIterator = newCsvIterator(reader);
            SchemaSample sample = collectSampleRows(
                csvIterator,
                options.commentPrefix(),
                schemaSampleSize,
                blockFactory.breaker(),
                errorPolicy
            );
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
            if (projectedColumns == null) {
                // null means "no projection info available" — fall back to all columns for backward compat.
                columnCount = schemaSize;
                projectedIdx = new int[schemaSize];
                for (int i = 0; i < schemaSize; i++) {
                    projectedIdx[i] = i;
                }
            } else if (projectedColumns.isEmpty()) {
                // Empty list means the optimizer pruned every column (e.g. COUNT(*)). Skip all type
                // conversion and emit row-count-only Pages from convertRowsToPage.
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
                if (value != null) {
                    value = value.trim();
                }
                Object result = tryConvertValue(value, projectedTypes[i]);
                if (lastFieldError != null) {
                    if (mode == ErrorPolicy.Mode.NULL_FIELD.ordinal()) {
                        rowBuffer[i] = null;
                        onFieldError(lastFieldError, value, projectedAttrs[i]);
                        lastFieldError = null;
                    } else {
                        String err = lastFieldError;
                        lastFieldError = null;
                        // Field-type error: skip_row drops the whole row when one field is bad;
                        // null_field is usually the better escape hatch. structural=false so the
                        // FAIL_FAST hint suggests null_field.
                        onRowError(err, null, row, false);
                        return false;
                    }
                } else {
                    rowBuffer[i] = result;
                }
            }
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
            // Handles ISO-8601 with zone, zone-less timestamps, date-only, and whitespace-separated date-times
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
            if (modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                // Malformed user data → client error (HTTP 400), not a 500. Include the row index,
                // a capped row excerpt, and a hint pointing at the relaxed error modes so the
                // user knows there is a way out without grepping the docs.
                String hint = structural
                    ? "; set error_mode to skip_row (or null_field) in WITH options to skip and warn instead of failing"
                    : "; set error_mode to null_field in WITH options to null-fill the bad field instead of failing";
                throw new ParsingException(
                    cause,
                    Source.EMPTY,
                    "{}",
                    "CSV parse error at row [" + totalRowCount + "]: " + message + "; row: " + CsvErrorMessages.summarizeRow(row) + hint
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
