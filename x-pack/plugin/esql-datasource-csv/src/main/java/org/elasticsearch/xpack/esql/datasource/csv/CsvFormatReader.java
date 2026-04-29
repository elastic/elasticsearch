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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
 * All options are set via the {@code WITH} clause and parsed by {@link #withConfig(java.util.Map)}.
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
 *           a 0-based counter is appended (e.g. {@code col0, col1, col2, ...}).</td></tr>
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

    private final BlockFactory blockFactory;
    private final CsvMapper sharedCsvMapper;
    private final CsvFormatOptions options;
    private final String format;
    private final List<String> extensions;
    private final List<Attribute> resolvedSchema;
    private final int schemaSampleSize;

    public CsvFormatReader(BlockFactory blockFactory) {
        this(blockFactory, CsvFormatOptions.DEFAULT, "csv", List.of(".csv", ".tsv"), null, CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE);
    }

    public CsvFormatReader(BlockFactory blockFactory, String format, List<String> extensions) {
        this(blockFactory, CsvFormatOptions.DEFAULT, format, extensions, null, CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE);
    }

    public CsvFormatReader(BlockFactory blockFactory, CsvFormatOptions options, String format, List<String> extensions) {
        this(blockFactory, options, format, extensions, null, CsvSchemaInferrer.DEFAULT_SAMPLE_SIZE);
    }

    private CsvFormatReader(
        BlockFactory blockFactory,
        CsvFormatOptions options,
        String format,
        List<String> extensions,
        List<Attribute> resolvedSchema,
        int schemaSampleSize
    ) {
        this.blockFactory = blockFactory;
        this.options = options;
        this.format = format;
        this.extensions = extensions;
        this.resolvedSchema = resolvedSchema;
        this.schemaSampleSize = schemaSampleSize;
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

    private static CsvFormatOptions parseOptionsFromConfig(Map<String, Object> config) {
        char delimiter = parseChar(config.get("delimiter"), ',');
        char quoteChar = parseChar(config.get("quote"), '"');
        char escapeChar = parseChar(config.get("escape"), '\\');
        String commentPrefix = parseString(config.get("comment"), "//");
        String nullValue = parseString(config.get("null_value"), "");
        Charset encoding = parseEncoding(config.get("encoding"));
        DateTimeFormatter datetimeFormatter = parseDatetimeFormat(config.get("datetime_format"));
        int maxFieldSize = parseInt(config.get("max_field_size"), CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE);
        CsvFormatOptions.MultiValueSyntax multiValueSyntax = parseMultiValueSyntax(config.get("multi_value_syntax"));
        boolean headerRow = parseBoolean(config.get("header_row"), true);
        String columnPrefix = parseString(config.get("column_prefix"), CsvFormatOptions.DEFAULT_COLUMN_PREFIX);

        if (delimiter == ','
            && quoteChar == '"'
            && escapeChar == '\\'
            && "//".equals(commentPrefix)
            && "".equals(nullValue)
            && StandardCharsets.UTF_8.equals(encoding)
            && datetimeFormatter == null
            && maxFieldSize == CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE
            && multiValueSyntax == CsvFormatOptions.MultiValueSyntax.BRACKETS
            && headerRow
            && CsvFormatOptions.DEFAULT_COLUMN_PREFIX.equals(columnPrefix)) {
            return null;
        }
        return new CsvFormatOptions(
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
    }

    private static CsvFormatOptions.MultiValueSyntax parseMultiValueSyntax(Object value) {
        if (value == null || value.toString().isEmpty()) {
            return CsvFormatOptions.MultiValueSyntax.BRACKETS;
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

    private static boolean parseBoolean(Object value, boolean defaultValue) {
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
        if ("true".equalsIgnoreCase(s)) {
            return true;
        }
        if ("false".equalsIgnoreCase(s)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value [" + value + "]");
    }

    private static Charset parseEncoding(Object value) {
        if (value == null || value.toString().isEmpty()) {
            return StandardCharsets.UTF_8;
        }
        try {
            return Charset.forName(value.toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid encoding [" + value + "]", e);
        }
    }

    private static DateTimeFormatter parseDatetimeFormat(Object value) {
        if (value == null || value.toString().isEmpty()) {
            return null;
        }
        try {
            return DateTimeFormatter.ofPattern(value.toString(), Locale.ROOT);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid datetime format [" + value + "]", e);
        }
    }

    public CsvFormatReader withOptions(CsvFormatOptions newOptions) {
        return new CsvFormatReader(blockFactory, newOptions, format, extensions, resolvedSchema, schemaSampleSize);
    }

    @Override
    public CsvFormatReader withSchema(List<Attribute> schema) {
        return new CsvFormatReader(blockFactory, options, format, extensions, schema, schemaSampleSize);
    }

    @Override
    public FormatReader withConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return this;
        }
        CsvFormatOptions parsed = parseOptionsFromConfig(config);
        int newSampleSize = parseInt(config.get("schema_sample_size"), schemaSampleSize);
        Check.isTrue(newSampleSize > 0, "schema_sample_size must be positive, got: {}", newSampleSize);
        CsvFormatReader result = parsed != null ? withOptions(parsed) : this;
        if (newSampleSize != result.schemaSampleSize) {
            result = new CsvFormatReader(
                result.blockFactory,
                result.options,
                result.format,
                result.extensions,
                result.resolvedSchema,
                newSampleSize
            );
        }
        return result;
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
        String[] columnNames = headerLine.split(Pattern.quote(Character.toString(options.delimiter())));
        Iterator<List<?>> csvIterator = newCsvIterator(reader);
        CircuitBreaker breaker = blockFactory.breaker();
        SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, breaker);
        try {
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows());
        } finally {
            breaker.addWithoutBreaking(-sample.reservedBytes());
        }
    }

    private List<Attribute> inferSchemaWithSyntheticNames(BufferedReader reader) throws IOException {
        Iterator<List<?>> csvIterator = newCsvIterator(reader);
        CircuitBreaker breaker = blockFactory.breaker();
        SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, breaker);
        try {
            if (sample.rows().isEmpty()) {
                throw new IOException("CSV file has no data rows");
            }
            int columnCount = 0;
            for (String[] row : sample.rows()) {
                if (row.length > columnCount) {
                    columnCount = row.length;
                }
            }
            String[] columnNames = synthesizeColumnNames(columnCount, options.columnPrefix());
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows());
        } finally {
            breaker.addWithoutBreaking(-sample.reservedBytes());
        }
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
            throw new ParsingException(
                "CSV header has duplicate column names {}; if the file has no header row, "
                    + "set [\"header_row\": false] in the WITH options",
                duplicates
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

    static SchemaSample collectSampleRows(Iterator<List<?>> csvIterator, String commentPrefix, int sampleSize, CircuitBreaker breaker) {
        List<String[]> sampleRows = new ArrayList<>();
        long reservedBytes = 0;
        boolean success = false;
        try {
            boolean hasCommentFilter = commentPrefix != null && commentPrefix.isEmpty() == false;
            while (sampleRows.size() < sampleSize && csvIterator.hasNext()) {
                List<?> rowList = csvIterator.next();
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
            }
            success = true;
            return new SchemaSample(sampleRows, reservedBytes);
        } finally {
            if (success == false) {
                breaker.addWithoutBreaking(-reservedBytes);
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
        if (context.firstSplit()) {
            effectiveSchema = null;
        } else {
            reader.readLine();
            effectiveSchema = resolvedSchema;
        }
        ErrorPolicy effective = context.errorPolicy() != null ? context.errorPolicy() : defaultErrorPolicy();
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

    private List<Attribute> parseSchema(String schemaLine) {
        String[] columns = schemaLine.split(Pattern.quote(Character.toString(options.delimiter())));
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
                    rows.addAll(readRowsBracketAware(batchSize - rows.size()));
                } else {
                    while (rows.size() < batchSize && csvIterator.hasNext()) {
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
         */
        private List<String[]> readRowsBracketAware(int batchSize) throws IOException {
            List<String[]> rows = new ArrayList<>();
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
                String[] row = splitLineBracketAware(logicalLine.toString());
                rows.add(row);
            }
            return rows;
        }

        private static boolean hasUnclosedQuote(String s, char quote) {
            boolean inQuotes = false;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == quote) {
                    if (i + 1 < s.length() && s.charAt(i + 1) == quote) {
                        i++;
                        continue;
                    }
                    inQuotes = !inQuotes;
                }
            }
            return inQuotes;
        }

        /**
         * Splits a CSV line by delimiter, treating quoted fields and {@code [..,..,..]} as single cells.
         * Commas inside quotes or brackets are not delimiters. Escaped commas ({@code \,}) are skipped.
         */
        private String[] splitLineBracketAware(String line) {
            List<String> entries = new ArrayList<>();
            char delim = options.delimiter();
            char quote = options.quoteChar();
            char esc = options.escapeChar();
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;
            boolean inBrackets = false;
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
                } else if (inBrackets) {
                    current.append(c);
                    if (c == ']') {
                        inBrackets = false;
                        entries.add(current.toString());
                        current = new StringBuilder();
                        i++;
                        while (i < line.length() && line.charAt(i) == ' ') {
                            i++;
                        }
                        if (i < line.length() && line.charAt(i) == delim) {
                            i++;
                            continue;
                        }
                        continue;
                    }
                    i++;
                } else if (c == quote) {
                    inQuotes = true;
                    i++;
                } else if (c == '[' && current.length() == 0) {
                    inBrackets = true;
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
                throw new EsqlIllegalArgumentException("Unclosed quoted field in line [{}]", line);
            }
            if (inBrackets) {
                throw new EsqlIllegalArgumentException("Unclosed bracket cell in line [{}]", line);
            }
            if (current.length() > 0) {
                entries.add(current.toString().trim());
            }
            return entries.toArray(String[]::new);
        }

        private List<Attribute> inferSchemaFromBatchReader(String headerLine) throws IOException {
            String[] columnNames = headerLine.split(Pattern.quote(Character.toString(options.delimiter())));
            csvIterator = newCsvIterator(reader);
            SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, blockFactory.breaker());
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
            SchemaSample sample = collectSampleRows(csvIterator, options.commentPrefix(), schemaSampleSize, blockFactory.breaker());
            if (sample.rows().isEmpty()) {
                blockFactory.breaker().addWithoutBreaking(-sample.reservedBytes());
                return null;
            }
            int columnCount = 0;
            for (String[] row : sample.rows()) {
                if (row.length > columnCount) {
                    columnCount = row.length;
                }
            }
            String[] columnNames = synthesizeColumnNames(columnCount, options.columnPrefix());
            prefetchedRows = sample.rows();
            prefetchedRowsBytes = sample.reservedBytes();
            return CsvSchemaInferrer.inferSchema(columnNames, sample.rows());
        }

        private void initProjection() {
            int schemaSize = schema.size();
            if (projectedColumns == null || projectedColumns.isEmpty()) {
                columnCount = schemaSize;
                projectedIdx = new int[schemaSize];
                for (int i = 0; i < schemaSize; i++) {
                    projectedIdx[i] = i;
                }
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
            BlockUtils.BuilderWrapper[] builders = new BlockUtils.BuilderWrapper[columnCount];
            try {
                for (int i = 0; i < columnCount; i++) {
                    builders[i] = BlockUtils.wrapperFor(
                        blockFactory,
                        ElementType.fromJava(javaClassForDataType(projectedTypes[i])),
                        rows.size()
                    );
                }
                int schemaSize = schema.size();
                int acceptedRows = 0;
                for (String[] row : rows) {
                    totalRowCount++;
                    if (row.length > schemaSize) {
                        onRowError("CSV row has [" + row.length + "] columns but schema defines [" + schemaSize + "] columns", null, row);
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
                        onRowError(err, null, row);
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

        private void onRowError(String message, Exception cause, String[] row) {
            if (modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                throw new EsqlIllegalArgumentException(cause, message);
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
            skipWarnings.add("Row [" + totalRowCount + "] field [" + attr.name() + "] value [" + value + "]: " + message);
            if (logErrors) {
                logger.warn(
                    "Null-filling unparseable field [{}] value [{}] in row [{}] (error {}/{}): {}",
                    attr.name(),
                    value,
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
                throw new EsqlIllegalArgumentException(
                    cause,
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
