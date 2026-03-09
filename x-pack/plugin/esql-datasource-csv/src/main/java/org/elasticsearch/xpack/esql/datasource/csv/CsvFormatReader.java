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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
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
 * {@code datetime} ({@code date}, {@code dt}), {@code null} ({@code n}).
 *
 * <h2>Configurable options</h2>
 * All options are set via the {@code WITH} clause and parsed by {@link #withConfig(java.util.Map)}.
 *
 * <table>
 *   <caption>CSV options and their equivalents in other engines</caption>
 *   <tr><th>ES/ESQL key</th><th>Default</th><th>Spark</th><th>DuckDB</th><th>ClickHouse</th></tr>
 *   <tr><td>{@code delimiter}</td><td>{@code ,}</td><td>{@code sep}</td>
 *       <td>{@code delim}</td><td>{@code format_csv_delimiter}</td></tr>
 *   <tr><td>{@code quote}</td><td>{@code "}</td><td>{@code quote}</td>
 *       <td>{@code quote}</td><td>{@code format_csv_allow_single_quotes}</td></tr>
 *   <tr><td>{@code escape}</td><td>{@code \}</td><td>{@code escape}</td>
 *       <td>{@code escape}</td><td>—</td></tr>
 *   <tr><td>{@code comment}</td><td>{@code //}</td><td>{@code comment}</td>
 *       <td>{@code comment}</td><td>—</td></tr>
 *   <tr><td>{@code null_value}</td><td>(empty)</td><td>{@code nullValue}</td>
 *       <td>{@code nullstr}</td><td>{@code format_csv_null_representation}</td></tr>
 *   <tr><td>{@code encoding}</td><td>{@code UTF-8}</td><td>{@code encoding}</td>
 *       <td>—</td><td>—</td></tr>
 *   <tr><td>{@code datetime_format}</td><td>ISO-8601 / epoch</td><td>{@code timestampFormat}</td>
 *       <td>{@code timestampformat}</td><td>{@code date_time_input_format}</td></tr>
 *   <tr><td>{@code max_field_size}</td><td>10 MB</td><td>{@code maxCharsPerColumn}</td>
 *       <td>{@code max_line_size}</td><td>—</td></tr>
 * </table>
 *
 * <h2>Error handling</h2>
 * Controlled by {@link ErrorPolicy} and its {@link ErrorPolicy.Mode}:
 * <table>
 *   <caption>Error mode comparison</caption>
 *   <tr><th>ES/ESQL key</th><th>Spark</th><th>DuckDB</th><th>Behaviour</th></tr>
 *   <tr><td>{@code fail_fast}</td><td>FAILFAST</td><td>(default)</td><td>Abort on first error</td></tr>
 *   <tr><td>{@code skip_row}</td><td>DROPMALFORMED</td><td>ignore_errors</td>
 *       <td>Drop the entire bad row</td></tr>
 *   <tr><td>{@code null_field}</td><td>PERMISSIVE</td><td>—</td>
 *       <td>Null-fill unparseable fields, keep the row</td></tr>
 * </table>
 *
 * <h2>Example</h2>
 * <pre>{@code
 *   FROM s3://bucket/data.tsv WITH {"delimiter": "\t", "error_mode": "skip_row", "max_errors": 100}
 * }</pre>
 *
 * <p>Works with any {@link org.elasticsearch.xpack.esql.datasources.spi.StorageProvider}
 * (HTTP, S3, local filesystem).
 */
public class CsvFormatReader implements SegmentableFormatReader {

    private static final Logger logger = LogManager.getLogger(CsvFormatReader.class);

    private static final int READER_BUFFER_SIZE = 64 * 1024;

    private final BlockFactory blockFactory;

    /**
     * Jackson CsvMapper is thread-safe after configuration (all enable/disable
     * calls happen in the constructor). Shared across all CsvBatchIterator
     * instances to avoid repeated configuration overhead.
     */
    private final CsvMapper sharedCsvMapper;

    private final CsvFormatOptions options;

    public CsvFormatReader(BlockFactory blockFactory) {
        this(blockFactory, CsvFormatOptions.DEFAULT);
    }

    private CsvFormatReader(BlockFactory blockFactory, CsvFormatOptions options) {
        this.blockFactory = blockFactory;
        this.options = options;
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

        if (delimiter == ','
            && quoteChar == '"'
            && escapeChar == '\\'
            && "//".equals(commentPrefix)
            && "".equals(nullValue)
            && StandardCharsets.UTF_8.equals(encoding)
            && datetimeFormatter == null
            && maxFieldSize == CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE) {
            return null;
        }
        return new CsvFormatOptions(delimiter, quoteChar, escapeChar, commentPrefix, nullValue, encoding, datetimeFormatter, maxFieldSize);
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

    /**
     * Returns a new CsvFormatReader configured with the given options.
     */
    public CsvFormatReader withOptions(CsvFormatOptions newOptions) {
        return new CsvFormatReader(blockFactory, newOptions);
    }

    @Override
    public FormatReader withConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return this;
        }
        CsvFormatOptions parsed = parseOptionsFromConfig(config);
        return parsed == null ? this : withOptions(parsed);
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
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || (options.commentPrefix().isEmpty() == false && line.startsWith(options.commentPrefix()))) {
                    continue;
                }
                return parseSchema(line);
            }
            throw new IOException("CSV file has no schema line");
        }
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return read(object, projectedColumns, batchSize, defaultErrorPolicy());
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize, ErrorPolicy errorPolicy)
        throws IOException {
        InputStream stream = object.newStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, options.encoding()), READER_BUFFER_SIZE);
        ErrorPolicy effective = errorPolicy != null ? errorPolicy : defaultErrorPolicy();
        return new CsvBatchIterator(reader, stream, projectedColumns, batchSize, null, effective);
    }

    @Override
    public CloseableIterator<Page> readSplit(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        boolean skipFirstLine,
        boolean lastSplit,
        List<Attribute> resolvedAttributes
    ) throws IOException {
        return readSplit(object, projectedColumns, batchSize, skipFirstLine, lastSplit, resolvedAttributes, defaultErrorPolicy());
    }

    @Override
    public CloseableIterator<Page> readSplit(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        boolean skipFirstLine,
        boolean lastSplit,
        List<Attribute> resolvedAttributes,
        ErrorPolicy errorPolicy
    ) throws IOException {
        InputStream stream = object.newStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, options.encoding()), READER_BUFFER_SIZE);
        if (skipFirstLine) {
            reader.readLine();
        }
        ErrorPolicy effective = errorPolicy != null ? errorPolicy : defaultErrorPolicy();
        return new CsvBatchIterator(reader, stream, projectedColumns, batchSize, resolvedAttributes, effective);
    }

    /**
     * Quote-aware record boundary detection for parallel parsing.
     * Tracks CSV quoting state so that newlines inside quoted fields are not
     * treated as record boundaries. Handles RFC 4180 escaped quotes ({@code ""})
     * correctly — a pair of double-quotes inside a quoted field does not toggle
     * the quoting state. Safe for TSV (which has no quoting, so the
     * {@code inQuotes} flag never toggles).
     * <p>
     * <b>Limitation:</b> this method only handles RFC 4180 doubled-quote escaping
     * ({@code ""}). Backslash-based escaping ({@code \"}) is not recognized here;
     * when {@code escapeChar} differs from {@code quoteChar}, boundaries may be
     * mis-detected inside fields that use backslash escaping. This is acceptable
     * because split-based parallel reads are an optimisation — the Jackson parser
     * in each split handles escape characters correctly.
     */
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
        return "csv";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".csv", ".tsv");
    }

    @Override
    public void close() throws IOException {
        // No resources to close at reader level
    }

    private List<Attribute> parseSchema(String schemaLine) {
        String[] columns = schemaLine.split(Pattern.quote(Character.toString(options.delimiter())));
        List<Attribute> attributes = new ArrayList<>(columns.length);

        for (String column : columns) {
            String trimmedColumn = column.trim();
            String[] parts = trimmedColumn.split(":");
            if (parts.length != 2) {
                throw new ParsingException("Invalid CSV schema format: [{}]. Expected 'name:type'", column);
            }

            String name = parts[0].trim();
            String trimmedType = parts[1].trim();
            String typeName = trimmedType.toUpperCase(java.util.Locale.ROOT);
            DataType dataType = parseDataType(typeName);

            EsField field = new EsField(name, dataType, java.util.Map.of(), true, EsField.TimeSeriesFieldType.NONE);
            attributes.add(new FieldAttribute(Source.EMPTY, name, field));
        }

        return attributes;
    }

    private DataType parseDataType(String typeName) {
        return switch (typeName) {
            case "INTEGER", "INT", "I" -> DataType.INTEGER;
            case "LONG", "L" -> DataType.LONG;
            case "DOUBLE", "D" -> DataType.DOUBLE;
            case "KEYWORD", "K", "STRING", "S" -> DataType.KEYWORD;
            case "TEXT", "TXT" -> DataType.TEXT;
            case "BOOLEAN", "BOOL" -> DataType.BOOLEAN;
            case "DATETIME", "DATE", "DT" -> DataType.DATETIME;
            case "NULL", "N" -> DataType.NULL;
            default -> throw EsqlIllegalArgumentException.illegalDataType(typeName);
        };
    }

    /**
     * Iterator that reads CSV data in batches and converts to ESQL Pages.
     * <p>
     * Performance-critical design choices:
     * <ul>
     *   <li>Pre-computed {@code int[]} for projected column indices — avoids autoboxing
     *   <li>Pre-computed {@code DataType[]} and {@code Attribute[]} arrays — avoids list lookups per field
     *   <li>Hoisted invariant flags ({@code hasCommentFilter}, {@code hasCustomNullValue}) — avoids per-row checks
     *   <li>Exception-free error path: {@code tryConvertValue} returns {@code null} on failure and sets
     *       {@code lastFieldError} — avoids exception allocation/stack-fill on the hot path
     *   <li>Reusable {@code Object[]} buffer across rows — avoids per-row allocation
     *   <li>Mode ordinal resolved once at construction — single int comparison per row instead of method calls
     * </ul>
     */
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

        private List<Attribute> schema;
        private int[] projectedIdx;
        private DataType[] projectedTypes;
        private Attribute[] projectedAttrs;
        private int columnCount;
        private Object[] rowBuffer;
        private Iterator<List<?>> csvIterator;
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
            ErrorPolicy errorPolicy
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
                reader.close();
                stream.close();
            }
        }

        private Page readNextBatch() throws IOException {
            if (schema == null) {
                if (preResolvedSchema != null) {
                    schema = preResolvedSchema;
                } else {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || (hasCommentFilter && line.startsWith(options.commentPrefix()))) {
                            continue;
                        }
                        schema = parseSchema(line);
                        break;
                    }
                    if (schema == null) {
                        return null;
                    }
                }
                initProjection();
                CsvSchema csvSchema = CsvSchema.emptySchema()
                    .withColumnSeparator(options.delimiter())
                    .withQuoteChar(options.quoteChar())
                    .withEscapeChar(options.escapeChar())
                    .withNullValue(options.nullValue());

                csvIterator = sharedCsvMapper.readerFor(List.class).with(csvSchema).readValues(reader);
            }

            while (true) {
                List<String[]> rows = new ArrayList<>();
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

                if (rows.isEmpty()) {
                    return null;
                }

                Page page = convertRowsToPage(rows);
                if (page != null || modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                    return page;
                }
            }
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
                        org.elasticsearch.compute.data.ElementType.fromJava(javaClassForDataType(projectedTypes[i])),
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

        /**
         * Converts a raw CSV row into {@link #rowBuffer} in place.
         * Returns {@code true} if the row was accepted, {@code false} if it was skipped.
         * Throws on budget exceeded or strict-mode failure.
         */
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

        /**
         * Attempts to convert a string value to the target type.
         * On success, returns the converted value and {@code lastFieldError} is null.
         * On failure, returns null and sets {@code lastFieldError} to a description.
         * This avoids exception allocation on the hot path.
         */
        private Object tryConvertValue(String value, DataType dataType) {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("null")) {
                return null;
            }
            if (hasCustomNullValue && value.equals(nullValueStr)) {
                return null;
            }
            return switch (dataType) {
                case INTEGER -> tryParseInt(value);
                case LONG -> tryParseLong(value);
                case DOUBLE -> tryParseDouble(value);
                case KEYWORD, TEXT -> new BytesRef(value);
                case BOOLEAN -> tryParseBoolean(value);
                case DATETIME -> tryParseDatetime(value);
                case NULL -> null;
                default -> {
                    lastFieldError = "Unsupported data type: " + dataType;
                    yield null;
                }
            };
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
                return Booleans.parseBoolean(value);
            } catch (IllegalArgumentException e) {
                lastFieldError = "Failed to parse CSV value [" + value + "] as [BOOLEAN]";
                return null;
            }
        }

        private Object tryParseDatetime(String value) {
            if (looksNumeric(value)) {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException e) {
                    // overflow — fall through
                }
            }
            if (datetimeFormatter != null) {
                try {
                    return LocalDateTime.parse(value, datetimeFormatter).toInstant(ZoneOffset.UTC).toEpochMilli();
                } catch (DateTimeParseException e) {
                    lastFieldError = "Failed to parse CSV datetime value [" + value + "]";
                    return null;
                }
            }
            try {
                return Instant.parse(value).toEpochMilli();
            } catch (DateTimeParseException e) {
                lastFieldError = "Failed to parse CSV datetime value [" + value + "]";
                return null;
            }
        }

        private void onRowError(String message, Exception cause, String[] row) {
            if (modeOrdinal == ErrorPolicy.Mode.FAIL_FAST.ordinal()) {
                throw new EsqlIllegalArgumentException(cause, message);
            }
            errorCount++;
            if (logErrors) {
                logger.warn("Skipping malformed CSV row (error {}/{}): {}", errorCount, errorPolicy.maxErrors(), message);
            }
            checkBudget(message, cause);
        }

        private void onFieldError(String message, String value, Attribute attr) {
            errorCount++;
            if (logErrors) {
                logger.warn(
                    "Null-filling unparseable field [{}] value [{}] (error {}/{}): {}",
                    attr.name(),
                    value,
                    errorCount,
                    errorPolicy.maxErrors(),
                    message
                );
            }
            checkBudget(message, null);
        }

        private void checkBudget(String message, Exception cause) {
            if (errorPolicy.isBudgetExceeded(errorCount, totalRowCount)) {
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
                case LONG, DATETIME -> Long.class;
                case DOUBLE -> Double.class;
                case KEYWORD, TEXT -> BytesRef.class;
                case BOOLEAN -> Boolean.class;
                case NULL -> Void.class;
                default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
            };
        }

        private static boolean looksNumeric(String value) {
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
