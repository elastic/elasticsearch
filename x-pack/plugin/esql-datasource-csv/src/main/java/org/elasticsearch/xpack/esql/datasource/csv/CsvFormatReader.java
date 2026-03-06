/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Simple CSV format reader for external datasources.
 *
 * <p>CSV Format:
 * - First line: schema definition (column_name:type_name,...)
 * - Subsequent lines: data rows
 * - Empty values are treated as null
 * - Lines starting with "//" are comments and ignored
 *
 * <p>Supported types: integer, long, double, keyword, text, boolean, datetime
 *
 * <p>This reader works with any StorageProvider (HTTP, S3, local).
 */
public class CsvFormatReader implements SegmentableFormatReader {

    private static final int READER_BUFFER_SIZE = 64 * 1024;

    private final BlockFactory blockFactory;

    /**
     * Jackson CsvMapper is thread-safe after configuration (all enable/disable
     * calls happen in the constructor). Shared across all CsvBatchIterator
     * instances to avoid repeated configuration overhead.
     */
    private final CsvMapper sharedCsvMapper;

    public CsvFormatReader(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
        this.sharedCsvMapper = new CsvMapper();
        this.sharedCsvMapper.enable(CsvParser.Feature.TRIM_SPACES);
        this.sharedCsvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        this.sharedCsvMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
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
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8), READER_BUFFER_SIZE)
        ) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("//")) {
                    continue;
                }
                return parseSchema(line);
            }
            throw new IOException("CSV file has no schema line");
        }
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        InputStream stream = object.newStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8), READER_BUFFER_SIZE);

        return new CsvBatchIterator(reader, stream, projectedColumns, batchSize, null);
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
        InputStream stream = object.newStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8), READER_BUFFER_SIZE);
        if (skipFirstLine) {
            reader.readLine();
        }
        return new CsvBatchIterator(reader, stream, projectedColumns, batchSize, resolvedAttributes);
    }

    /**
     * Quote-aware record boundary detection for parallel parsing.
     * Tracks CSV quoting state so that newlines inside quoted fields are not
     * treated as record boundaries. Handles RFC 4180 escaped quotes ({@code ""})
     * correctly — a pair of double-quotes inside a quoted field does not toggle
     * the quoting state. Safe for TSV (which has no quoting, so the
     * {@code inQuotes} flag never toggles).
     */
    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        long consumed = 0;
        boolean inQuotes = false;
        byte[] buf = new byte[8192];
        int bytesRead;
        while ((bytesRead = stream.read(buf, 0, buf.length)) > 0) {
            for (int i = 0; i < bytesRead; i++) {
                consumed++;
                byte b = buf[i];
                if (b == '"') {
                    if (inQuotes) {
                        if (i + 1 < bytesRead) {
                            if (buf[i + 1] == '"') {
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
                        if (next == '"') {
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
        String[] columns = schemaLine.split(",");
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
     * Uses Jackson CSV parser for robust CSV parsing with proper quote and escape handling.
     */
    private class CsvBatchIterator implements CloseableIterator<Page> {
        private final BufferedReader reader;
        private final InputStream stream;
        private final List<String> projectedColumns;
        private final int batchSize;
        private final List<Attribute> preResolvedSchema;

        private List<Attribute> schema;
        private List<Integer> projectedIndices;
        private Iterator<List<?>> csvIterator;
        private Page nextPage;
        private boolean closed = false;

        CsvBatchIterator(
            BufferedReader reader,
            InputStream stream,
            List<String> projectedColumns,
            int batchSize,
            List<Attribute> preResolvedSchema
        ) {
            this.reader = reader;
            this.stream = stream;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.preResolvedSchema = preResolvedSchema;
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
                    projectedIndices = computeProjectedIndices();
                } else {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || line.startsWith("//")) {
                            continue;
                        }
                        schema = parseSchema(line);
                        projectedIndices = computeProjectedIndices();
                        break;
                    }
                    if (schema == null) {
                        return null;
                    }
                }
                CsvSchema csvSchema = CsvSchema.emptySchema()
                    .withColumnSeparator(',')
                    .withQuoteChar('"')
                    .withEscapeChar('\\')
                    .withNullValue("");

                csvIterator = sharedCsvMapper.readerFor(List.class).with(csvSchema).readValues(reader);
            }

            // Read batch of rows using Jackson CSV parser
            List<String[]> rows = new ArrayList<>();
            while (rows.size() < batchSize && csvIterator.hasNext()) {
                List<?> rowList = csvIterator.next();
                // Convert List to String array
                String[] row = new String[rowList.size()];
                for (int i = 0; i < rowList.size(); i++) {
                    Object val = rowList.get(i);
                    row[i] = val != null ? val.toString() : null;
                }
                // Skip comment lines (Jackson doesn't have native comment support)
                if (row.length > 0) {
                    String firstCell = row[0];
                    if (firstCell != null) {
                        String trimmedFirstCell = firstCell.trim();
                        if (trimmedFirstCell.startsWith("//")) {
                            continue;
                        }
                    }
                }
                rows.add(row);
            }

            if (rows.isEmpty()) {
                return null; // No more data
            }

            return convertRowsToPage(rows);
        }

        private List<Integer> computeProjectedIndices() {
            if (projectedColumns == null || projectedColumns.isEmpty()) {
                // Return all columns
                List<Integer> indices = new ArrayList<>(schema.size());
                for (int i = 0; i < schema.size(); i++) {
                    indices.add(i);
                }
                return indices;
            }

            // Map projected column names to indices
            List<Integer> indices = new ArrayList<>(projectedColumns.size());
            for (String colName : projectedColumns) {
                int index = -1;
                for (int i = 0; i < schema.size(); i++) {
                    Attribute attr = schema.get(i);
                    if (attr.name().equals(colName)) {
                        index = i;
                        break;
                    }
                }
                if (index == -1) {
                    throw new EsqlIllegalArgumentException("Column not found in CSV schema: [{}]", colName);
                }
                indices.add(index);
            }
            return indices;
        }

        private Page convertRowsToPage(List<String[]> rows) {
            int rowCount = rows.size();
            int columnCount = projectedIndices.size();

            // Create block builders for projected columns
            BlockUtils.BuilderWrapper[] builders = new BlockUtils.BuilderWrapper[columnCount];
            try {
                for (int i = 0; i < columnCount; i++) {
                    int schemaIndex = projectedIndices.get(i);
                    Attribute attr = schema.get(schemaIndex);
                    builders[i] = BlockUtils.wrapperFor(
                        blockFactory,
                        org.elasticsearch.compute.data.ElementType.fromJava(javaClassForDataType(attr.dataType())),
                        rowCount
                    );
                }

                // Fill blocks with data
                for (String[] row : rows) {
                    // Jackson CSV may return shorter arrays if trailing values are empty
                    // We need to handle this gracefully
                    if (row.length > schema.size()) {
                        throw new ParsingException("CSV row has [{}] columns but schema defines [{}] columns", row.length, schema.size());
                    }

                    for (int i = 0; i < columnCount; i++) {
                        int schemaIndex = projectedIndices.get(i);
                        Attribute attr = schema.get(schemaIndex);

                        // Handle case where row is shorter than expected (trailing empty values)
                        String value = schemaIndex < row.length ? row[schemaIndex] : "";
                        if (value != null) {
                            value = value.trim();
                        }

                        Object converted = convertValue(value, attr.dataType());
                        BlockUtils.BuilderWrapper wrapper = builders[i];
                        wrapper.append().accept(converted);
                    }
                }

                // Build blocks
                Block[] blocks = new Block[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    BlockUtils.BuilderWrapper wrapper = builders[i];
                    Block.Builder builder = wrapper.builder();
                    blocks[i] = builder.build();
                }

                return new Page(rowCount, blocks);
            } finally {
                Releasables.closeExpectNoException(builders);
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

        private Object convertValue(String value, DataType dataType) {
            // Jackson CSV uses null for empty values when configured with withNullValue("")
            // Also handle explicit "null" string
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("null")) {
                return null;
            }

            try {
                return switch (dataType) {
                    case INTEGER -> Integer.parseInt(value);
                    case LONG -> Long.parseLong(value);
                    case DOUBLE -> Double.parseDouble(value);
                    case KEYWORD, TEXT -> new BytesRef(value);
                    case BOOLEAN -> Booleans.parseBoolean(value);
                    case DATETIME -> parseDatetime(value);
                    case NULL -> null;
                    default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
                };
            } catch (NumberFormatException e) {
                throw new EsqlIllegalArgumentException(e, "Failed to parse CSV value [{}] as [{}]", value, dataType);
            }
        }

        private long parseDatetime(String value) {
            // Numeric strings (epoch millis) contain only digits and optionally a leading minus
            if (looksNumeric(value)) {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException e) {
                    // overflow or not actually numeric, fall through to ISO-8601
                }
            }
            try {
                return Instant.parse(value).toEpochMilli();
            } catch (DateTimeParseException e) {
                throw new EsqlIllegalArgumentException(e, "Failed to parse CSV datetime value [{}]", value);
            }
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
