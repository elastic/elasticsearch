/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Build-time generator for Parquet fixture files. Converts CSV to Parquet.
 * Accepts source CSV path and output Parquet path as arguments.
 * Uses {@link CsvFixtureParser} for bracket-aware CSV parsing with correct multi-value handling.
 */
public final class ParquetFixtureGenerator {

    private ParquetFixtureGenerator() {}

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.out and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: ParquetFixtureGenerator <source-csv-path> <output-parquet-path> [<split-count>]");
            System.exit(1);
        }
        Path sourcePath = Path.of(args[0]);
        Path outputPath = Path.of(args[1]);
        int splitCount = args.length == 3 ? Integer.parseInt(args[2]) : 1;
        if (splitCount < 1) {
            throw new IllegalArgumentException("split-count must be >= 1, got " + splitCount);
        }
        if (Files.exists(sourcePath) == false) {
            throw new IOException("Source CSV not found: " + sourcePath);
        }
        if (splitCount == 1) {
            Files.createDirectories(outputPath.getParent());
            byte[] parquetBytes = generateFromCsv(sourcePath, 0, Integer.MAX_VALUE);
            Files.write(outputPath, parquetBytes);
            System.out.println("Generated Parquet fixture: " + outputPath);
            return;
        }
        // Multi-file mode: outputPath is a template like /path/employees.parquet — emit
        // /path/employees_00.parquet … _NN.parquet by splitting rows into contiguous chunks.
        // Contiguous (rather than round-robin) gives each file a non-overlapping value range
        // so MIN/MAX statistics differ across files, which exercises the per-split metadata
        // aggregation merge path more thoroughly than identical row distributions.
        Path parent = outputPath.getParent();
        Files.createDirectories(parent);
        String fileName = outputPath.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String stem = dot < 0 ? fileName : fileName.substring(0, dot);
        String ext = dot < 0 ? "" : fileName.substring(dot);

        CsvFixtureParser.CsvFixtureResult parsed = CsvFixtureParser.parseCsvFile(sourcePath);
        int totalRows = parsed.rows().size();
        for (int i = 0; i < splitCount; i++) {
            int start = (int) ((long) totalRows * i / splitCount);
            int end = (int) ((long) totalRows * (i + 1) / splitCount);
            String shardName = String.format(Locale.ROOT, "%s_%02d%s", stem, i, ext);
            Path shardPath = parent.resolve(shardName);
            byte[] shardBytes = generateFromParsed(parsed, start, end);
            Files.write(shardPath, shardBytes);
            System.out.println("Generated Parquet fixture shard [" + start + "," + end + "): " + shardPath);
        }
    }

    private static byte[] generateFromCsv(Path sourcePath, int rowStart, int rowEnd) throws IOException {
        return generateFromParsed(CsvFixtureParser.parseCsvFile(sourcePath), rowStart, rowEnd);
    }

    private static byte[] generateFromParsed(CsvFixtureParser.CsvFixtureResult result, int rowStart, int rowEnd) throws IOException {
        List<CsvFixtureParser.ColumnSpec> columns = result.schema();
        List<Object[]> allRows = result.rows();
        List<Object[]> rows = allRows.subList(Math.max(0, rowStart), Math.min(rowEnd, allRows.size()));

        // Detect list-typed columns over the full dataset, not just the slice, so shards
        // emitted by multi-file mode share an identical schema regardless of which shard a
        // list value happens to land in.
        boolean[] isListColumn = new boolean[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            for (Object[] row : allRows) {
                if (c < row.length && row[c] instanceof List) {
                    isListColumn[c] = true;
                    break;
                }
            }
        }

        MessageType schema = buildSchema(columns, isListColumn);
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (Object[] row : rows) {
                Group g = factory.newGroup();
                for (int i = 0; i < columns.size(); i++) {
                    Object value = i < row.length ? row[i] : null;
                    addValue(g, columns.get(i), isListColumn[i], value);
                }
                writer.write(g);
            }
        }
        return baos.toByteArray();
    }

    private static MessageType buildSchema(List<CsvFixtureParser.ColumnSpec> columns, boolean[] isListColumn) {
        List<Type> fields = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            CsvFixtureParser.ColumnSpec col = columns.get(i);
            if (isListColumn[i]) {
                Type listType = parquetListType(col);
                fields.add(listType);
            } else {
                fields.add(parquetScalarType(col.type()).named(col.name()));
            }
        }
        return new MessageType("schema", fields);
    }

    private static Type parquetListType(CsvFixtureParser.ColumnSpec col) {
        return switch (col.type()) {
            case "integer", "short", "byte" -> Types.optionalList()
                .optionalElement(PrimitiveType.PrimitiveTypeName.INT32)
                .named(col.name());
            case "long" -> Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT64).named(col.name());
            case "double", "scaled_float" -> Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.DOUBLE).named(col.name());
            case "float", "half_float" -> Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.FLOAT).named(col.name());
            case "boolean" -> Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(col.name());
            case "date" -> Types.optionalList()
                .optionalElement(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named(col.name());
            case "text", "txt" -> Types.optionalList()
                .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(col.name());
            default -> Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.BINARY).named(col.name());
        };
    }

    private static Types.PrimitiveBuilder<PrimitiveType> parquetScalarType(String type) {
        return switch (type) {
            case "integer", "short", "byte" -> Types.optional(PrimitiveType.PrimitiveTypeName.INT32);
            case "long" -> Types.optional(PrimitiveType.PrimitiveTypeName.INT64);
            case "double", "scaled_float" -> Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE);
            case "float", "half_float" -> Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT);
            case "boolean" -> Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN);
            case "ip" -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY);
            case "date" -> Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS));
            case "text", "txt" -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType());
            default -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY);
        };
    }

    private static void addValue(Group g, CsvFixtureParser.ColumnSpec col, boolean isList, Object value) {
        if (value == null) {
            return;
        }
        if (isList) {
            List<Object> listValue;
            if (value instanceof List<?> lv) {
                @SuppressWarnings("unchecked")
                List<Object> cast = (List<Object>) lv;
                listValue = cast;
            } else {
                listValue = List.of(value);
            }
            addListValue(g, col, listValue);
        } else {
            addScalarValue(g, col, value);
        }
    }

    private static void addListValue(Group g, CsvFixtureParser.ColumnSpec col, List<Object> list) {
        if (list == null || list.isEmpty()) {
            return;
        }
        Group listGroup = g.addGroup(col.name());
        for (Object elem : list) {
            Group listElement = listGroup.addGroup("list");
            if (elem != null) {
                appendListElement(listElement, col.type(), elem);
            }
        }
    }

    private static void appendListElement(Group listElement, String type, Object elem) {
        switch (type) {
            case "integer", "short", "byte" -> listElement.append("element", ((Number) elem).intValue());
            case "long" -> listElement.append("element", ((Number) elem).longValue());
            case "double", "scaled_float", "float", "half_float" -> listElement.append("element", ((Number) elem).doubleValue());
            case "boolean" -> listElement.append("element", Boolean.TRUE.equals(elem));
            case "date" -> listElement.append("element", ((Number) elem).longValue());
            default -> listElement.append("element", elem.toString());
        }
    }

    private static void addScalarValue(Group g, CsvFixtureParser.ColumnSpec col, Object value) {
        try {
            switch (col.type()) {
                case "integer", "short", "byte" -> g.add(col.name(), ((Number) value).intValue());
                case "long" -> g.add(col.name(), ((Number) value).longValue());
                case "double", "scaled_float" -> g.add(col.name(), ((Number) value).doubleValue());
                case "float", "half_float" -> g.add(col.name(), ((Number) value).floatValue());
                case "boolean" -> g.add(col.name(), Boolean.TRUE.equals(value));
                case "date" -> g.add(col.name(), ((Number) value).longValue());
                case "ip" -> g.add(col.name(), value.toString());
                default -> g.add(col.name(), value.toString());
            }
        } catch (Exception e) {
            // Skip unparseable values
        }
    }

    private static OutputFile createOutputFile(java.io.ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        baos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        baos.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }
}
