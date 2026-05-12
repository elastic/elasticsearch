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

/**
 * Build-time generator for Parquet fixture files. Converts CSV to Parquet.
 * <p>
 * Single-file mode: {@code <source-csv> <output.parquet>}
 * <br>
 * Split mode:       {@code <source-csv> <output-dir> <num-parts>} — writes
 * {@code <basename>_00.parquet}, {@code <basename>_01.parquet}, … into the directory.
 * <p>
 * Uses {@link CsvFixtureParser} for bracket-aware CSV parsing with correct multi-value handling.
 */
public final class ParquetFixtureGenerator {

    private ParquetFixtureGenerator() {}

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.out and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length == 2) {
            Path sourcePath = Path.of(args[0]);
            Path outputPath = Path.of(args[1]);
            if (Files.exists(sourcePath) == false) {
                throw new IOException("Source CSV not found: " + sourcePath);
            }
            Files.createDirectories(outputPath.getParent());
            byte[] parquetBytes = generateFromCsv(sourcePath, 0, Integer.MAX_VALUE);
            Files.write(outputPath, parquetBytes);
            System.out.println("Generated Parquet fixture: " + outputPath);
        } else if (args.length == 3) {
            Path sourcePath = Path.of(args[0]);
            Path outputDir = Path.of(args[1]);
            int numParts = Integer.parseInt(args[2]);
            if (Files.exists(sourcePath) == false) {
                throw new IOException("Source CSV not found: " + sourcePath);
            }
            Files.createDirectories(outputDir);
            String baseName = sourcePath.getFileName().toString().replaceFirst("\\.csv$", "");
            CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(sourcePath);
            int total = result.rows().size();
            int partSize = (total + numParts - 1) / numParts;
            for (int part = 0; part < numParts; part++) {
                int from = part * partSize;
                int to = Math.min(from + partSize, total);
                String fileName = String.format(java.util.Locale.ROOT, "%s_%02d.parquet", baseName, part);
                Path outputPath = outputDir.resolve(fileName);
                byte[] parquetBytes = generateFromRows(result, from, to);
                Files.write(outputPath, parquetBytes);
                System.out.println("Generated Parquet fixture: " + outputPath);
            }
        } else {
            System.err.println("Usage: ParquetFixtureGenerator <source-csv> <output.parquet>");
            System.err.println("       ParquetFixtureGenerator <source-csv> <output-dir> <num-parts>");
            System.exit(1);
        }
    }

    private static byte[] generateFromCsv(Path sourcePath, int from, int to) throws IOException {
        return generateFromRows(CsvFixtureParser.parseCsvFile(sourcePath), from, to);
    }

    private static byte[] generateFromRows(CsvFixtureParser.CsvFixtureResult result, int from, int to) throws IOException {
        List<CsvFixtureParser.ColumnSpec> columns = result.schema();
        List<Object[]> rows = result.rows().subList(from, Math.min(to, result.rows().size()));

        boolean[] isListColumn = new boolean[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            for (Object[] row : rows) {
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
