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
import org.elasticsearch.xpack.esql.datasource.csv.SplitPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Build-time generator for Parquet fixture files. Converts CSV to Parquet.
 * <p>
 * Single-file mode: {@code <source-csv> <output.parquet> [codec]}
 * <br>
 * Split mode:       {@code <source-csv> <output-dir> <num-parts> [codec]} — writes
 * {@code <basename>_00.parquet}, {@code <basename>_01.parquet}, … into the directory.
 * <br>
 * Hive-by mode: {@code <source-csv> <output-dir> --hive-by <source-column> <partition-column-name>} —
 * groups rows by the value of {@code source-column} into Hive-style partition directories
 * {@code <output-dir>/<partition-column-name>=<value>/<basename>.parquet}. Rows where the source column
 * is null go into {@code <partition-column-name>=__HIVE_DEFAULT_PARTITION__}, matching how Hive encodes
 * null partitions on disk. The partition column name is intentionally distinct
 * from the source column so the source column can stay in the parquet payload without colliding with the
 * virtual partition column injected by the reader.
 * <p>
 * The optional {@code codec} argument selects the Parquet internal compression codec
 * (e.g. {@code SNAPPY}, {@code GZIP}, {@code ZSTD}, {@code LZ4_RAW}).
 * When omitted, {@code UNCOMPRESSED} is used.
 * <p>
 * Uses {@link CsvFixtureParser} for bracket-aware CSV parsing with correct multi-value handling.
 */
public final class ParquetFixtureGenerator {

    private ParquetFixtureGenerator() {}

    private static final Logger logger = LoggerFactory.getLogger(ParquetFixtureGenerator.class);
    private static final String HIVE_BY_FLAG = "--hive-by";

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.err and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length == 5 && HIVE_BY_FLAG.equals(args[2])) {
            Path sourcePath = Path.of(args[0]);
            Path outputDir = Path.of(args[1]);
            String sourceColumn = args[3];
            String partitionColumn = args[4];
            if (Files.exists(sourcePath) == false) {
                throw new IOException("Source CSV not found: " + sourcePath);
            }
            generateHivePartitionedByColumn(sourcePath, outputDir, sourceColumn, partitionColumn, CompressionCodecName.UNCOMPRESSED);
        } else if (args.length == 2 || (args.length == 3 && isCodecName(args[2]))) {
            Path sourcePath = Path.of(args[0]);
            Path outputPath = Path.of(args[1]);
            CompressionCodecName codec = args.length == 3 ? CompressionCodecName.valueOf(args[2]) : CompressionCodecName.UNCOMPRESSED;
            if (Files.exists(sourcePath) == false) {
                throw new IOException("Source CSV not found: " + sourcePath);
            }
            Files.createDirectories(outputPath.getParent());
            byte[] parquetBytes = generateFromCsv(sourcePath, 0, Integer.MAX_VALUE, codec);
            Files.write(outputPath, parquetBytes);
            logger.info("Generated Parquet fixture ({}): {}", codec, outputPath);
        } else if (args.length == 3 || args.length == 4) {
            Path sourcePath = Path.of(args[0]);
            Path outputDir = Path.of(args[1]);
            int numParts = Integer.parseInt(args[2]);
            CompressionCodecName codec = args.length == 4 ? CompressionCodecName.valueOf(args[3]) : CompressionCodecName.UNCOMPRESSED;
            if (Files.exists(sourcePath) == false) {
                throw new IOException("Source CSV not found: " + sourcePath);
            }
            Files.createDirectories(outputDir);
            String baseName = sourcePath.getFileName().toString().replaceFirst("\\.csv$", "");
            CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(sourcePath);
            int total = result.rows().size();
            for (int part = 0; part < numParts; part++) {
                SplitPartitioner.Range range = SplitPartitioner.partitionRange(total, numParts, part);
                if (range == null) {
                    break;
                }
                String fileName = String.format(Locale.ROOT, "%s_%02d.parquet", baseName, part);
                Path outputPath = outputDir.resolve(fileName);
                byte[] parquetBytes = generateFromRows(result, range.from(), range.to(), codec);
                Files.write(outputPath, parquetBytes);
                logger.info("Generated Parquet fixture ({}): {}", codec, outputPath);
            }
        } else {
            System.err.println("Usage: ParquetFixtureGenerator <source-csv> <output.parquet> [codec]");
            System.err.println("       ParquetFixtureGenerator <source-csv> <output-dir> <num-parts> [codec]");
            System.err.println(
                "       ParquetFixtureGenerator <source-csv> <output-dir> --hive-by <source-column> <partition-column-name>"
            );
            System.err.println("Codecs: UNCOMPRESSED (default), SNAPPY, GZIP, ZSTD, LZ4_RAW");
            System.exit(1);
        }
    }

    /**
     * Writes one Parquet file per distinct value of {@code sourceColumn} into Hive-style partition
     * directories named after {@code partitionColumn}. Rows where the source column is null go into the
     * literal Hive sentinel directory {@code <partitionColumn>=__HIVE_DEFAULT_PARTITION__}. The source
     * column is preserved in the parquet payload so the fixture can also be queried on the data column
     * itself; the partition column name is deliberately distinct to avoid colliding with that payload.
     */
    private static void generateHivePartitionedByColumn(
        Path sourcePath,
        Path outputDir,
        String sourceColumn,
        String partitionColumn,
        CompressionCodecName codec
    ) throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(sourcePath);
        int sourceColIdx = -1;
        for (int i = 0; i < result.schema().size(); i++) {
            if (result.schema().get(i).name().equals(sourceColumn)) {
                sourceColIdx = i;
                break;
            }
        }
        if (sourceColIdx < 0) {
            throw new IOException("Source column not found in CSV: " + sourceColumn);
        }

        String baseName = sourcePath.getFileName().toString().replaceFirst("\\.csv$", "");
        // LinkedHashMap so the on-disk layout iterates buckets in a deterministic order.
        Map<String, List<Object[]>> buckets = new LinkedHashMap<>();
        for (Object[] row : result.rows()) {
            Object cell = sourceColIdx < row.length ? row[sourceColIdx] : null;
            String bucket = cell == null ? "__HIVE_DEFAULT_PARTITION__" : cell.toString();
            buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(row);
        }

        Files.createDirectories(outputDir);
        for (Map.Entry<String, List<Object[]>> e : buckets.entrySet()) {
            Path partitionDir = outputDir.resolve(partitionColumn + "=" + e.getKey());
            Files.createDirectories(partitionDir);
            Path outputPath = partitionDir.resolve(baseName + ".parquet");
            byte[] bytes = generateFromRows(
                new CsvFixtureParser.CsvFixtureResult(result.schema(), e.getValue()),
                0,
                e.getValue().size(),
                codec
            );
            Files.write(outputPath, bytes);
            logger.info("Generated Hive partition ({}): {} ({} rows)", codec, outputPath, e.getValue().size());
        }
    }

    /**
     * Returns true when the string is one of the supported Parquet compression codec names.
     * Used to disambiguate the 3-arg form (single-file + codec) from the 3-arg split form
     * (split mode with num-parts).
     */
    private static boolean isCodecName(String s) {
        for (CompressionCodecName c : CompressionCodecName.values()) {
            if (c.name().equals(s)) {
                return true;
            }
        }
        return false;
    }

    private static byte[] generateFromCsv(Path sourcePath, int from, int to, CompressionCodecName codec) throws IOException {
        return generateFromRows(CsvFixtureParser.parseCsvFile(sourcePath), from, to, codec);
    }

    private static byte[] generateFromRows(CsvFixtureParser.CsvFixtureResult result, int from, int to, CompressionCodecName codec)
        throws IOException {
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

        boolean[] flatten = CsvFixtureParser.computeFlatten(columns);
        // Pre-compute per-column dotted-path segments; flat (literal-dot) columns get a 1-element
        // path containing the original full name, so resolveParent treats them as top-level.
        String[][] columnPaths = new String[columns.size()][];
        for (int i = 0; i < columns.size(); i++) {
            columnPaths[i] = flatten[i] ? new String[] { columns.get(i).name() } : columns.get(i).name().split("\\.");
        }
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile).withType(schema).withCompressionCodec(codec).build()) {
            for (Object[] row : rows) {
                Group g = factory.newGroup();
                Map<String, Group> parentCache = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    Object value = i < row.length ? row[i] : null;
                    Group target = resolveParent(g, columnPaths[i], parentCache);
                    String leafName = columnPaths[i][columnPaths[i].length - 1];
                    addValueAt(target, leafName, columns.get(i).type(), isListColumn[i], value);
                }
                writer.write(g);
            }
        }
        return baos.toByteArray();
    }

    /**
     * Returns the parent {@link Group} for the given path, creating nested groups lazily and
     * caching them by dotted-prefix so two columns sharing the same parent (e.g.
     * {@code event.action} and {@code event.outcome}) write into the same group instance.
     */
    private static Group resolveParent(Group root, String[] path, Map<String, Group> parentCache) {
        if (path.length == 1) {
            return root;
        }
        StringBuilder key = new StringBuilder();
        Group current = root;
        for (int i = 0; i < path.length - 1; i++) {
            if (i > 0) {
                key.append('.');
            }
            key.append(path[i]);
            String cacheKey = key.toString();
            Group existing = parentCache.get(cacheKey);
            if (existing == null) {
                existing = current.addGroup(path[i]);
                parentCache.put(cacheKey, existing);
            }
            current = existing;
        }
        return current;
    }

    /**
     * Builds a Parquet schema from the CSV columns. CSV headers containing a literal {@code .}
     * (e.g. {@code event.action:STRING}) are interpreted as paths into nested STRUCT groups —
     * columns sharing a prefix are grouped under a single optional Parquet group, <b>unless</b>
     * that prefix already exists as a separate top-level column in the same CSV (e.g.
     * {@code languages} + {@code languages.long}). In that case the dotted children remain flat
     * to preserve backward compatibility with existing fixtures that rely on literal-dot
     * top-level column names.
     */
    private static MessageType buildSchema(List<CsvFixtureParser.ColumnSpec> columns, boolean[] isListColumn) {
        boolean[] flatten = CsvFixtureParser.computeFlatten(columns);
        Node root = new Node();
        for (int i = 0; i < columns.size(); i++) {
            if (flatten[i]) {
                Node leaf = new Node();
                leaf.columnIndex = i;
                root.children.put(columns.get(i).name(), leaf);
                continue;
            }
            String[] segments = columns.get(i).name().split("\\.");
            Node current = root;
            for (int s = 0; s < segments.length - 1; s++) {
                current = current.children.computeIfAbsent(segments[s], k -> new Node());
            }
            Node leaf = new Node();
            leaf.columnIndex = i;
            current.children.put(segments[segments.length - 1], leaf);
        }
        List<Type> fields = new ArrayList<>();
        for (Map.Entry<String, Node> e : root.children.entrySet()) {
            fields.add(buildType(e.getKey(), e.getValue(), columns, isListColumn));
        }
        return new MessageType("schema", fields);
    }

    private static Type buildType(String name, Node node, List<CsvFixtureParser.ColumnSpec> columns, boolean[] isListColumn) {
        if (node.columnIndex >= 0) {
            int i = node.columnIndex;
            CsvFixtureParser.ColumnSpec col = columns.get(i);
            if (isListColumn[i]) {
                // Reuse list-type construction but force the user-facing leaf name.
                return parquetListType(new CsvFixtureParser.ColumnSpec(name, col.type()));
            }
            return parquetScalarType(col.type()).named(name);
        }
        Types.GroupBuilder<org.apache.parquet.schema.GroupType> builder = Types.optionalGroup();
        for (Map.Entry<String, Node> e : node.children.entrySet()) {
            builder = builder.addField(buildType(e.getKey(), e.getValue(), columns, isListColumn));
        }
        return builder.named(name);
    }

    /** Trie node for grouping dotted CSV columns into nested Parquet groups. */
    private static final class Node {
        /** Index into the columns list when this node is a leaf; -1 for inner nodes. */
        int columnIndex = -1;
        /** Ordered children so the generated schema is deterministic. */
        final LinkedHashMap<String, Node> children = new LinkedHashMap<>();
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

    private static void addValueAt(Group g, String leafName, String type, boolean isList, Object value) {
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
            addListValueAt(g, leafName, type, listValue);
        } else {
            addScalarValueAt(g, leafName, type, value);
        }
    }

    private static void addListValueAt(Group g, String leafName, String type, List<Object> list) {
        if (list == null || list.isEmpty()) {
            return;
        }
        Group listGroup = g.addGroup(leafName);
        for (Object elem : list) {
            Group listElement = listGroup.addGroup("list");
            if (elem != null) {
                appendListElement(listElement, type, elem);
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

    private static void addScalarValueAt(Group g, String leafName, String type, Object value) {
        try {
            switch (type) {
                case "integer", "short", "byte" -> g.add(leafName, ((Number) value).intValue());
                case "long" -> g.add(leafName, ((Number) value).longValue());
                case "double", "scaled_float" -> g.add(leafName, ((Number) value).doubleValue());
                case "float", "half_float" -> g.add(leafName, ((Number) value).floatValue());
                case "boolean" -> g.add(leafName, Boolean.TRUE.equals(value));
                case "date" -> g.add(leafName, ((Number) value).longValue());
                case "ip" -> g.add(leafName, value.toString());
                default -> g.add(leafName, value.toString());
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
