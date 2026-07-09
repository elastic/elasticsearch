/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser;
import org.elasticsearch.xpack.esql.datasource.csv.SplitPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Build-time generator for ORC fixture files. Converts CSV to ORC.
 * <p>
 * Single-file mode: {@code <source-csv> <output.orc>}
 * <br>
 * Split mode:       {@code <source-csv> <output-dir> <num-parts>} — writes
 * {@code <basename>_00.orc}, {@code <basename>_01.orc}, … into the directory. Used by the
 * shared {@code external-multifile*.csv-spec} tests which glob {@code multifile_split/*.orc}.
 * <p>
 * Uses {@link CsvFixtureParser} for bracket-aware CSV parsing and correct multi-value handling
 * for list columns.
 */
public final class OrcFixtureGenerator {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private OrcFixtureGenerator() {}

    private static final Logger logger = LoggerFactory.getLogger(OrcFixtureGenerator.class);

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.err and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length == 2) {
            java.nio.file.Path sourcePath = java.nio.file.Path.of(args[0]);
            java.nio.file.Path outputPath = java.nio.file.Path.of(args[1]);
            if (Files.exists(sourcePath) == false) {
                throw new IOException("Source CSV not found: " + sourcePath);
            }
            Files.createDirectories(outputPath.getParent());
            Files.deleteIfExists(outputPath);
            generate(sourcePath, outputPath);
            logger.info("Generated ORC fixture: {}", outputPath);
        } else if (args.length == 3) {
            java.nio.file.Path sourcePath = java.nio.file.Path.of(args[0]);
            java.nio.file.Path outputDir = java.nio.file.Path.of(args[1]);
            int numParts = Integer.parseInt(args[2]);
            if (Files.exists(sourcePath) == false) {
                throw new IOException("Source CSV not found: " + sourcePath);
            }
            Files.createDirectories(outputDir);
            CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(sourcePath);
            int total = result.rows().size();
            String baseName = sourcePath.getFileName().toString().replaceFirst("\\.csv$", "");
            for (int part = 0; part < numParts; part++) {
                SplitPartitioner.Range range = SplitPartitioner.partitionRange(total, numParts, part);
                if (range == null) {
                    break;
                }
                String fileName = String.format(Locale.ROOT, "%s_%02d.orc", baseName, part);
                java.nio.file.Path outputPath = outputDir.resolve(fileName);
                Files.deleteIfExists(outputPath);
                generateFromRows(result, range.from(), range.to(), outputPath);
                logger.info("Generated ORC split fixture: {} (rows {}-{})", outputPath, range.from(), range.to());
            }
        } else {
            System.err.println("Usage: OrcFixtureGenerator <source-csv-path> <output-orc-path>");
            System.err.println("       OrcFixtureGenerator <source-csv-path> <output-dir> <num-parts>");
            System.exit(1);
        }
    }

    /**
     * Generates an ORC file at the given path from the source CSV.
     */
    public static void generate(java.nio.file.Path sourcePath, java.nio.file.Path outputPath) throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(sourcePath);
        generateFromRows(result, 0, result.rows().size(), outputPath);
    }

    private static void generateFromRows(CsvFixtureParser.CsvFixtureResult result, int from, int to, java.nio.file.Path outputPath)
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

        TypeDescription schema = buildSchema(columns, isListColumn);
        Configuration conf = new Configuration(false);
        conf.set("orc.dictionary.key.threshold", "0");
        NoChmodFileSystem localFs = new NoChmodFileSystem();
        localFs.setConf(conf);

        Path orcPath = new Path(outputPath.toUri());
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(localFs)
            .compress(CompressionKind.NONE);

        // Precompute per-column child-index paths (top-level fields have a 1-element path);
        // used at write-time to navigate StructColumnVector.fields[]. Flat (literal-dot)
        // columns resolve to a single top-level index even if their name contains dots.
        boolean[] flatten = CsvFixtureParser.computeFlatten(columns);
        int[][] columnVectorPaths = new int[columns.size()][];
        for (int i = 0; i < columns.size(); i++) {
            if (flatten[i]) {
                int idx = schema.getFieldNames().indexOf(columns.get(i).name());
                if (idx < 0) {
                    throw new IllegalStateException("Could not locate flat column [" + columns.get(i).name() + "] in ORC schema");
                }
                columnVectorPaths[i] = new int[] { idx };
            } else {
                columnVectorPaths[i] = resolveVectorPath(schema, columns.get(i).name());
            }
        }

        try (Writer writer = OrcFile.createWriter(orcPath, writerOptions)) {
            VectorizedRowBatch batch = schema.createRowBatch(100);
            int[] listChildIndex = new int[columns.size()];
            for (Object[] row : rows) {
                int r = batch.size++;
                for (int i = 0; i < columns.size(); i++) {
                    Object value = i < row.length ? row[i] : null;
                    ColumnVector leaf = navigateToLeaf(batch, columnVectorPaths[i]);
                    setOrcValue(leaf, columns.get(i), isListColumn[i], r, value, listChildIndex, i);
                }
                if (batch.size == batch.getMaxSize()) {
                    setListChildCounts(batch, columns.size(), columnVectorPaths, isListColumn, listChildIndex);
                    writer.addRowBatch(batch);
                    batch.reset();
                    for (int i = 0; i < listChildIndex.length; i++) {
                        listChildIndex[i] = 0;
                    }
                }
            }
            if (batch.size > 0) {
                setListChildCounts(batch, columns.size(), columnVectorPaths, isListColumn, listChildIndex);
                writer.addRowBatch(batch);
            }
        }
    }

    /**
     * Resolves a dotted column name to the root-to-leaf path of child indices within the ORC schema.
     * Traversal stops at the first non-STRUCT type, so LIST/MAP children are not descended into.
     */
    private static int[] resolveVectorPath(TypeDescription schema, String dottedName) {
        String[] segments = dottedName.split("\\.");
        int[] path = new int[segments.length];
        TypeDescription current = schema;
        for (int i = 0; i < segments.length; i++) {
            if (current.getCategory() != TypeDescription.Category.STRUCT) {
                throw new IllegalStateException(
                    "Cannot resolve segment [" + segments[i] + "] of [" + dottedName + "]: parent is not a STRUCT"
                );
            }
            List<String> names = current.getFieldNames();
            path[i] = names.indexOf(segments[i]);
            if (path[i] < 0) {
                throw new IllegalStateException("Could not resolve segment [" + segments[i] + "] of [" + dottedName + "] in ORC schema");
            }
            current = current.getChildren().get(path[i]);
        }
        return path;
    }

    private static ColumnVector navigateToLeaf(VectorizedRowBatch batch, int[] path) {
        ColumnVector vector = batch.cols[path[0]];
        for (int i = 1; i < path.length; i++) {
            vector = ((StructColumnVector) vector).fields[path[i]];
        }
        return vector;
    }

    /**
     * Builds an ORC schema from the CSV columns. CSV headers containing a literal {@code .}
     * (e.g. {@code event.action:STRING}) are interpreted as paths into nested STRUCT types —
     * columns sharing a prefix are grouped under a single struct field, <b>unless</b> that
     * prefix already exists as a separate top-level column in the same CSV (e.g.
     * {@code languages} + {@code languages.long}). In that case the dotted children remain
     * flat to preserve backward compatibility with existing fixtures.
     */
    private static TypeDescription buildSchema(List<CsvFixtureParser.ColumnSpec> columns, boolean[] isListColumn) {
        boolean[] flatten = CsvFixtureParser.computeFlatten(columns);
        SchemaNode root = new SchemaNode();
        for (int i = 0; i < columns.size(); i++) {
            if (flatten[i]) {
                SchemaNode leaf = new SchemaNode();
                leaf.columnIndex = i;
                root.children.put(columns.get(i).name(), leaf);
                continue;
            }
            String[] segments = columns.get(i).name().split("\\.");
            SchemaNode current = root;
            for (int s = 0; s < segments.length - 1; s++) {
                current = current.children.computeIfAbsent(segments[s], k -> new SchemaNode());
            }
            SchemaNode leaf = new SchemaNode();
            leaf.columnIndex = i;
            current.children.put(segments[segments.length - 1], leaf);
        }
        TypeDescription struct = TypeDescription.createStruct();
        for (Map.Entry<String, SchemaNode> e : root.children.entrySet()) {
            struct.addField(e.getKey(), buildType(e.getValue(), columns, isListColumn));
        }
        return struct;
    }

    private static TypeDescription buildType(SchemaNode node, List<CsvFixtureParser.ColumnSpec> columns, boolean[] isListColumn) {
        if (node.columnIndex >= 0) {
            int i = node.columnIndex;
            CsvFixtureParser.ColumnSpec col = columns.get(i);
            return isListColumn[i] ? orcListTypeFor(col.type()) : orcScalarTypeFor(col.type());
        }
        TypeDescription struct = TypeDescription.createStruct();
        for (Map.Entry<String, SchemaNode> e : node.children.entrySet()) {
            struct.addField(e.getKey(), buildType(e.getValue(), columns, isListColumn));
        }
        return struct;
    }

    /** Trie node for grouping dotted CSV columns into nested ORC struct fields. */
    private static final class SchemaNode {
        int columnIndex = -1;
        final LinkedHashMap<String, SchemaNode> children = new LinkedHashMap<>();
    }

    private static TypeDescription orcListTypeFor(String type) {
        return switch (type) {
            case "integer", "short", "byte" -> TypeDescription.createList(TypeDescription.createInt());
            case "long" -> TypeDescription.createList(TypeDescription.createLong());
            case "double", "scaled_float", "float", "half_float" -> TypeDescription.createList(TypeDescription.createDouble());
            case "boolean" -> TypeDescription.createList(TypeDescription.createBoolean());
            case "date" -> TypeDescription.createList(TypeDescription.createTimestampInstant());
            case "text", "txt" -> TypeDescription.createList(TypeDescription.createString());
            default -> TypeDescription.createList(orcKeywordStringType());
        };
    }

    private static TypeDescription orcScalarTypeFor(String type) {
        return switch (type) {
            case "integer", "short", "byte" -> TypeDescription.createInt();
            case "long" -> TypeDescription.createLong();
            case "double", "scaled_float", "float", "half_float" -> TypeDescription.createDouble();
            case "boolean" -> TypeDescription.createBoolean();
            case "date" -> TypeDescription.createTimestampInstant();
            case "text", "txt" -> TypeDescription.createString();
            default -> orcKeywordStringType();
        };
    }

    private static TypeDescription orcKeywordStringType() {
        return TypeDescription.createVarchar().withMaxLength(65535);
    }

    private static void setOrcValue(
        Object col,
        CsvFixtureParser.ColumnSpec spec,
        boolean isList,
        int row,
        Object value,
        int[] listChildIndex,
        int colIndex
    ) {
        if (value == null) {
            setNull(col, row, isList);
            return;
        }
        if (isList) {
            // Some rows may be scalar (e.g. plain author) while others use bracket multi-values.
            List<Object> listValue;
            if (value instanceof List<?> lv) {
                @SuppressWarnings("unchecked")
                List<Object> cast = (List<Object>) lv;
                listValue = cast;
            } else {
                listValue = List.of(value);
            }
            setOrcListValue(col, spec, row, listValue, listChildIndex, colIndex);
        } else {
            setOrcScalarValue(col, spec, row, value);
        }
    }

    private static void setOrcListValue(
        Object col,
        CsvFixtureParser.ColumnSpec spec,
        int row,
        List<Object> list,
        int[] listChildIndex,
        int colIndex
    ) {
        ListColumnVector listCol = (ListColumnVector) col;
        if (list == null || list.isEmpty()) {
            listCol.noNulls = false;
            listCol.isNull[row] = true;
            listCol.offsets[row] = 0;
            listCol.lengths[row] = 0;
            return;
        }
        int start = listChildIndex[colIndex];
        listCol.offsets[row] = start;
        listCol.lengths[row] = list.size();
        listCol.child.ensureSize(start + list.size(), true);
        listChildIndex[colIndex] = start + list.size();

        Object child = listCol.child;
        String type = spec.type();
        for (int i = 0; i < list.size(); i++) {
            Object elem = list.get(i);
            int idx = start + i;
            if (elem == null) {
                if (child instanceof LongColumnVector l) {
                    l.noNulls = false;
                    l.isNull[idx] = true;
                } else if (child instanceof DoubleColumnVector d) {
                    d.noNulls = false;
                    d.isNull[idx] = true;
                } else if (child instanceof BytesColumnVector b) {
                    b.setRef(idx, EMPTY_BYTES, 0, 0);
                } else if (child instanceof TimestampColumnVector t) {
                    t.noNulls = false;
                    t.isNull[idx] = true;
                }
            } else {
                appendListChild(child, type, idx, elem);
            }
        }
    }

    private static void appendListChild(Object child, String type, int idx, Object elem) {
        switch (type) {
            case "integer", "short", "byte" -> ((LongColumnVector) child).vector[idx] = ((Number) elem).intValue();
            case "long" -> ((LongColumnVector) child).vector[idx] = ((Number) elem).longValue();
            case "double", "scaled_float", "float", "half_float" -> ((DoubleColumnVector) child).vector[idx] = ((Number) elem)
                .doubleValue();
            case "boolean" -> ((LongColumnVector) child).vector[idx] = Boolean.TRUE.equals(elem) ? 1 : 0;
            case "date" -> {
                TimestampColumnVector t = (TimestampColumnVector) child;
                long millis = ((Number) elem).longValue();
                t.time[idx] = millis;
                t.nanos[idx] = 0;
            }
            default -> {
                byte[] bytes = elem.toString().getBytes(StandardCharsets.UTF_8);
                ((BytesColumnVector) child).setRef(idx, bytes, 0, bytes.length);
            }
        }
    }

    private static void setOrcScalarValue(Object col, CsvFixtureParser.ColumnSpec spec, int row, Object value) {
        try {
            switch (spec.type()) {
                case "integer", "short", "byte" -> setLong((LongColumnVector) col, row, ((Number) value).intValue());
                case "long" -> setLong((LongColumnVector) col, row, ((Number) value).longValue());
                case "double", "scaled_float", "float", "half_float" -> setDouble(
                    (DoubleColumnVector) col,
                    row,
                    ((Number) value).doubleValue()
                );
                case "boolean" -> setLong((LongColumnVector) col, row, Boolean.TRUE.equals(value) ? 1 : 0);
                case "date" -> setTimestamp((TimestampColumnVector) col, row, ((Number) value).longValue());
                default -> setString((BytesColumnVector) col, row, value.toString());
            }
        } catch (Exception e) {
            setNull(col, row, false);
        }
    }

    private static void setListChildCounts(
        VectorizedRowBatch batch,
        int columnCount,
        int[][] columnVectorPaths,
        boolean[] isListColumn,
        int[] listChildIndex
    ) {
        for (int i = 0; i < columnCount; i++) {
            if (isListColumn[i]) {
                ListColumnVector listCol = (ListColumnVector) navigateToLeaf(batch, columnVectorPaths[i]);
                listCol.childCount = listChildIndex[i];
                if (listCol.child instanceof BytesColumnVector b) {
                    for (int j = 0; j < listChildIndex[i]; j++) {
                        if (b.vector[j] == null) {
                            b.setRef(j, EMPTY_BYTES, 0, 0);
                        }
                    }
                }
            }
        }
    }

    private static void setNull(Object col, int row, boolean isList) {
        if (isList) {
            ListColumnVector listCol = (ListColumnVector) col;
            listCol.noNulls = false;
            listCol.isNull[row] = true;
            listCol.offsets[row] = 0;
            listCol.lengths[row] = 0;
        } else if (col instanceof LongColumnVector l) {
            l.noNulls = false;
            l.isNull[row] = true;
        } else if (col instanceof DoubleColumnVector d) {
            d.noNulls = false;
            d.isNull[row] = true;
        } else if (col instanceof BytesColumnVector b) {
            b.noNulls = false;
            b.isNull[row] = true;
        } else if (col instanceof TimestampColumnVector t) {
            t.noNulls = false;
            t.isNull[row] = true;
        }
    }

    private static void setLong(LongColumnVector col, int row, long value) {
        col.vector[row] = value;
    }

    private static void setDouble(DoubleColumnVector col, int row, double value) {
        col.vector[row] = value;
    }

    private static void setString(BytesColumnVector col, int row, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        col.setRef(row, bytes, 0, bytes.length);
    }

    private static void setTimestamp(TimestampColumnVector col, int row, long millis) {
        col.time[row] = millis;
        col.nanos[row] = 0;
    }

    /**
     * Minimal FileSystem that skips chmod calls blocked by Elasticsearch's entitlement system.
     */
    private static class NoChmodFileSystem extends RawLocalFileSystem {
        @Override
        public void setPermission(Path p, FsPermission permission) {
            // no-op
        }
    }
}
