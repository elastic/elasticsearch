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
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 * Build-time generator for ORC fixture files. Converts CSV to ORC.
 * Accepts source CSV path and output ORC path as arguments.
 * Uses {@link CsvFixtureParser} for bracket-aware CSV parsing.
 * Uses correct multi-value handling for list columns.
 */
public final class OrcFixtureGenerator {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private OrcFixtureGenerator() {}

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.out and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: OrcFixtureGenerator <source-csv-path> <output-orc-path>");
            System.exit(1);
        }
        java.nio.file.Path sourcePath = java.nio.file.Path.of(args[0]);
        java.nio.file.Path outputPath = java.nio.file.Path.of(args[1]);
        if (Files.exists(sourcePath) == false) {
            throw new IOException("Source CSV not found: " + sourcePath);
        }
        Files.createDirectories(outputPath.getParent());
        Files.deleteIfExists(outputPath);
        generate(sourcePath, outputPath);
        System.out.println("Generated ORC fixture: " + outputPath);
    }

    /**
     * Generates an ORC file at the given path from the source CSV.
     */
    public static void generate(java.nio.file.Path sourcePath, java.nio.file.Path outputPath) throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(sourcePath);
        List<CsvFixtureParser.ColumnSpec> columns = result.schema();
        List<Object[]> rows = result.rows();

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

        try (Writer writer = OrcFile.createWriter(orcPath, writerOptions)) {
            VectorizedRowBatch batch = schema.createRowBatch(100);
            int[] listChildIndex = new int[columns.size()];
            for (Object[] row : rows) {
                int r = batch.size++;
                for (int i = 0; i < columns.size(); i++) {
                    Object value = i < row.length ? row[i] : null;
                    setOrcValue(batch.cols[i], columns.get(i), isListColumn[i], r, value, listChildIndex, i);
                }
                if (batch.size == batch.getMaxSize()) {
                    setListChildCounts(batch, columns.size(), isListColumn, listChildIndex);
                    writer.addRowBatch(batch);
                    batch.reset();
                    for (int i = 0; i < listChildIndex.length; i++) {
                        listChildIndex[i] = 0;
                    }
                }
            }
            if (batch.size > 0) {
                setListChildCounts(batch, columns.size(), isListColumn, listChildIndex);
                writer.addRowBatch(batch);
            }
        }
    }

    private static TypeDescription buildSchema(List<CsvFixtureParser.ColumnSpec> columns, boolean[] isListColumn) {
        TypeDescription struct = TypeDescription.createStruct();
        for (int i = 0; i < columns.size(); i++) {
            CsvFixtureParser.ColumnSpec col = columns.get(i);
            TypeDescription type = isListColumn[i] ? orcListTypeFor(col.type()) : orcScalarTypeFor(col.type());
            struct.addField(col.name(), type);
        }
        return struct;
    }

    private static TypeDescription orcListTypeFor(String type) {
        return switch (type) {
            case "integer", "short", "byte" -> TypeDescription.createList(TypeDescription.createInt());
            case "long" -> TypeDescription.createList(TypeDescription.createLong());
            case "double", "scaled_float", "float", "half_float" -> TypeDescription.createList(TypeDescription.createDouble());
            case "boolean" -> TypeDescription.createList(TypeDescription.createBoolean());
            case "date" -> TypeDescription.createList(TypeDescription.createTimestampInstant());
            default -> TypeDescription.createList(TypeDescription.createString());
        };
    }

    private static TypeDescription orcScalarTypeFor(String type) {
        return switch (type) {
            case "integer", "short", "byte" -> TypeDescription.createInt();
            case "long" -> TypeDescription.createLong();
            case "double", "scaled_float", "float", "half_float" -> TypeDescription.createDouble();
            case "boolean" -> TypeDescription.createBoolean();
            case "date" -> TypeDescription.createTimestampInstant();
            default -> TypeDescription.createString();
        };
    }

    @SuppressWarnings("unchecked")
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
            setOrcListValue(col, spec, row, (List<Object>) value, listChildIndex, colIndex);
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

    private static void setListChildCounts(VectorizedRowBatch batch, int columnCount, boolean[] isListColumn, int[] listChildIndex) {
        for (int i = 0; i < columnCount; i++) {
            if (isListColumn[i]) {
                ListColumnVector listCol = (ListColumnVector) batch.cols[i];
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
