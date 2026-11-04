/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.DocValuesType;
import org.apache.parquet.format.Type;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Final index settings (private once the mount API sets them) that mark an index as backed by a Parquet file, in the way
 * {@code index.store.snapshot.*} marks a searchable snapshot. Users cannot set them directly once the mount API exists. A check on
 * {@link #REPOSITORY} selects the read-only engine.
 *
 * <p>Spike: the file and the columns to serve come from settings. The plan moves this to the mount API, which derives the fields from
 * the schema of the file (A1b, A2b).
 */
public final class ParquetBackedSettings {

    /** The repository that holds the file, which also marks the index as Parquet-backed. */
    public static final Setting<String> REPOSITORY = Setting.simpleString(
        "index.parquet.repository",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * The row groups of files that the index holds, in the order of their rows. Each entry is {@code path#firstRowGroup+count}, where
     * the path is below the base path of the repository with {@code /} between the segments, for example
     * {@code events/2026/part-0001.parquet#0+12}. A mount splits its files over several indices at row-group borders, so an index may
     * hold only a range of the row groups of a file.
     */
    public static final Setting<List<String>> PIECES = Setting.listSetting(
        "index.parquet.pieces",
        List.of(),
        Function.identity(),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /** The fields that the files serve, as {@code field:column} or {@code field:column:conversion} entries separated by commas. */
    public static final Setting<String> FIELDS = Setting.simpleString(
        "index.parquet.fields",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    private ParquetBackedSettings() {}

    public static List<Setting<?>> all() {
        return List.of(REPOSITORY, PIECES, FIELDS);
    }

    public static boolean isParquetBacked(Settings indexSettings) {
        return REPOSITORY.exists(indexSettings);
    }

    /** A range of the row groups of one file, which is a piece of the index. */
    record PieceSpec(String path, int firstRowGroup, int rowGroupCount) {
        public RepositoryLocation location(String repository) {
            return new RepositoryLocation(repository, path);
        }

        @Override
        public String toString() {
            return path + "#" + firstRowGroup + "+" + rowGroupCount;
        }
    }

    /** The pieces of the settings, in order. */
    static List<PieceSpec> pieceSpecs(Settings indexSettings) {
        List<PieceSpec> specs = new ArrayList<>();
        for (String entry : PIECES.get(indexSettings)) {
            int hash = entry.lastIndexOf('#');
            int plus = entry.lastIndexOf('+');
            if (hash < 1 || plus < hash + 2 || plus == entry.length() - 1) {
                throw new IllegalArgumentException(
                    "[" + PIECES.getKey() + "] must hold path#firstRowGroup+count entries, found [" + entry + "]"
                );
            }
            try {
                int first = Integer.parseInt(entry.substring(hash + 1, plus));
                int count = Integer.parseInt(entry.substring(plus + 1));
                if (first < 0 || count < 1) {
                    throw new NumberFormatException();
                }
                specs.add(new PieceSpec(entry.substring(0, hash), first, count));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("[" + PIECES.getKey() + "] has an invalid row group range in [" + entry + "]");
            }
        }
        if (specs.isEmpty()) {
            throw new IllegalArgumentException("[" + PIECES.getKey() + "] must name at least one piece");
        }
        return specs;
    }

    /** A field of the index and the column that serves it. */
    record FieldSpec(String field, String column, Conversion conversion) {}

    /**
     * The fields of the settings, in order. Each is {@code field:column}, or {@code field:column:conversion} when the values of the
     * column need a {@link Conversion}.
     */
    static List<FieldSpec> fieldSpecs(Settings indexSettings) {
        List<FieldSpec> specs = new ArrayList<>();
        for (String entry : FIELDS.get(indexSettings).split(",")) {
            String[] parts = entry.trim().split(":");
            if (parts.length < 2 || parts.length > 3 || parts[0].isEmpty() || parts[1].isEmpty()) {
                throw new IllegalArgumentException(
                    "[" + FIELDS.getKey() + "] must hold field:column or field:column:conversion entries, found [" + entry + "]"
                );
            }
            specs.add(new FieldSpec(parts[0], parts[1], parts.length == 3 ? Conversion.fromExternalName(parts[2]) : Conversion.NONE));
        }
        return specs;
    }

    /**
     * The bindings of the fields to the columns of the file, and the synthesized {@code _id} when the index is columnar.
     *
     * @throws IllegalArgumentException if a column is missing, nested or repeated, or has a type that the format does not serve yet
     */
    static List<ColumnBinding> bindings(Settings indexSettings, ParquetFileInfo file) {
        List<ColumnBinding> bindings = new ArrayList<>();
        int number = 0;
        for (FieldSpec spec : fieldSpecs(indexSettings)) {
            ParquetFileInfo.LeafColumn column = file.column(spec.column());
            if (column == null) {
                throw new IllegalArgumentException("[" + file.location() + "] has no column [" + spec.column() + "]");
            }
            if (column.flat() == false || column.repeated()) {
                throw new IllegalArgumentException("column [" + column.path() + "] is nested or repeated, which is not supported yet");
            }
            DocValuesType type = docValuesType(column);
            if (spec.conversion() != Conversion.NONE && type != DocValuesType.NUMERIC) {
                throw new IllegalArgumentException("the conversion [" + spec.conversion().externalName() + "] needs an integer column");
            }
            bindings.add(new ColumnBinding(spec.field(), number++, column.path(), type, spec.conversion()));
        }
        if (IndexSettings.MODE.get(indexSettings).isColumnar()) {
            bindings.add(ColumnBinding.synthesizedId(number));
        }
        return bindings;
    }

    private static DocValuesType docValuesType(ParquetFileInfo.LeafColumn column) {
        if (column.physicalType() == Type.INT32.getValue() || column.physicalType() == Type.INT64.getValue()) {
            return DocValuesType.NUMERIC;
        }
        if (column.physicalType() == Type.BYTE_ARRAY.getValue()) {
            return DocValuesType.BINARY;
        }
        throw new IllegalArgumentException(
            "column [" + column.path() + "] has the physical type [" + Type.findByValue(column.physicalType()) + "], not supported yet"
        );
    }
}
