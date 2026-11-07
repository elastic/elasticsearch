/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.apache.lucene.index.DocValuesType;
import org.apache.parquet.format.Type;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.parquet.docvalues.Conversion;
import org.elasticsearch.xpack.parquet.docvalues.LogicalKind;
import org.elasticsearch.xpack.parquet.docvalues.ParquetFileInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derives the mapping of an index from the schema of a Parquet file (A2b of the plan). The mapping must be complete before the segments
 * exist, because no document goes through the document parser, and it must agree with the conversion of each field: the field type
 * decides what its doc values hold. Every field has {@code index: false} and keeps its values only in doc values, and a string is a
 * {@code keyword} with {@code multi_value: false}, which is a single value as raw bytes in the binary doc values of a columnar index.
 *
 * <p>The request can override the result for a column: leave it out, rename it, choose another field type, or give the conversion of
 * its values. A column that the mapping does not support yet is reported as unmapped with the reason, and never silently dropped.
 */
public final class ParquetMappingDeriver {

    /** The metadata fields that a column must not be named like. */
    private static final String RESERVED_PREFIX = "_";

    /** What the request says about one column. Every part is optional. */
    public record ColumnOverride(@Nullable String name, @Nullable String type, @Nullable Conversion conversion) {
        public static final ColumnOverride NONE = new ColumnOverride(null, null, null);
    }

    /**
     * @param columns overrides by the dotted path of the column
     * @param exclude the columns that are left out of the mapping
     * @param timestampColumn the column that becomes {@code @timestamp}, or {@code null}
     */
    public record Overrides(Map<String, ColumnOverride> columns, Set<String> exclude, @Nullable String timestampColumn) {
        public static final Overrides NONE = new Overrides(Map.of(), Set.of(), null);
    }

    /** A field of the index and the column that serves it. */
    public record DerivedField(String name, String column, String type, Conversion conversion, boolean required) {

        /** How the segment serves the field. */
        public DocValuesType docValuesType() {
            return type.equals("keyword") ? DocValuesType.BINARY : DocValuesType.NUMERIC;
        }
    }

    /** A column that has no field, with the reason. */
    public record UnmappedColumn(String column, String reason) {}

    /** The fields, in the order of the columns, and the columns that have none. */
    public record DerivedMapping(List<DerivedField> fields, List<UnmappedColumn> unmapped) {

        /** The {@code properties} of the mapping. */
        public Map<String, Object> properties() {
            Map<String, Object> properties = new LinkedHashMap<>();
            for (DerivedField field : fields) {
                Map<String, Object> definition = new LinkedHashMap<>();
                definition.put("type", field.type());
                definition.put("index", false);
                if (field.type().equals("keyword")) {
                    // a single value is stored as its bytes, which is what the column holds (A8 of the plan)
                    definition.put("multi_value", false);
                }
                properties.put(field.name(), definition);
            }
            return properties;
        }

        /** The value of the {@code index.parquet.fields} setting: {@code field:column} or {@code field:column:conversion}. */
        public String fieldsSetting() {
            List<String> entries = new ArrayList<>();
            for (DerivedField field : fields) {
                entries.add(
                    field.conversion() == Conversion.NONE
                        ? field.name() + ":" + field.column()
                        : field.name() + ":" + field.column() + ":" + field.conversion().externalName()
                );
            }
            return String.join(",", entries);
        }
    }

    private record DefaultType(String type, Conversion conversion) {}

    private ParquetMappingDeriver() {}

    public static DerivedMapping derive(ParquetFileInfo file, Overrides overrides) {
        for (String column : overrides.columns().keySet()) {
            requireColumn(file, column, "an override");
        }
        for (String column : overrides.exclude()) {
            requireColumn(file, column, "an exclusion");
        }
        if (overrides.timestampColumn() != null) {
            requireColumn(file, overrides.timestampColumn(), "timestamp_field");
        }

        List<DerivedField> fields = new ArrayList<>();
        List<UnmappedColumn> unmapped = new ArrayList<>();
        Map<String, String> columnByName = new HashMap<>();
        for (ParquetFileInfo.LeafColumn column : file.columns()) {
            if (overrides.exclude().contains(column.path())) {
                continue;
            }
            String reason = unsupportedReason(column);
            if (reason != null) {
                unmapped.add(new UnmappedColumn(column.path(), reason));
                continue;
            }
            ColumnOverride override = overrides.columns().getOrDefault(column.path(), ColumnOverride.NONE);
            DefaultType defaults = defaultType(column);
            if (defaults == null) {
                unmapped.add(new UnmappedColumn(column.path(), "its type [" + describe(column) + "] is not supported yet"));
                continue;
            }
            String type = override.type() != null ? override.type() : defaults.type();
            Conversion conversion = override.conversion() != null
                ? override.conversion()
                : (override.type() != null ? Conversion.NONE : defaults.conversion());
            validate(column, type, conversion);
            String name = column.path().equals(overrides.timestampColumn())
                ? "@timestamp"
                : (override.name() != null ? override.name() : column.path());
            if (name.startsWith(RESERVED_PREFIX)) {
                unmapped.add(new UnmappedColumn(column.path(), "a field cannot be named [" + name + "], give the column another name"));
                continue;
            }
            String clash = columnByName.put(name, column.path());
            if (clash != null) {
                throw new IllegalArgumentException(
                    "the columns [" + clash + "] and [" + column.path() + "] both map to the field [" + name + "]"
                );
            }
            fields.add(new DerivedField(name, column.path(), type, conversion, column.required()));
        }
        if (overrides.timestampColumn() != null) {
            DerivedField timestamp = fields.stream()
                .filter(f -> f.name().equals("@timestamp"))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("the timestamp column [" + overrides.timestampColumn() + "] has no field"));
            if (timestamp.type().equals("date") == false && timestamp.type().equals("date_nanos") == false) {
                throw new IllegalArgumentException(
                    "the timestamp column ["
                        + overrides.timestampColumn()
                        + "] maps as ["
                        + timestamp.type()
                        + "], so it needs a type override of date or date_nanos, with the conversion of its values"
                );
            }
        }
        return new DerivedMapping(List.copyOf(fields), List.copyOf(unmapped));
    }

    private static void requireColumn(ParquetFileInfo file, String column, String what) {
        if (file.column(column) == null) {
            throw new IllegalArgumentException(what + " names the column [" + column + "], which [" + file.location() + "] does not have");
        }
    }

    /** Nested and repeated columns, which no field type serves yet. */
    private static String unsupportedReason(ParquetFileInfo.LeafColumn column) {
        if (column.flat() == false) {
            return "it is inside a struct, list or map, which is not supported yet";
        }
        if (column.repeated()) {
            return "it is repeated, which is not supported yet";
        }
        return null;
    }

    private static String describe(ParquetFileInfo.LeafColumn column) {
        String physical = String.valueOf(Type.findByValue(column.physicalType()));
        return column.logical() == LogicalKind.NONE ? physical : physical + " " + column.logical();
    }

    /** The default of the type table in A2b, for the types that the format serves. {@code null} for the other types. */
    private static DefaultType defaultType(ParquetFileInfo.LeafColumn column) {
        LogicalKind logical = column.logical();
        if (column.physicalType() == Type.INT32.getValue()) {
            return switch (logical) {
                case NONE, INT_8, INT_16, INT_32, UINT_8, UINT_16 -> new DefaultType("integer", Conversion.NONE);
                case UINT_32 -> new DefaultType("long", Conversion.NONE);
                case DATE -> new DefaultType("date", Conversion.DAYS_TO_MILLIS);
                default -> null;
            };
        }
        if (column.physicalType() == Type.INT64.getValue()) {
            return switch (logical) {
                case NONE, INT_64 -> new DefaultType("long", Conversion.NONE);
                case TIMESTAMP_MILLIS -> new DefaultType("date", Conversion.NONE);
                case TIMESTAMP_MICROS -> new DefaultType("date_nanos", Conversion.MICROS_TO_NANOS);
                case TIMESTAMP_NANOS -> new DefaultType("date_nanos", Conversion.NONE);
                default -> null;
            };
        }
        if (column.physicalType() == Type.BYTE_ARRAY.getValue()) {
            return switch (logical) {
                case NONE, STRING, ENUM, JSON -> new DefaultType("keyword", Conversion.NONE);
                default -> null;
            };
        }
        return null;
    }

    /** A field type and a conversion that the column and the format can serve. */
    private static void validate(ParquetFileInfo.LeafColumn column, String type, Conversion conversion) {
        Set<String> allowed;
        if (column.physicalType() == Type.INT32.getValue()) {
            allowed = Set.of("integer", "long", "short", "byte", "date");
        } else if (column.physicalType() == Type.INT64.getValue()) {
            allowed = Set.of("long", "date", "date_nanos");
        } else {
            allowed = Set.of("keyword");
        }
        if (allowed.contains(type) == false) {
            throw new IllegalArgumentException(
                "column ["
                    + column.path()
                    + "] ("
                    + describe(column)
                    + ") cannot be mapped as ["
                    + type
                    + "], it can be mapped as "
                    + allowed.stream().sorted().toList()
            );
        }
        if (conversion != Conversion.NONE && type.equals("keyword")) {
            throw new IllegalArgumentException("the conversion [" + conversion.externalName() + "] needs an integer column");
        }
        boolean toDate = conversion == Conversion.SECONDS_TO_MILLIS
            || conversion == Conversion.DAYS_TO_MILLIS
            || conversion == Conversion.MICROS_TO_MILLIS;
        if (toDate && type.equals("date") == false || conversion == Conversion.MICROS_TO_NANOS && type.equals("date_nanos") == false) {
            throw new IllegalArgumentException(
                "the conversion ["
                    + conversion.externalName()
                    + "] of column ["
                    + column.path()
                    + "] does not give the values of a ["
                    + type
                    + "] field"
            );
        }
    }
}
