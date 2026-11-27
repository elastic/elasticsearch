/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Parquet's collections: a {@code LIST} whose element is a group. A {@code LIST} of primitives is not one: it is already a
 * multivalued column. A {@code MAP} is not one either. Schema discovery describes them, and {@link ParquetExpansion} reads
 * one at element grain.
 */
final class ParquetCollections {
    private ParquetCollections() {}

    /**
     * A list of structures in a file's schema, whose elements a {@code NESTED_EXPAND} reads as rows and whose leaves it
     * reads as their columns.
     *
     * @param path        the collection's dotted name in the file's schema
     * @param leaves      the element's leaves, by name relative to {@code path}, sorted by name
     * @param unsupported why the collection cannot be read at element grain, or {@code null} when it can. Set when an
     *                    element holds a collection with more than one leaf, whose values would arrive unpaired.
     */
    record Collection(String path, List<Leaf> leaves, @Nullable String unsupported) {
        Collection {
            leaves = List.copyOf(leaves);
        }
    }

    /**
     * One leaf of an element.
     *
     * @param name relative to the collection's path; a structure inside the element is dotted
     * @param type the type it reads as
     */
    record Leaf(String name, DataType type) {}

    /** The collections of {@code schema}, each with its leaves sorted by name. */
    static List<Collection> describe(MessageType schema) {
        List<Collection> out = new ArrayList<>();
        for (Type field : schema.getFields()) {
            describe(field, field.getName(), out);
        }
        return out;
    }

    private static void describe(Type field, String path, List<Collection> out) {
        if (field.isPrimitive() || field.getName().indexOf('.') >= 0) {
            // A collection is named by its dotted path, so one whose own name, or whose structure's name, holds a dot
            // would resolve to another field or to none; it is not described, and stays an unsupported column
            return;
        }
        GroupType group = field.asGroupType();
        GroupType element = element(group);
        if (element != null) {
            out.add(collection(path, element));
            return;
        }
        // A repeated group with no annotation is a list in Parquet's older layout, not a structure: a list of structures below
        // it would have its elements merged across the outer list's entries, so neither it nor anything under it is described
        if (group.getLogicalTypeAnnotation() == null && group.isRepetition(Type.Repetition.REPEATED) == false) {
            for (Type child : group.getFields()) {
                describe(child, path + "." + child.getName(), out);
            }
        }
    }

    /** The element group of a collection, or {@code null} when {@code group} is not one. */
    @Nullable
    private static GroupType element(GroupType group) {
        LogicalTypeAnnotation logical = group.getLogicalTypeAnnotation();
        if (group.getFieldCount() != 1 || group.getType(0).isRepetition(Type.Repetition.REPEATED) == false) {
            return null;
        }
        Type repeated = group.getType(0);
        if (logical instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            Type elementType = ParquetFormatReader.isListElementType(repeated, group.getName())
                ? repeated
                : repeated.asGroupType().getType(0);
            // Only a structure is an element with fields; a list or map as the element is not a list of structures
            if (elementType.isPrimitive() || elementType.getLogicalTypeAnnotation() != null) {
                return null;
            }
            return elementType.asGroupType();
        }
        return null;
    }

    private static Collection collection(String path, GroupType element) {
        List<Leaf> leaves = new ArrayList<>();
        String[] unsupported = new String[1];
        leaves(element, "", path, leaves, unsupported);
        leaves.sort(Comparator.comparing(Leaf::name));
        return new Collection(path, leaves, unsupported[0]);
    }

    /**
     * The element's leaves. A structure's fields are further leaves of the same element. A collection inside the element with
     * one leaf is a multivalued leaf; with more, its values would arrive unpaired, so the whole collection is refused.
     * A collection there is any repeated group, with or without the {@code LIST} annotation: a legacy file repeats a
     * group of fields directly, and its fields would arrive unpaired just the same. A repeated primitive is a
     * multivalued leaf as it stands.
     */
    private static void leaves(GroupType group, String prefix, String path, List<Leaf> out, String[] unsupported) {
        for (Type child : group.getFields()) {
            if (child.getName().indexOf('.') >= 0) {
                // A leaf is named by its dotted path inside the element, so a field whose own name holds a dot would
                // resolve to another field, or to none; it is not offered, as a map or a variant is not
                continue;
            }
            String name = prefix + child.getName();
            if (child.isPrimitive()) {
                DataType type = leafType(child.asPrimitiveType());
                if (type != null) {
                    out.add(new Leaf(name, type));
                }
                continue;
            }
            GroupType childGroup = child.asGroupType();
            LogicalTypeAnnotation logical = childGroup.getLogicalTypeAnnotation();
            if (logical instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
                continue;
            }
            if (logical instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation || childGroup.isRepetition(Type.Repetition.REPEATED)) {
                List<PrimitiveType> inner = new ArrayList<>();
                primitives(childGroup, inner);
                if (inner.size() == 1) {
                    DataType type = leafType(inner.getFirst());
                    if (type != null) {
                        out.add(new Leaf(name, type));
                    }
                } else if (unsupported[0] == null) {
                    unsupported[0] = "["
                        + path
                        + "."
                        + name
                        + "] is a collection with more than one field inside each element of ["
                        + path
                        + "], which cannot be read yet";
                }
                continue;
            }
            leaves(childGroup, name + ".", path, out, unsupported);
        }
    }

    private static void primitives(Type type, List<PrimitiveType> out) {
        if (type.isPrimitive()) {
            out.add(type.asPrimitiveType());
            return;
        }
        for (Type child : type.asGroupType().getFields()) {
            primitives(child, out);
        }
    }

    /** The type a leaf reads as, or {@code null} for a physical type the decode does not convert yet. */
    @Nullable
    static DataType leafType(PrimitiveType primitive) {
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();
        return switch (primitive.getPrimitiveTypeName()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case INT32 -> {
                if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    yield DataType.DATETIME;
                }
                // As a column of the same type: only an unsigned 32-bit value needs a long; 8 and 16 bits fit an integer
                if (logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation i && i.isSigned() == false && i.getBitWidth() == 32) {
                    yield DataType.LONG;
                }
                if (logical == null || logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                    yield DataType.INTEGER;
                }
                yield null;
            }
            case INT64 -> {
                if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
                    yield ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS ? DataType.DATETIME : DataType.DATE_NANOS;
                }
                if (logical == null || logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation i && i.isSigned()) {
                    yield DataType.LONG;
                }
                yield null;
            }
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case BINARY -> logical == null
                || logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation ? DataType.KEYWORD : null;
            case INT96, FIXED_LEN_BYTE_ARRAY -> null;
        };
    }

    /**
     * Where a collection's elements sit in the levels of its leaves: the repetition level that starts a new element, the
     * definition level at which the list has an element at all, and the one at which that element is not null. Shared by
     * every decode of a collection so they cannot disagree about what an element is.
     *
     * @param elementPath the path of the element group, whose descendants are the leaves
     */
    record Shape(Collection collection, String[] elementPath, int collectionRep, int existsDef, int elementDef) {

        /** The leaf named {@code name} relative to the element, or {@code null} when this file's element lacks it. */
        @Nullable
        Leaf leaf(String name) {
            return collection.leaves().stream().filter(c -> c.name().equals(name)).findFirst().orElse(null);
        }

        /** The descriptor of leaf {@code relative} in {@code schema}, which holds the collection at this shape's path. */
        ColumnDescriptor descriptor(MessageType schema, String relative) {
            return ParquetCollections.descriptor(schema, elementPath, relative);
        }
    }

    /**
     * The shape of the collection at {@code path} of {@code schema}, or {@code null} when the schema holds no readable
     * collection there.
     */
    @Nullable
    static Shape shape(MessageType schema, String path) {
        Collection collection = describe(schema).stream().filter(c -> c.path().equals(path)).findFirst().orElse(null);
        if (collection == null) {
            return null;
        }
        String[] parts = path.split("\\.");
        Type node = schema;
        for (String part : parts) {
            node = node.asGroupType().getType(part);
        }
        GroupType group = node.asGroupType();
        GroupType element = element(group);
        String[] repeatedPath = Arrays.copyOf(parts, parts.length + 1);
        repeatedPath[parts.length] = group.getType(0).getName();
        int collectionRep = schema.getMaxRepetitionLevel(repeatedPath);
        int existsDef = schema.getMaxDefinitionLevel(repeatedPath);
        int elementDef = existsDef;
        String[] elementPath = repeatedPath;
        if (element != group.getType(0)) {
            elementPath = Arrays.copyOf(repeatedPath, repeatedPath.length + 1);
            elementPath[repeatedPath.length] = element.getName();
            elementDef = schema.getMaxDefinitionLevel(elementPath);
        }
        return new Shape(collection, elementPath, collectionRep, existsDef, elementDef);
    }

    /** The descriptor of the leaf under the element whose name, relative to the element, is {@code relative}. */
    static ColumnDescriptor descriptor(MessageType schema, String[] elementPath, String relative) {
        String[] relativeParts = relative.split("\\.");
        for (ColumnDescriptor column : schema.getColumns()) {
            String[] columnPath = column.getPath();
            if (columnPath.length < elementPath.length + relativeParts.length
                || Arrays.equals(columnPath, 0, elementPath.length, elementPath, 0, elementPath.length) == false) {
                continue;
            }
            // A leaf that is itself a list holds its value further down; match the named groups only
            int at = elementPath.length;
            boolean matches = true;
            for (String part : relativeParts) {
                if (at >= columnPath.length || columnPath[at].equals(part) == false) {
                    matches = false;
                    break;
                }
                at++;
            }
            if (matches) {
                return column;
            }
        }
        throw new IllegalStateException("no Parquet column for collection leaf [" + relative + "]");
    }
}
