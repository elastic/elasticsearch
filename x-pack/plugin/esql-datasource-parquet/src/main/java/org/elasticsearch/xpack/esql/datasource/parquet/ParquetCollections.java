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
import org.apache.parquet.schema.Type;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Parquet's collections: a {@code LIST} whose element is a group. A {@code LIST} of primitives is not one: it is already a
 * multivalued column. A {@code MAP} is not one either. Schema discovery describes them, and {@link ParquetExpansion} reads
 * one at element grain.
 */
final class ParquetCollections {
    private ParquetCollections() {}

    /**
     * A list of structures in a file's schema, whose elements a {@code NESTED_EXPAND} reads as rows and whose leaves it
     * reads as their columns. A collection inside another's element is one of its own, at its dotted path, as an index's
     * {@code nested} field inside another is: it is no leaf of the outer one.
     *
     * @param path        the collection's dotted name in the file's schema
     * @param leaves      the element's leaves, by name relative to {@code path}, sorted by name
     * @param unsupported why the collection cannot be read at element grain, or {@code null} when it can. Set when an
     *                    element holds more fields than the reader's field cap.
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
        return describe(schema, name -> {}, Integer.MAX_VALUE);
    }

    /**
     * As {@link #describe(MessageType)}, telling {@code charge} the name of every field it visits inside an element, so the
     * walk is charged as the column walk is. A collection whose element holds more than {@code maxLeaves} fields cannot be
     * read: it is described with no more of them, and says so.
     */
    static List<Collection> describe(MessageType schema, Consumer<String> charge, int maxLeaves) {
        List<Collection> out = new ArrayList<>();
        Walk walk = new Walk(charge, maxLeaves, out);
        for (Type field : schema.getFields()) {
            describe(field, field.getName(), out, walk);
        }
        return out;
    }

    /**
     * What every element walk shares: the charge for each field visited, the cap on the fields of one element, and the
     * collections described, to which a collection inside an element is added too.
     */
    private record Walk(Consumer<String> charge, int maxLeaves, List<Collection> collections) {}

    private static void describe(Type field, String path, List<Collection> out, Walk walk) {
        if (field.isPrimitive() || field.getName().indexOf('.') >= 0) {
            // A collection is named by its dotted path, so one whose own name, or whose structure's name, holds a dot
            // would resolve to another field or to none; it is not described, and stays an unsupported column
            return;
        }
        GroupType group = field.asGroupType();
        GroupType element = element(group);
        if (element != null) {
            out.add(collection(path, element, walk));
            return;
        }
        // A repeated group with no annotation is a list in Parquet's older layout, not a structure: a list of structures below
        // it would have its elements merged across the outer list's entries, so neither it nor anything under it is described
        if (group.getLogicalTypeAnnotation() == null && group.isRepetition(Type.Repetition.REPEATED) == false) {
            for (Type child : group.getFields()) {
                describe(child, path + "." + child.getName(), out, walk);
            }
        }
    }

    /**
     * The element group of a collection, or {@code null} when {@code group} is not one. A list that is itself repeated is
     * not one: its elements would be merged across the entries of the repetition around it. This is the rule for a field
     * of the schema; inside an element, see {@link #innerElement}.
     */
    @Nullable
    private static GroupType element(GroupType group) {
        return group.isRepetition(Type.Repetition.REPEATED) ? null : structureElement(group);
    }

    /** The element of {@code list} when it is a {@code LIST} of structures, or {@code null}. */
    @Nullable
    private static GroupType structureElement(GroupType list) {
        Type elementType = ParquetFormatReader.listElementOf(list);
        // Only a structure is an element with fields; a list or map as the element is not a list of structures
        if (elementType == null || elementType.isPrimitive() || elementType.getLogicalTypeAnnotation() != null) {
            return null;
        }
        return elementType.asGroupType();
    }

    private static Collection collection(String path, GroupType element, Walk walk) {
        List<Leaf> leaves = new ArrayList<>();
        String[] unsupported = new String[1];
        leaves(element, "", path, leaves, unsupported, walk);
        leaves.sort(Comparator.comparing(Leaf::name));
        return new Collection(path, leaves, unsupported[0]);
    }

    /**
     * The element's leaves. A structure's fields are further leaves of the same element, and every other field is one leaf,
     * typed as the same field is as a column ({@link ParquetFormatReader#convertParquetTypeToEsql}): a list of primitives
     * and a repeated primitive are multivalued leaves, and a field the reader cannot read as a column, a map, a variant or
     * a list of lists or of maps among them, is an unsupported leaf rather than a missing one.
     * <p>
     * A collection inside the element, a {@code LIST} of structures or a repeated group with or without fields of its own
     * (the older layout of a list), is no leaf: it is described as a collection of its own at its dotted path, as an
     * index's {@code nested} field inside another is, and a {@code NESTED_EXPAND} of the outer one does not read it.
     */
    private static void leaves(GroupType group, String prefix, String path, List<Leaf> out, String[] unsupported, Walk walk) {
        for (Type child : group.getFields()) {
            walk.charge().accept(child.getName());
            if (out.size() >= walk.maxLeaves()) {
                if (unsupported[0] == null) {
                    unsupported[0] = "["
                        + path
                        + "] holds more than ["
                        + walk.maxLeaves()
                        + "] fields in each element, which cannot be read";
                }
                return;
            }
            if (child.getName().indexOf('.') >= 0) {
                // A leaf is named by its dotted path inside the element, so a field whose own name holds a dot would
                // resolve to another field, or to none; it is not offered
                continue;
            }
            String name = prefix + child.getName();
            if (child.isPrimitive()) {
                out.add(new Leaf(name, ParquetFormatReader.convertParquetTypeToEsql(child)));
                continue;
            }
            GroupType childGroup = child.asGroupType();
            LogicalTypeAnnotation logical = childGroup.getLogicalTypeAnnotation();
            if (logical instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
                // An unsupported column, as the same field is outside a list: reading it gives nulls
                out.add(new Leaf(name, DataType.UNSUPPORTED));
                continue;
            }
            if (logical instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation || childGroup.isRepetition(Type.Repetition.REPEATED)) {
                GroupType inner = innerElement(childGroup);
                if (inner == null) {
                    // A list of primitives is a multivalued leaf; a list of lists or of maps is an unsupported one
                    out.add(new Leaf(name, ParquetFormatReader.convertParquetTypeToEsql(childGroup)));
                } else {
                    walk.collections().add(collection(path + "." + name, inner, walk));
                }
                continue;
            }
            leaves(childGroup, name + ".", path, out, unsupported, walk);
        }
    }

    /**
     * The element group of a list inside an element, or {@code null} when it holds no structures: a repeated group without
     * an annotation is its own element, the older layout of a list, and a {@code LIST} has one when its element is a
     * structure.
     */
    @Nullable
    private static GroupType innerElement(GroupType list) {
        if (list.getLogicalTypeAnnotation() == null && list.isRepetition(Type.Repetition.REPEATED)) {
            return list;
        }
        return structureElement(list);
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
     * collection there. The collection is described as discovery describes it, with no more than {@code maxLeaves} fields
     * in an element, so one discovery could not read says so in {@link Collection#unsupported()} here too.
     */
    @Nullable
    static Shape shape(MessageType schema, String path, int maxLeaves) {
        Collection collection = describe(schema, name -> {}, maxLeaves).stream()
            .filter(c -> c.path().equals(path))
            .findFirst()
            .orElse(null);
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
