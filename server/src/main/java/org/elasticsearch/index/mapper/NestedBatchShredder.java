/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.mapper.ShardBatchMapper.NestedGroupResolution;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper.PackedArray;
import org.elasticsearch.sourcebatch.SourceValueType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Builds the elements of a nested field as a batch of their own, so that the field mappers map them column-major like any other
 * batch.
 *
 * <p>The coordinating node encodes without the mapping. The elements of a nested array arrive as one {@code KEY_VALUE} blob each,
 * packed in one column named after the nested field, and the object spelling ({@code {"n":{"b":1}}}) arrives as the leaves under the
 * object {@code n}. Both are read here and written, one row for each element, through an {@link EscfBatchBuilder}, in the order of the
 * parents and in source order for each parent.
 *
 * <p>What the sequential path does differently from the batch path, or what the batch path cannot see, is refused with a
 * {@link Fallback}: the sequential path then indexes the whole chunk. These are elements that are not objects, objects in elements,
 * a row that spells the nested field two ways, and more elements in a row than {@code index.mapping.nested_objects.limit} allows.
 */
final class NestedBatchShredder {

    private NestedBatchShredder() {}

    /** The chunk cannot be mapped by the batch path, and the sequential path must index it. */
    static final class Fallback extends RuntimeException {
        Fallback(String message) {
            super(message, null, false, false);
        }
    }

    /**
     * The elements of one nested path in one chunk.
     *
     * @param batch one row for each element, which the caller releases
     * @param parentOffsets the elements of document {@code d} are the rows {@code [parentOffsets[d], parentOffsets[d + 1])}
     * @param childToRow the document of the chunk that each element belongs to
     * @param floatingKeys the names of the leaves that got a floating point number from an array element, which a blob stores without
     *                     the way the number was written, so only a numeric field can take it
     */
    record Children(EscfBatch batch, int[] parentOffsets, int[] childToRow, Set<String> floatingKeys) {}

    /**
     * Shreds the values of {@code group} in {@code chunk}.
     *
     * @param childrenPerRow the number of nested documents of each row so far, over the nested paths, which this adds to
     * @param maxChildrenPerRow {@code index.mapping.nested_objects.limit}
     * @return the elements, or {@code null} when no row of the chunk has an element
     * @throws Fallback when the chunk cannot be mapped by the batch path
     */
    static Children shred(
        EscfBatch chunk,
        NestedGroupResolution group,
        Recycler<BytesRef> recycler,
        int[] childrenPerRow,
        long maxChildrenPerRow
    ) {
        final int rows = chunk.docCount();
        final EscfColumn[] valueColumns = columns(chunk, group.valueLeaves());
        final EscfColumn[] objectColumns = columns(chunk, group.objectSpellingLeaves());
        final int[] objectParents = new int[objectColumns.length];
        for (int i = 0; i < objectParents.length; i++) {
            objectParents[i] = chunk.schema().getLeafParent(group.objectSpellingLeaves()[i]);
        }
        final Set<String> floatingKeys = new HashSet<>();
        final int[] parentOffsets = new int[rows + 1];
        int[] childToRow = new int[Math.min(rows, 1024)];
        int childCount = 0;
        try (EscfBatchBuilder builder = new EscfBatchBuilder(recycler)) {
            for (int row = 0; row < rows; row++) {
                parentOffsets[row] = childCount;
                int valueSource = -1;
                for (int i = 0; i < valueColumns.length; i++) {
                    if (valueColumns[i].getTypeByte(row) != SourceValueType.ABSENT) {
                        if (valueSource != -1) {
                            throw new Fallback("a row spells nested field [" + group.mapper().fullPath() + "] more than once");
                        }
                        valueSource = i;
                    }
                }
                int objectParent = -1;
                for (int i = 0; i < objectColumns.length; i++) {
                    if (objectColumns[i].getTypeByte(row) != SourceValueType.ABSENT) {
                        if (objectParent != -1 && objectParent != objectParents[i]) {
                            throw new Fallback("a row spells nested field [" + group.mapper().fullPath() + "] as more than one object");
                        }
                        objectParent = objectParents[i];
                    }
                }
                if (valueSource != -1 && objectParent != -1) {
                    throw new Fallback("a row spells nested field [" + group.mapper().fullPath() + "] as an array and as an object");
                }
                final int children;
                try {
                    if (valueSource != -1) {
                        children = shredValue(builder, valueColumns[valueSource], row, floatingKeys);
                    } else if (objectParent != -1) {
                        shredObject(builder, objectColumns, group.relativeKeys(), row, floatingKeys);
                        children = 1;
                    } else {
                        children = 0;
                    }
                } catch (IllegalArgumentException e) {
                    // EscfRowBuffer rejects a key that an element has twice; the sequential path decides what that means
                    throw new Fallback("nested field [" + group.mapper().fullPath() + "] has an element with a duplicate key");
                }
                childrenPerRow[row] += children;
                if (childrenPerRow[row] > maxChildrenPerRow) {
                    throw new Fallback("a row has more than [" + maxChildrenPerRow + "] nested documents");
                }
                if (childCount + children > childToRow.length) {
                    childToRow = Arrays.copyOf(childToRow, Math.max(childCount + children, childToRow.length * 2));
                }
                Arrays.fill(childToRow, childCount, childCount + children, row);
                childCount += children;
            }
            parentOffsets[rows] = childCount;
            assert contiguousAndInSourceOrder(parentOffsets, childToRow, childCount);
            if (childCount == 0) {
                return null;
            }
            return new Children(builder.buildPartition(0), parentOffsets, Arrays.copyOf(childToRow, childCount), floatingKeys);
        }
    }

    private static EscfColumn[] columns(EscfBatch chunk, int[] leaves) {
        final EscfColumn[] columns = new EscfColumn[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            columns[i] = chunk.column(leaves[i]);
        }
        return columns;
    }

    /**
     * Shreds the value at the nested path: no element for an absent value, an explicit {@code null}, and an empty array, one element
     * for an object, and one for each element of an array of objects.
     */
    private static int shredValue(EscfBatchBuilder builder, EscfColumn column, int row, Set<String> floatingKeys) {
        final byte type = column.getTypeByte(row);
        switch (type) {
            case SourceValueType.NULL -> {
                return 0;
            }
            case SourceValueType.KEY_VALUE -> {
                shredElement(builder, column.getKeyValue(row), floatingKeys);
                return 1;
            }
            case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> {
                final ArrayReader elements = column.getArrayValue(row);
                int children = 0;
                while (elements.next()) {
                    if (elements.type() != SourceValueType.KEY_VALUE) {
                        // a scalar element is a document error on the sequential path, and a null one is skipped there
                        throw new Fallback("an element of a nested field is not an object");
                    }
                    shredElement(builder, elements.nestedKeyValue(), floatingKeys);
                    children++;
                }
                return children;
            }
            case SourceValueType.ABSENT -> {
                return 0;
            }
            case SourceValueType.TRUE, SourceValueType.FALSE, SourceValueType.INT, SourceValueType.FLOAT, SourceValueType.LONG,
                SourceValueType.DOUBLE, SourceValueType.STRING, SourceValueType.BINARY -> throw new Fallback(
                    "the value of a nested field is not an object or an array"
                );
            default -> throw new IllegalStateException("unexpected value type [" + type + "]");
        }
    }

    /** Writes one element, an object of a blob, as a row. */
    private static void shredElement(EscfBatchBuilder builder, KeyValueReader element, Set<String> floatingKeys) {
        final EscfRowBuffer row = builder.beginRow();
        while (element.next()) {
            final String key = element.key();
            final byte type = element.type();
            switch (type) {
                case SourceValueType.NULL -> row.nullField(key);
                case SourceValueType.TRUE -> row.booleanField(key, true);
                case SourceValueType.FALSE -> row.booleanField(key, false);
                case SourceValueType.INT -> row.longField(key, element.intValue());
                case SourceValueType.LONG -> row.longField(key, element.longValue());
                case SourceValueType.FLOAT -> {
                    floatingKeys.add(key);
                    row.doubleField(key, element.floatValue());
                }
                case SourceValueType.DOUBLE -> {
                    floatingKeys.add(key);
                    row.doubleField(key, element.doubleValue());
                }
                case SourceValueType.STRING -> row.stringField(key, element.utf8Value());
                case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> {
                    checkArray(element.nestedArray(), key, floatingKeys);
                    row.arrayField(key, type, element.valueBytes());
                }
                case SourceValueType.KEY_VALUE -> throw new Fallback("an element of a nested field has an object");
                case SourceValueType.BINARY -> throw new Fallback("an element of a nested field has binary data");
                case SourceValueType.ABSENT -> throw new IllegalStateException("an entry of an element is absent");
                default -> throw new IllegalStateException("unexpected value type [" + type + "]");
            }
        }
        row.finishRow();
        builder.commit(0);
    }

    /**
     * Checks an array of an element: an object in it makes extra documents, which the batch path leaves to the sequential path, and a
     * floating point number in it has lost the way it was written.
     */
    private static void checkArray(ArrayReader array, String key, Set<String> floatingKeys) {
        while (array.next()) {
            final byte type = array.type();
            switch (type) {
                case SourceValueType.KEY_VALUE -> throw new Fallback("an array of an element of a nested field has an object");
                case SourceValueType.FLOAT, SourceValueType.DOUBLE -> floatingKeys.add(key);
                case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> checkArray(array.nestedArray(), key, floatingKeys);
                case SourceValueType.NULL, SourceValueType.TRUE, SourceValueType.FALSE, SourceValueType.INT, SourceValueType.LONG,
                    SourceValueType.STRING, SourceValueType.BINARY -> {
                }
                case SourceValueType.ABSENT -> throw new IllegalStateException("an element of an array is absent");
                default -> throw new IllegalStateException("unexpected value type [" + type + "]");
            }
        }
    }

    /**
     * Writes the object spelling, {@code {"n":{"b":1}}}, as one element: the leaves of the object that this row has, under their name
     * relative to the nested path. The values are the typed values of the leaf columns, which keep the way a double was written.
     */
    private static void shredObject(
        EscfBatchBuilder builder,
        EscfColumn[] columns,
        String[] relativeKeys,
        int row,
        Set<String> floatingKeys
    ) {
        final EscfRowBuffer element = builder.beginRow();
        for (int i = 0; i < columns.length; i++) {
            final EscfColumn column = columns[i];
            final String key = relativeKeys[i];
            final byte type = column.getTypeByte(row);
            switch (type) {
                case SourceValueType.ABSENT -> {
                }
                case SourceValueType.NULL -> element.nullField(key);
                case SourceValueType.TRUE -> element.booleanField(key, true);
                case SourceValueType.FALSE -> element.booleanField(key, false);
                case SourceValueType.INT, SourceValueType.LONG -> element.longField(key, column.getLongValue(row));
                case SourceValueType.FLOAT, SourceValueType.DOUBLE -> {
                    if (column.kind() == EscfColumnKind.DOUBLE) {
                        // a column of doubles keeps the spelling of each number, which a keyword field indexes
                        element.doubleField(key, column.getDoubleValue(row), column.getStringValue(row).bytes());
                    } else {
                        // the other columns, a union of types, do not
                        floatingKeys.add(key);
                        element.doubleField(key, column.getDoubleValue(row));
                    }
                }
                case SourceValueType.STRING -> element.stringField(key, column.getStringValue(row).bytes());
                case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> {
                    final PackedArray packed = repack(column.getArrayValue(row), key, floatingKeys);
                    element.arrayField(key, packed.arrayType(), packed.packed());
                }
                case SourceValueType.KEY_VALUE -> throw new Fallback("an object of a nested field has an object");
                case SourceValueType.BINARY -> throw new Fallback("an object of a nested field has binary data");
                default -> throw new IllegalStateException("unexpected value type [" + type + "]");
            }
        }
        element.finishRow();
        builder.commit(0);
    }

    /** Packs an array that a column holds, in the inline format of a blob, which {@link EscfRowBuffer#arrayField} takes. */
    private static PackedArray repack(ArrayReader array, String key, Set<String> floatingKeys) {
        byte[] types = new byte[8];
        long[] numerics = new long[8];
        Object[] vars = new Object[8];
        int count = 0;
        while (array.next()) {
            if (count == types.length) {
                types = Arrays.copyOf(types, count * 2);
                numerics = Arrays.copyOf(numerics, count * 2);
                vars = Arrays.copyOf(vars, count * 2);
            }
            final byte type = array.type();
            types[count] = type;
            switch (type) {
                case SourceValueType.INT -> numerics[count] = array.intValue();
                case SourceValueType.LONG -> numerics[count] = array.longValue();
                case SourceValueType.FLOAT -> {
                    floatingKeys.add(key);
                    numerics[count] = Float.floatToRawIntBits(array.floatValue());
                }
                case SourceValueType.DOUBLE -> {
                    floatingKeys.add(key);
                    numerics[count] = Double.doubleToRawLongBits(array.doubleValue());
                }
                case SourceValueType.STRING -> vars[count] = array.textValue().bytes();
                case SourceValueType.NULL, SourceValueType.TRUE, SourceValueType.FALSE -> {
                }
                case SourceValueType.KEY_VALUE -> throw new Fallback("an array of an object of a nested field has an object");
                case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> throw new Fallback(
                    "an array of an object of a nested field has an array"
                );
                case SourceValueType.BINARY -> throw new Fallback("an array of an object of a nested field has binary data");
                case SourceValueType.ABSENT -> throw new IllegalStateException("an element of an array is absent");
                default -> throw new IllegalStateException("unexpected value type [" + type + "]");
            }
            count++;
        }
        return SourceBatchEncodeHelper.packAccumulatedElements(types, numerics, vars, count, false);
    }

    private static boolean contiguousAndInSourceOrder(int[] parentOffsets, int[] childToRow, int childCount) {
        for (int row = 0; row + 1 < parentOffsets.length; row++) {
            if (parentOffsets[row] > parentOffsets[row + 1]) {
                return false;
            }
            for (int c = parentOffsets[row]; c < parentOffsets[row + 1]; c++) {
                if (childToRow[c] != row) {
                    return false;
                }
            }
        }
        return parentOffsets[parentOffsets.length - 1] == childCount;
    }
}
