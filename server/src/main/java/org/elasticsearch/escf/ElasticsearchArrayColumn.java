/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * An ESCF column whose values are all arrays of a single fixed primitive element kind, stored
 * Apache-Arrow {@code List<primitive>} style: a per-row element-range offset vector ({@code offsets})
 * over a single dense primitive {@code child} sub-column. Row {@code d}'s elements are the child
 * elements in {@code [offsets[d], offsets[d + 1])}. There are no inline arrays.
 *
 * <p>The child is itself an {@link ElasticsearchColumn} (a long/double/string column over the
 * flattened element space, with no validity), so element access reuses the primitive column getters.
 * Every row reports {@link SourceValueType#FIXED_ARRAY} since elements are homogeneous.
 */
final class ElasticsearchArrayColumn extends ElasticsearchColumn {

    private final ElasticsearchColumn child;
    private final int[] rowOffsets;

    ElasticsearchArrayColumn(int docCount, FixedBitSet absent, ElasticsearchColumn child, int[] rowOffsets) {
        super(docCount, absent);
        this.child = child;
        this.rowOffsets = rowOffsets;
    }

    /** Reconstructs an array column from its serialized {@code child_kind(1) | child_values} data field. */
    static ElasticsearchArrayColumn fromData(int docCount, FixedBitSet absent, ElasticsearchColumnData col) {
        int[] rowOffsets = col.offsets();
        int totalElems = rowOffsets[docCount];
        BytesReference d = col.data();
        byte childKind = d.get(0);
        // Everything past the child-kind prefix byte; a paged slice only re-bases, so this stays pooled.
        BytesReference childData = d.slice(1, d.length() - 1);
        ElasticsearchColumn child = switch (childKind) {
            case ElasticsearchColumnKind.LONG -> new ElasticsearchLongColumn(totalElems, null, childData);
            case ElasticsearchColumnKind.DOUBLE -> new ElasticsearchDoubleColumn(totalElems, null, childData);
            case ElasticsearchColumnKind.STRING -> {
                int[] childOffsets = new int[totalElems + 1];
                for (int i = 0; i <= totalElems; i++) {
                    childOffsets[i] = childData.getIntLE(i * 4);
                }
                int childDataBase = (totalElems + 1) * 4;
                yield new ElasticsearchStringColumn(
                    totalElems,
                    null,
                    childData.slice(childDataBase, childData.length() - childDataBase),
                    childOffsets
                );
            }
            default -> throw new IllegalStateException("Unsupported ESCF array child kind: " + ElasticsearchColumnKind.name(childKind));
        };
        return new ElasticsearchArrayColumn(docCount, absent, child, rowOffsets);
    }

    @Override
    byte kind() {
        return ElasticsearchColumnKind.ARRAY;
    }

    @Override
    byte typeByteForPresent(int d) {
        return SourceValueType.FIXED_ARRAY;
    }

    @Override
    ArrayReader getArrayValue(int d) {
        return new ElasticsearchArrayReader(child, rowOffsets[d], rowOffsets[d + 1]);
    }
}
