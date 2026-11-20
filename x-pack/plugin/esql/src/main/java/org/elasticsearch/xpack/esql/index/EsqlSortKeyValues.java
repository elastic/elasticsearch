/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

/**
 * The sort keys of a segment's documents, read from the chunks of a {@link DocValuesExpressionReader} as longs that sort like
 * the keys: doubles as {@link NumericUtils#doubleToSortableLong sortable longs} and booleans as {@code 0} and {@code 1}. A
 * document has a key when the expression returns a single value for it, and takes the missing value otherwise.
 * <p>
 * A sort that fails leaves a chunk unreleased, which is fine since its block doesn't account for memory.
 */
final class EsqlSortKeyValues {
    private final DocValuesExpressionReader reader;
    private final DataType type;
    private final long missing;

    EsqlSortKeyValues(DocValuesExpressionReader reader, DataType type, long missing) {
        this.reader = reader;
        this.type = type;
        this.missing = missing;
    }

    /**
     * Writes the keys of the next chunk of documents to {@code keys} from {@code offset}.
     *
     * @return the number of documents in the chunk, or {@code 0} once every document is read
     */
    int next(long[] keys, int offset) throws IOException {
        try (Block block = reader.next()) {
            if (block == null) {
                return 0;
            }
            int count = block.getPositionCount();
            switch (type) {
                case LONG, DATETIME, DATE_NANOS -> {
                    LongBlock b = (LongBlock) block;
                    for (int p = 0; p < count; p++) {
                        keys[offset + p] = hasKey(b, p) ? b.getLong(b.getFirstValueIndex(p)) : missing;
                    }
                }
                case INTEGER -> {
                    IntBlock b = (IntBlock) block;
                    for (int p = 0; p < count; p++) {
                        keys[offset + p] = hasKey(b, p) ? b.getInt(b.getFirstValueIndex(p)) : missing;
                    }
                }
                case DOUBLE -> {
                    DoubleBlock b = (DoubleBlock) block;
                    for (int p = 0; p < count; p++) {
                        keys[offset + p] = hasKey(b, p) ? NumericUtils.doubleToSortableLong(b.getDouble(b.getFirstValueIndex(p))) : missing;
                    }
                }
                case BOOLEAN -> {
                    BooleanBlock b = (BooleanBlock) block;
                    for (int p = 0; p < count; p++) {
                        keys[offset + p] = hasKey(b, p) ? (b.getBoolean(b.getFirstValueIndex(p)) ? 1 : 0) : missing;
                    }
                }
                default -> throw new IllegalStateException("unsupported key type [" + type + "]");
            }
            return count;
        }
    }

    private static boolean hasKey(Block block, int position) {
        return block.getValueCount(position) == 1;
    }
}
