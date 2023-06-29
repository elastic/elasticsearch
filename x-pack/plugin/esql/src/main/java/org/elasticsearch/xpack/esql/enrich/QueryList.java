/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/**
 * Generates a list of Lucene queries based on the input block.
 */
abstract class QueryList {
    protected final Block block;

    protected QueryList(Block block) {
        this.block = block;
    }

    /**
     * Returns the number of positions in this query list
     */
    int getPositionCount() {
        return block.getPositionCount();
    }

    /**
     * Returns the query at the given position.
     */
    @Nullable
    abstract Query getQuery(int position);

    /**
     * Returns a list of term queries for the given field and the input block.
     */
    static QueryList termQueryList(MappedFieldType field, SearchExecutionContext searchExecutionContext, Block block) {
        return new QueryList(block) {
            private final IntFunction<Object> blockValueReader = QueryList.blockToJavaObject(block);

            @Override
            Query getQuery(int position) {
                final int first = block.getFirstValueIndex(position);
                final int count = block.getValueCount(position);
                return switch (count) {
                    case 0 -> null;
                    case 1 -> field.termQuery(blockValueReader.apply(first), searchExecutionContext);
                    default -> {
                        final List<Object> terms = new ArrayList<>(count);
                        for (int i = 0; i < count; i++) {
                            final Object value = blockValueReader.apply(first + i);
                            terms.add(value);
                        }
                        yield field.termsQuery(terms, searchExecutionContext);
                    }
                };
            }
        };
    }

    private static IntFunction<Object> blockToJavaObject(Block block) {
        return switch (block.elementType()) {
            case BOOLEAN -> {
                BooleanBlock booleanBlock = (BooleanBlock) block;
                yield booleanBlock::getBoolean;
            }
            case BYTES_REF -> {
                BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
                yield offset -> bytesRefBlock.getBytesRef(offset, new BytesRef());
            }
            case DOUBLE -> {
                DoubleBlock doubleBlock = ((DoubleBlock) block);
                yield doubleBlock::getDouble;
            }
            case INT -> {
                IntBlock intBlock = (IntBlock) block;
                yield intBlock::getInt;
            }
            case LONG -> {
                LongBlock longBlock = (LongBlock) block;
                yield longBlock::getLong;
            }
            case NULL -> offset -> null;
            case DOC -> throw new UnsupportedOperationException("can't read values from doc block");
            case UNKNOWN -> throw new IllegalArgumentException("can't read values from [" + block + "]");
        };
    }
}
