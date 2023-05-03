/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomLong;

public class BlockTestUtils {
    /**
     * Generate a random value of the appropriate type to fit into blocks of {@code e}.
     */
    public static Object randomValue(ElementType e) {
        return switch (e) {
            case INT -> randomInt();
            case LONG -> randomLong();
            case DOUBLE -> randomDouble();
            case BYTES_REF -> new BytesRef(randomAlphaOfLength(5));
            case BOOLEAN -> randomBoolean();
            case DOC -> new BlockUtils.Doc(randomInt(), randomInt(), between(0, Integer.MAX_VALUE));
            case NULL -> null;
            case UNKNOWN -> throw new IllegalArgumentException("can't make random values for [" + e + "]");
        };
    }

    /**
     * Append {@code value} to {@code builder} or throw an
     * {@link IllegalArgumentException} if the types don't line up.
     */
    public static void append(Block.Builder builder, Object value) {
        if (value == null) {
            builder.appendNull();
        } else if (builder instanceof IntBlock.Builder b && value instanceof Integer v) {
            b.appendInt(v);
        } else if (builder instanceof LongBlock.Builder b && value instanceof Long v) {
            b.appendLong(v);
        } else if (builder instanceof DoubleBlock.Builder b && value instanceof Double v) {
            b.appendDouble(v);
        } else if (builder instanceof BytesRefBlock.Builder b && value instanceof BytesRef v) {
            b.appendBytesRef(v);
        } else if (builder instanceof BooleanBlock.Builder b && value instanceof Boolean v) {
            b.appendBoolean(v);
        } else if (builder instanceof DocBlock.Builder b && value instanceof BlockUtils.Doc v) {
            b.appendShard(v.shard()).appendSegment(v.segment()).appendDoc(v.doc());
        } else {
            throw new IllegalArgumentException("Can't append [" + value + "/" + value.getClass() + "] to [" + builder + "]");
        }
    }

    public static void readInto(List<List<Object>> values, Page page) {
        if (values.isEmpty()) {
            while (values.size() < page.getBlockCount()) {
                values.add(new ArrayList<>());
            }
        } else {
            if (values.size() != page.getBlockCount()) {
                throw new IllegalArgumentException("Can't load values from pages with different numbers of blocks");
            }
        }
        for (int i = 0; i < page.getBlockCount(); i++) {
            readInto(values.get(i), page.getBlock(i));
        }
    }

    public static void readInto(List<Object> values, Block block) {
        for (int p = 0; p < block.getPositionCount(); p++) {
            values.add(toJavaObject(block, p));
        }
    }
}
