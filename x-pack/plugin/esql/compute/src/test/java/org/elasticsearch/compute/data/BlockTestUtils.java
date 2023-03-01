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

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomLong;

public class BlockTestUtils {
    public record Doc(int shard, int segment, int doc) {}

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
            case DOC -> new Doc(randomInt(), randomInt(), between(0, Integer.MAX_VALUE));
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
        } else if (builder instanceof DocBlock.Builder b && value instanceof Doc v) {
            b.appendShard(v.shard).appendSegment(v.segment).appendDoc(v.doc);
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
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                values.add(null);
            } else if (block instanceof IntBlock b) {
                values.add(b.getInt(i));
            } else if (block instanceof LongBlock b) {
                values.add(b.getLong(i));
            } else if (block instanceof DoubleBlock b) {
                values.add(b.getDouble(i));
            } else if (block instanceof BytesRefBlock b) {
                values.add(b.getBytesRef(i, new BytesRef()));
            } else if (block instanceof BooleanBlock b) {
                values.add(b.getBoolean(i));
            } else if (block instanceof DocBlock b) {
                DocVector v = b.asVector();
                values.add(new Doc(v.shards().getInt(i), v.segments().getInt(i), v.docs().getInt(i)));
            } else {
                throw new IllegalArgumentException("can't read values from [" + block + "]");
            }
        }
    }
}
