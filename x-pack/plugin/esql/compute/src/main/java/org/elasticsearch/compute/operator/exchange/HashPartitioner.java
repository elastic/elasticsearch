/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.Objects;

/**
 * Assigns rows to stable hash partitions so equal keys from different producers reach the same consumer.
 */
public final class HashPartitioner {

    private static final long SEED = 0x5f3759dfL;
    private static final long NULL_HASH = 0x9E3779B9L;

    private final int[] keyChannels;
    private final ElementType[] keyTypes;
    private final int partitionCount;
    private final BytesRef scratch = new BytesRef();

    public HashPartitioner(int[] keyChannels, ElementType[] keyTypes, int partitionCount) {
        this.keyChannels = Objects.requireNonNull(keyChannels).clone();
        this.keyTypes = Objects.requireNonNull(keyTypes).clone();
        if (keyChannels.length == 0) {
            throw new IllegalArgumentException("key channels must not be empty");
        }
        if (keyChannels.length != keyTypes.length) {
            throw new IllegalArgumentException("key channel and type counts must match");
        }
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partition count must be positive");
        }
        for (int keyChannel : keyChannels) {
            if (keyChannel < 0) {
                throw new IllegalArgumentException("key channels must not be negative");
            }
        }
        for (ElementType keyType : keyTypes) {
            Objects.requireNonNull(keyType);
            switch (keyType) {
                case BOOLEAN, INT, LONG, DOUBLE, BYTES_REF, NULL -> {
                }
                default -> throw unsupportedType(keyType);
            }
        }
        this.partitionCount = partitionCount;
    }

    public int partitionCount() {
        return partitionCount;
    }

    /** Fills {@code partitionOut[0..page.getPositionCount())} with each row's partition. */
    public void partition(Page page, int[] partitionOut) {
        Objects.requireNonNull(page);
        Objects.requireNonNull(partitionOut);
        int positionCount = page.getPositionCount();
        if (partitionOut.length < positionCount) {
            throw new IllegalArgumentException(
                "partition output has [" + partitionOut.length + "] slots but page has [" + positionCount + "] positions"
            );
        }
        for (int keyChannel : keyChannels) {
            if (keyChannel >= page.getBlockCount()) {
                throw new IllegalArgumentException("key channel [" + keyChannel + "] is outside the page");
            }
        }

        for (int position = 0; position < positionCount; position++) {
            long hash = 0;
            for (int i = 0; i < keyChannels.length; i++) {
                Block keyBlock = page.getBlock(keyChannels[i]);
                ElementType blockType = keyBlock.elementType();
                if (blockType != keyTypes[i] && blockType != ElementType.NULL) {
                    throw new IllegalArgumentException("expected shuffle key type [" + keyTypes[i] + "] but was [" + blockType + "]");
                }
                hash = 31 * hash + hashValue(keyBlock, position, scratch);
            }
            partitionOut[position] = (int) Math.floorMod(hash, (long) partitionCount);
        }
    }

    static long hashValue(Block block, int position, BytesRef scratch) {
        ElementType elementType = block.elementType();
        if (elementType == ElementType.NULL || block.isNull(position)) {
            return NULL_HASH;
        }
        int valueIndex = block.getFirstValueIndex(position);
        return switch (elementType) {
            case BOOLEAN -> MurmurHash3.fmix((((BooleanBlock) block).getBoolean(valueIndex) ? 1L : 0L) ^ SEED);
            case INT -> MurmurHash3.fmix(((long) ((IntBlock) block).getInt(valueIndex)) ^ SEED);
            case LONG -> MurmurHash3.fmix(((LongBlock) block).getLong(valueIndex) ^ SEED);
            case DOUBLE -> {
                double value = ((DoubleBlock) block).getDouble(valueIndex);
                if (value == 0.0d) {
                    value = 0.0d;
                }
                yield MurmurHash3.fmix(Double.doubleToLongBits(value) ^ SEED);
            }
            case BYTES_REF -> {
                BytesRef value = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
                long hash = Murmur3HashFunction.hash(value.bytes, value.offset, value.length);
                yield MurmurHash3.fmix(hash ^ SEED);
            }
            case NULL -> NULL_HASH;
            default -> throw unsupportedType(elementType);
        };
    }

    private static IllegalArgumentException unsupportedType(ElementType type) {
        return new IllegalArgumentException("unsupported shuffle key type [" + type + "]");
    }
}
