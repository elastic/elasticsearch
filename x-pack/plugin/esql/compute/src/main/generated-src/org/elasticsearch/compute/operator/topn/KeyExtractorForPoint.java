/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.PointBlock;
import org.elasticsearch.compute.data.PointVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

abstract class KeyExtractorForPoint implements KeyExtractor {
    static KeyExtractorForPoint extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, PointBlock block) {
        PointVector v = block.asVector();
        if (v != null) {
            return new KeyExtractorForPoint.ForVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorForPoint.MinForAscending(encoder, nul, nonNul, block)
                : new KeyExtractorForPoint.MinForUnordered(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorForPoint.MaxForAscending(encoder, nul, nonNul, block)
            : new KeyExtractorForPoint.MaxForUnordered(encoder, nul, nonNul, block);
    }

    private final byte nul;
    private final byte nonNul;

    KeyExtractorForPoint(TopNEncoder encoder, byte nul, byte nonNul) {
        assert encoder == TopNEncoder.DEFAULT_SORTABLE;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final int nonNul(BreakingBytesRefBuilder key, double x, double y) {
        key.append(nonNul);
        TopNEncoder.DEFAULT_SORTABLE.encodePoint(x, y, key);
        return 16 + 1;
    }

    protected final int nul(BreakingBytesRefBuilder key) {
        key.append(nul);
        return 1;
    }

    static class ForVector extends KeyExtractorForPoint {
        private final PointVector vector;

        ForVector(TopNEncoder encoder, byte nul, byte nonNul, PointVector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            return nonNul(key, vector.getX(position), vector.getY(position));
        }
    }

    static class MinForAscending extends KeyExtractorForPoint {
        private final PointBlock block;

        MinForAscending(TopNEncoder encoder, byte nul, byte nonNul, PointBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            int index = block.getFirstValueIndex(position);
            return nonNul(key, block.getX(index), block.getY(index));
        }
    }

    static class MaxForAscending extends KeyExtractorForPoint {
        private final PointBlock block;

        MaxForAscending(TopNEncoder encoder, byte nul, byte nonNul, PointBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            int index = block.getFirstValueIndex(position) + block.getValueCount(position) - 1;
            return nonNul(key, block.getX(index), block.getY(index));
        }
    }

    static class MinForUnordered extends KeyExtractorForPoint {
        private final PointBlock block;

        MinForUnordered(TopNEncoder encoder, byte nul, byte nonNul, PointBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                return nul(key);
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            double minX = block.getX(start);
            double minY = block.getY(start);
            for (int i = start + 1; i < end; i++) {
                double x = block.getX(i);
                double y = block.getY(i);
                if (x < minX) {
                    minX = x;
                }
                if (y < minY) {
                    minY = y;
                }
            }
            return nonNul(key, minX, minY);
        }
    }

    static class MaxForUnordered extends KeyExtractorForPoint {
        private final PointBlock block;

        MaxForUnordered(TopNEncoder encoder, byte nul, byte nonNul, PointBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                return nul(key);
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            double maxX = block.getX(start);
            double maxY = block.getY(start);
            for (int i = start + 1; i < end; i++) {
                double x = block.getX(i);
                double y = block.getY(i);
                if (x > maxX) {
                    maxX = x;
                }
                if (y > maxY) {
                    maxY = y;
                }
            }
            return nonNul(key, maxX, maxY);
        }
    }
}
