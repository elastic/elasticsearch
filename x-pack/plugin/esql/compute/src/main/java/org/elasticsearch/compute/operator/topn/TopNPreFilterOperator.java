/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.sort.LongTopNSet;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

public final class TopNPreFilterOperator extends AbstractPageMappingOperator {
    public record Factory(ElementType elementType, int channel, boolean asc, boolean nullsFirst, int limit) implements OperatorFactory {
        public Factory {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be positive; got [" + limit + "]");
            }
            if (elementType != ElementType.LONG && elementType != ElementType.NULL) {
                throw new UnsupportedOperationException(TopNPreFilterOperator.class.getSimpleName() + " doesn't support " + elementType);
            }
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TopNPreFilterOperator(driverContext, channel, asc, nullsFirst, limit);
        }

        @Override
        public String describe() {
            return "TopNPreFilterOperator[channel=" + channel + ", asc=" + asc + ", nullsFirst=" + nullsFirst + ", limit=" + limit + "]";
        }
    }

    private final int channel;
    private final boolean asc;
    private final boolean nullsFirst;
    private final int limit;
    private final LongTopNPreFilter preFilter;

    TopNPreFilterOperator(DriverContext driverContext, int channel, boolean asc, boolean nullsFirst, int limit) {
        this.channel = channel;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.limit = limit;
        this.preFilter = new LongTopNPreFilter(driverContext.bigArrays(), driverContext.breaker(), asc, nullsFirst, limit);
    }

    @Override
    protected Page process(Page page) {
        return preFilter.process(page, channel);
    }

    @Override
    public String toString() {
        return "TopNPreFilterOperator[channel=" + channel + ", asc=" + asc + ", nullsFirst=" + nullsFirst + ", limit=" + limit + "]";
    }

    @Override
    public void close() {
        Releasables.close(preFilter, super::close);
    }

    static final class LongTopNPreFilter implements Releasable {
        private final CircuitBreaker breaker;
        private final boolean asc;
        private final boolean nullsFirst;
        private final int limit;
        private final LongTopNSet topValues;
        private int[] positions = new int[0];
        private long usedBytes;

        LongTopNPreFilter(BigArrays bigArrays, CircuitBreaker breaker, boolean asc, boolean nullsFirst, int limit) {
            this.breaker = breaker;
            this.asc = asc;
            this.nullsFirst = nullsFirst;
            this.limit = limit;
            this.topValues = new LongTopNSet(bigArrays, asc ? SortOrder.ASC : SortOrder.DESC, limit);
        }

        Page process(Page page, int channel) {
            try {
                LongBlock block = page.getBlock(channel);
                LongVector vector = block.asVector();
                if (vector != null) {
                    collectVector(vector);
                } else {
                    collectBlock(block);
                }
                if (topValues.getCount() < limit) {
                    return page.shallowCopy();
                }
                final long bottom = topValues.getWorstValue();
                final int positionCount = page.getPositionCount();
                ensurePositionsCapacity(positionCount);
                int selected = vector != null ? filterVector(vector, bottom) : filterBlock(block, bottom);
                if (selected == 0) {
                    return null;
                }
                if (selected == positionCount) {
                    return page.shallowCopy();
                }
                return page.filter(false, positions, 0, selected);
            } finally {
                page.releaseBlocks();
            }
        }

        private void collectVector(LongVector vector) {
            int positionCount = vector.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                topValues.collect(vector.getLong(p));
            }
        }

        private void collectBlock(LongBlock block) {
            if (block.areAllValuesNull()) {
                return;
            }
            int positionCount = block.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                for (int i = 0; i < count; i++) {
                    topValues.collect(block.getLong(first + i));
                }
            }
        }

        private int filterVector(LongVector vector, long bottom) {
            int positionCount = vector.getPositionCount();
            int selected = 0;
            if (asc) {
                for (int p = 0; p < positionCount; p++) {
                    if (vector.getLong(p) <= bottom) {
                        positions[selected++] = p;
                    }
                }
            } else {
                for (int p = 0; p < positionCount; p++) {
                    if (vector.getLong(p) >= bottom) {
                        positions[selected++] = p;
                    }
                }
            }
            return selected;
        }

        private int filterBlock(LongBlock block, long bottom) {
            int positionCount = block.getPositionCount();
            if (block.areAllValuesNull()) {
                if (nullsFirst == false) {
                    return 0;
                }
                for (int p = 0; p < positionCount; p++) {
                    positions[p] = p;
                }
                return positionCount;
            }
            int selected = 0;
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                if (count == 0) {
                    if (nullsFirst) {
                        positions[selected++] = p;
                    }
                    continue;
                }
                int first = block.getFirstValueIndex(p);
                int end = first + count;
                for (int i = first; i < end; i++) {
                    final long value = block.getLong(i);
                    final boolean competitive = asc ? value <= bottom : value >= bottom;
                    if (competitive) {
                        positions[selected++] = p;
                        break;
                    }
                }
            }
            return selected;
        }

        private void ensurePositionsCapacity(int size) {
            if (positions.length >= size) {
                return;
            }
            int newSize = ArrayUtil.oversize(size, Integer.BYTES);
            long newBytes = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) newSize * Integer.BYTES);
            breaker.addEstimateBytesAndMaybeBreak(newBytes, "top_n_pre_filter");
            positions = new int[newSize];
            breaker.addWithoutBreaking(-usedBytes, "top_n_pre_filter");
            usedBytes = newBytes;
        }

        @Override
        public void close() {
            Releasables.close(topValues, () -> breaker.addWithoutBreaking(-usedBytes));
        }
    }
}
