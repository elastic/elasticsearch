/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.index.mapper.BlockLoader;

abstract class StubBlockFactory implements BlockLoader.BlockFactory {

    @Override
    public void adjustBreaker(long delta) throws CircuitBreakingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.BooleanBuilder booleansFromDocValues(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.BooleanBuilder booleans(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.BytesRefBuilder bytesRefsFromDocValues(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.BytesRefBuilder bytesRefs(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.SingletonBytesRefBuilder singletonBytesRefs(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.DoubleBuilder doublesFromDocValues(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.DoubleBuilder doubles(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.FloatBuilder denseVectors(int expectedVectorsCount, int dimensions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.IntBuilder intsFromDocValues(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.IntBuilder ints(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.LongBuilder longsFromDocValues(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.LongBuilder longs(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.SingletonLongBuilder singletonLongs(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.SingletonIntBuilder singletonInts(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.SingletonDoubleBuilder singletonDoubles(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Builder nulls(int expectedCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Block constantNulls(int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Block constantBytes(BytesRef value, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Block constantInt(int value, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count, boolean isDense) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.SortedSetOrdinalsBuilder sortedSetOrdinalsBuilder(SortedSetDocValues ordinals, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Block buildAggregateMetricDoubleDirect(
        BlockLoader.Block minBlock,
        BlockLoader.Block maxBlock,
        BlockLoader.Block sumBlock,
        BlockLoader.Block countBlock
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.ExponentialHistogramBuilder exponentialHistogramBlockBuilder(int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Block buildExponentialHistogramBlockDirect(
        BlockLoader.Block minima,
        BlockLoader.Block maxima,
        BlockLoader.Block sums,
        BlockLoader.Block valueCounts,
        BlockLoader.Block zeroThresholds,
        BlockLoader.Block encodedHistograms
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Block buildTDigestBlockDirect(
        BlockLoader.Block encodedDigests,
        BlockLoader.Block minima,
        BlockLoader.Block maxima,
        BlockLoader.Block sums,
        BlockLoader.Block valueCounts
    ) {
        throw new UnsupportedOperationException();
    }
}
