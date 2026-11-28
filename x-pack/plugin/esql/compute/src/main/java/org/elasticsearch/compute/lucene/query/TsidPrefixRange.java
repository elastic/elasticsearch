/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * The {@code _tsid}s whose leading {@code prefixBits} bits are in {@code [lower, upper)}.
 */
public record TsidPrefixRange(int prefixBits, int lower, int upper) {
    public TsidPrefixRange {
        if (prefixBits < 1 || prefixBits >= Integer.SIZE - 1) {
            throw new IllegalArgumentException("prefix bits must be in [1, 30]; got [" + prefixBits + "]");
        }
        if (lower < 0 || lower >= upper || upper > 1 << prefixBits) {
            throw new IllegalArgumentException("invalid range [" + lower + ", " + upper + ") for [" + prefixBits + "] prefix bits");
        }
    }

    /**
     * The docs of a segment in this range: one doc range, as segments are sorted by {@code _tsid}.
     */
    PrimarySortRange.DocRange docRange(LeafReader reader) throws IOException {
        final SortedDocValues tsid = reader.getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
        if (tsid == null) {
            return new PrimarySortRange.DocRange(0, 0);
        }
        final PartitionedDocValues.PrefixPartitions partitions = tsid instanceof PartitionedDocValues partitioned
            ? partitioned.prefixPartitions(null)
            : null;
        if (partitions == null) {
            throw new IllegalStateException("_tsid prefix range [" + this + "] needs prefix partitions in every segment");
        }
        final int partitionBits = ((PartitionedDocValues) tsid).prefixPartitionBits();
        final int startPartition = startPartition(partitions, partitionBits);
        final int endPartition = endPartition(partitions, partitionBits, startPartition);
        final int maxDoc = reader.maxDoc();
        return new PrimarySortRange.DocRange(startDoc(partitions, startPartition, maxDoc), startDoc(partitions, endPartition, maxDoc));
    }

    private static int startDoc(PartitionedDocValues.PrefixPartitions partitions, int partition, int maxDoc) {
        return partition < partitions.numPartitions() ? partitions.startDocs()[partition] : maxDoc;
    }

    private int scale(int prefix, int partitionBits) {
        if (partitionBits < prefixBits) {
            throw new IllegalStateException("_tsid prefix range [" + this + "] is finer than [" + partitionBits + "] partition bits");
        }
        return prefix << (partitionBits - prefixBits);
    }

    /**
     * The first partition that can hold the range.
     */
    int startPartition(PartitionedDocValues.PrefixPartitions partitions, int partitionBits) {
        return firstPartitionAtOrAfter(partitions, 0, scale(lower, partitionBits));
    }

    /**
     * The first partition after the range.
     */
    int endPartition(PartitionedDocValues.PrefixPartitions partitions, int partitionBits, int startPartition) {
        return firstPartitionAtOrAfter(partitions, startPartition, scale(upper, partitionBits));
    }

    private static int firstPartitionAtOrAfter(PartitionedDocValues.PrefixPartitions partitions, int fromPartition, int prefix) {
        final int found = Arrays.binarySearch(partitions.prefixes(), fromPartition, partitions.numPartitions(), prefix);
        return found >= 0 ? found : -found - 1;
    }

    @Override
    public String toString() {
        return lower + ".." + upper + "/" + prefixBits;
    }
}
