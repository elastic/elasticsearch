/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;

/**
 * An extension for doc-values which is the primary sort key, and values are grouped by some prefix bytes. One example is `_tsid` in
 * time-series, where time-series can be grouped by the first few bytes, and the query engine can process a slice of time-series
 * containing all "continuous" time-series sharing the same prefix. This is necessary instead of querying a slice of docs because some
 * aggregations such as rate require all data points from a single time-series to be processed by the same operator.
 */
public interface PartitionedDocValues {
    /**
     * @param numPartitions the actual number of prefixes available in the partition
     * @param prefixes the prefix keys
     * @param startDocs the startDocs of corresponding prefix keys
     */
    record PrefixPartitions(int numPartitions, int[] prefixes, int[] startDocs) {

    }

    /**
     * Returns the prefixed partition from the doc-values of this field if exists.
     * @param reused an existing prefix partitions can be reused to avoid allocating memory
     */
    @Nullable
    PrefixPartitions prefixPartitions(PrefixPartitions reused) throws IOException;

    /**
     * The number of bits used in prefix partitions
     */
    int prefixPartitionBits();

    /**
     * The field the prefix partitions of a time series segment are written for: the primary index sort field, which is {@code _tsid}
     * unless the index uses the grouped sort (see {@code IndexSettings#TIME_SERIES_DIMENSION_GROUP_FIELD}), in which case it is
     * {@code _metric_name_hash} and a partition spans the series of one (hash prefix of a) metric.
     */
    static String partitionedField(LeafReader reader) {
        Sort sort = reader.getMetaData().sort();
        if (sort == null || sort.getSort().length == 0) {
            return TimeSeriesIdFieldMapper.NAME;
        }
        return sort.getSort()[0].getField();
    }

    /**
     * Check if the given index searcher can be partitioned by the prefix of its primary sort field.
     * @param searcher the index searcher to check
     * @return true if all non-empty segments support prefix partitioning
     */
    static boolean canPartitionByTsidPrefix(IndexSearcher searcher) throws IOException {
        for (LeafReaderContext leafContext : searcher.getLeafContexts()) {
            var sortedDV = leafContext.reader().getSortedDocValues(partitionedField(leafContext.reader()));
            // empty segment
            if (sortedDV == null) {
                continue;
            }
            if (sortedDV instanceof PartitionedDocValues partition && partition.prefixPartitionBits() > 0) {
                continue;
            }
            return false;
        }
        return true;
    }
}
