/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

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
     * Whether an index with this version has the {@code _tsid} prefix partitions
     */
    static boolean canPartitionByTsidPrefix(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(IndexVersions.WRITE_TSID_PREFIX_PARTITION);
    }
}
