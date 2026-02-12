/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;

public interface PartitionedDocValues {
    int[] partitionStartDocs() throws IOException;

    boolean startDocPartitionsAvailable();

    static boolean tsidPartitionsByPrefix(IndexSearcher searcher) throws IOException {
        for (LeafReaderContext leafContext : searcher.getLeafContexts()) {
            var sortedDV = leafContext.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
            if (sortedDV == null || sortedDV instanceof PartitionedDocValues partition && partition.startDocPartitionsAvailable()) {
                continue;
            }
            return false;
        }
        return true;
    }
}
