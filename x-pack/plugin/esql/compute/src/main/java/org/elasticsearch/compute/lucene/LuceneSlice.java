/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Holds a list of multiple partial Lucene segments
 */
public record LuceneSlice(
    int slicePosition,
    boolean queryHead,
    ShardContext shardContext,
    List<PartialLeafReaderContext> leaves,
    ScoreMode scoreMode,
    Query query,
    List<Object> tags
) {

    int numLeaves() {
        return leaves.size();
    }

    PartialLeafReaderContext getLeaf(int index) {
        return leaves.get(index);
    }

    Weight createWeight() {
        var searcher = shardContext.searcher();
        try {
            return searcher.createWeight(query, scoreMode, 1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
