/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.compute.lucene.ShardContext;

import java.util.List;

/**
 * Holds a list of multiple partial Lucene segments
 */
public record LuceneSlice(
    int slicePosition,
    boolean queryHead,
    ShardContext shardContext,
    List<PartialLeafReaderContext> leaves,
    Weight weight,
    List<Object> tags
) {
    int numLeaves() {
        return leaves.size();
    }

    PartialLeafReaderContext getLeaf(int index) {
        return leaves.get(index);
    }
}
