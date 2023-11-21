/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.Weight;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;
import java.util.function.Supplier;

/**
 * Holds a list of multiple partial Lucene segments
 */
public record LuceneSlice(int shardIndex, SearchContext searchContext, List<PartialLeafReaderContext> leaves, Supplier<Weight> weight) {

    int numLeaves() {
        return leaves.size();
    }

    PartialLeafReaderContext getLeaf(int index) {
        return leaves.get(index);
    }
}
