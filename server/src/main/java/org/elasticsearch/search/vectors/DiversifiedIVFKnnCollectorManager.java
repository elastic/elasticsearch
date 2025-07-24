/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.BitSet;

import java.io.IOException;

public class DiversifiedIVFKnnCollectorManager implements KnnCollectorManager {
    private final int k;
    private final BitSetProducer parentsFilter;

    DiversifiedIVFKnnCollectorManager(int k, BitSetProducer parentsFilter) {
        this.k = k;
        this.parentsFilter = parentsFilter;
    }

    @Override
    public KnnCollector newCollector(int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context) throws IOException {
        BitSet parentBitSet = parentsFilter.getBitSet(context);
        if (parentBitSet == null) {
            return null;
        }
        return new DiversifyingNearestChildrenKnnCollector(k, visitedLimit, searchStrategy, parentBitSet);
    }
}
