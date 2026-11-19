/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;

/**
 * Abstract class for collectors that maintain a maximum score for KNN search.
 * It extends the {@link AbstractKnnCollector} and provides methods to manage
 * the minimum competitive document score, useful for tracking competitive scores
 * over multiple leaves.
 */
abstract class AbstractMaxScoreKnnCollector extends AbstractKnnCollector {
    public static final long LEAST_COMPETITIVE = NeighborQueue.encodeRaw(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);

    protected AbstractMaxScoreKnnCollector(int k, long visitLimit, KnnSearchStrategy searchStrategy) {
        super(k, visitLimit, searchStrategy);
    }

    /**
     * Returns the encoded minimum competitive document score. A local bound is not competitive until {@code k} results have been collected.
     */
    public abstract long getMinCompetitiveDocScore();

    /** Raises the encoded minimum competitive document to at least the supplied bound. */
    abstract void updateMinCompetitiveDocScore(long minCompetitiveDocScore);
}
