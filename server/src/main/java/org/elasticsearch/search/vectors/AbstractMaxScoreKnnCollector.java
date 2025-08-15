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
import org.apache.lucene.util.NumericUtils;

public abstract class AbstractMaxScoreKnnCollector extends AbstractKnnCollector {
    protected AbstractMaxScoreKnnCollector(int k, long visitLimit, KnnSearchStrategy searchStrategy) {
        super(k, visitLimit, searchStrategy);
    }

    public abstract long getMinCompetitiveDocScore();

    abstract void updateMinCompetitiveDocScore(long minCompetitiveDocScore);

    public static long encode(int docId, float score) {
        return (((long) NumericUtils.floatToSortableInt(score)) << 32) | (Integer.MAX_VALUE - docId);
    }

    public static float toScore(long value) {
        return NumericUtils.sortableIntToFloat((int) (value >>> 32));
    }

    public static int docId(long value) {
        return Integer.MAX_VALUE - ((int) value);
    }
}
