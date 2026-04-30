/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;

/**
 * A {@link KnnCollector} that can accept bulk document-score arrays.
 */
public interface BulkKnnCollector extends KnnCollector {
    /**
     * Collect a batch of documents and scores.
     *
     * @param docs the doc ids to collect
     * @param scores the corresponding scores
     * @param count number of entries to read from {@code docs} and {@code scores}
     * @param bestScore the best score in the batch, used for fast rejection
     * @return the number of entries that were accepted (added or replaced)
     */
    int bulkCollect(int[] docs, float[] scores, int count, float bestScore);

    TopDocs unsortedTopK();
}
