/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import java.io.IOException;

/**
 * A doc-values column that prefetches what reading a batch of documents reads, so that the downloads of many columns
 * overlap. The consumer, which knows the documents, calls it; a column never prefetches on its own.
 *
 * <p>Some bytes are only located by reading others, so a batch is prefetched in stages: the consumer runs stage {@code 0},
 * {@code 1}, and so on until one returns {@code false}, then reads the documents. A stage reads only what earlier stages
 * prefetched. The structures that locate values, such as the presence of a sparse column or a table of offsets, are
 * prefetched whole; the values only for the batch.
 */
public interface PrefetchableDocValues {

    /**
     * Runs stage {@code stage} for {@code docs[from, to)}, which are in ascending order. It does not move the column.
     *
     * @return whether stage {@code stage + 1} is needed
     */
    boolean prefetch(int stage, int[] docs, int from, int to) throws IOException;
}
