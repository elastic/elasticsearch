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
 * A doc-values column that asks for the bytes a batch of documents reads before they are read, so that the reads of many
 * columns overlap. A column never prefetches on its own: the consumer, which knows the documents, calls it.
 *
 * <p>A batch is prefetched in stages, since some bytes are only located by reading others. The consumer runs stage
 * {@code 0}, {@code 1}, and so on until one returns {@code false}, then reads the documents. A stage reads only what earlier
 * stages asked for, and reading the documents reads only what some stage asked for.
 *
 * <p>Prefetching asks for no read unit that reading the documents does not fetch; a structure within a single unit is
 * asked for whole. It does not move the column.
 */
public interface PrefetchableDocValues {

    /**
     * Runs stage {@code stage} for {@code docs[from, to)}, which are in ascending order.
     *
     * @param readUnit the bytes the store fetches at once, aligned on multiples of it
     * @return whether stage {@code stage + 1} is needed
     */
    boolean prefetch(int stage, int[] docs, int from, int to, int readUnit) throws IOException;
}
