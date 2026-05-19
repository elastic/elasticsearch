/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import java.io.IOException;

/**
 * Implemented by doc values iterators that can issue asynchronous I/O hints
 * for upcoming document accesses. Callers should invoke {@link #prefetch(int)}
 * before calling {@code advanceExact(docId)} to allow storage backends to
 * pre-populate the page cache for the data blocks that will be read.
 */
public interface Prefetchable {

    /**
     * Hint that the given document will be accessed in the near future.
     * Implementations should issue non-blocking I/O prefetch calls for the
     * data blocks backing the given document's values.
     */
    void prefetch(int docId) throws IOException;
}
