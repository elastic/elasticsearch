/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Reads values of a dense numeric column at an arbitrary document, in any order.
 * <p>
 * A dense column has one value per document, so a document id is also its value index. Implementations decode into their own
 * block, so reading through one does not disturb an iterator over the same column.
 */
public interface RandomAccessNumericValues {

    /** Returns the value of {@code docID}, which may be before or after the previous one read. */
    long valueAt(int docID) throws IOException;

    /** Implemented by doc values readers whose layout may support random access. */
    interface Provider {

        /** Returns a random access reader over this column, or {@code null} if its layout only supports iteration. */
        @Nullable
        RandomAccessNumericValues tryRandomAccess() throws IOException;
    }
}
