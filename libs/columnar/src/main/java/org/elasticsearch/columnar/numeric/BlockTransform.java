/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * A reversible in-place transform on a block of {@code long}s, applied adaptively: it fires only when
 * it shrinks the residual. Its {@code id} is frozen once shipped.
 */
public interface BlockTransform {

    /** Frozen stage id, unique among transforms and stable on disk. */
    byte id();

    /**
     * Mutates {@code block} in place and writes its reversal params to {@code params} when it helps.
     *
     * @return whether the transform fired
     */
    boolean tryEncode(long[] block, DataOutput params) throws IOException;

    /** Reverses {@link #tryEncode} in place, reading the params it wrote. */
    void decode(long[] block, DataInput params) throws IOException;
}
