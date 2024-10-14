/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import java.io.IOException;

/**
 * Use a simple array to store the values.
 */
public abstract class DenseVectorNumericByteValues {

    private final int dims;
    protected int valuesCursor;
    protected byte[] values;

    protected DenseVectorNumericByteValues(int dims) {
        this.dims = dims;
        valuesCursor = 0;
    }

    /**
     * Apply values array.
     */
    protected final void apply(byte[] values) {
        this.values = values;
    }

    /**
     * Advance the iterator to exactly {@code target} and return whether
     * {@code target} has a value.
     * {@code target} must be greater than or equal to the current
     * doc ID and must be a valid doc ID, ie. &ge; 0 and
     * &lt; {@code maxDoc}.
     */
    public abstract boolean advanceExact(int target) throws IOException;

    public final int docValueCount() {
        return dims;
    }

    public final byte nextValue() {
        return values[valuesCursor++];
    }
}
