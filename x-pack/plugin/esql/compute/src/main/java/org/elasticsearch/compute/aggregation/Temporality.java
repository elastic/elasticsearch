/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

/**
 * The metric temporality, specifying how to correctly interpret counter values.
 */
public enum Temporality {
    CUMULATIVE("cumulative"),
    DELTA("delta");

    final BytesRef bytesRef;

    Temporality(String value) {
        this.bytesRef = new BytesRef(value);
    }

    public BytesRef bytesRef() {
        return bytesRef;
    }

    /**
     * Resolves a {@link Temporality} from a {@link BytesRef} value.
     *
     * @return the matching temporality, or {@code null} if the value does not match any known temporality
     */
    @Nullable
    public static Temporality fromBytesRef(BytesRef value) {
        if (CUMULATIVE.bytesRef.equals(value)) {
            return CUMULATIVE;
        } else if (DELTA.bytesRef.equals(value)) {
            return DELTA;
        }
        return null;
    }
}
