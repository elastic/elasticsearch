/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

/**
 * The quantization pipeline used for encoding document vectors in IVF posting lists.
 */
public enum QuantizationType {
    /** Standard OSQ (Optimized Scalar Quantization) — the existing DiskBBQ pipeline. */
    BBQ(0),
    /** ASH (Asymmetric Scalar Hashing) — learned projection + scalar quantization. */
    ASH(1);

    private final int id;

    QuantizationType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static QuantizationType fromId(int id) {
        return switch (id) {
            case 0 -> BBQ;
            case 1 -> ASH;
            default -> throw new IllegalArgumentException("Unknown QuantizationType id: " + id);
        };
    }
}
