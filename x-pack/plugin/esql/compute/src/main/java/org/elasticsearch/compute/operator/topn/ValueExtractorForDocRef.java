/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.DocRefVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.Arrays;

/**
 * Writes a {@link DocRefVector} row as the registry ordinal of its origin, its segment and its doc id. Only the origins
 * that rows reference reach the registry.
 */
class ValueExtractorForDocRef implements ValueExtractor {
    private final DocRefEncoder encoder;
    private final DocRefVector vector;
    // block ordinal to registry ordinal, -1 until the first row with that origin
    private final int[] registryOrdinals;

    ValueExtractorForDocRef(TopNEncoder encoder, DocRefVector vector) {
        this.encoder = (DocRefEncoder) encoder;
        this.vector = vector;
        this.registryOrdinals = new int[vector.origins().size()];
        Arrays.fill(registryOrdinals, -1);
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        int blockOrdinal = vector.originOrdinals().getInt(position);
        int registryOrdinal = registryOrdinals[blockOrdinal];
        if (registryOrdinal < 0) {
            registryOrdinal = encoder.intern(vector.origins().get(blockOrdinal));
            registryOrdinals[blockOrdinal] = registryOrdinal;
        }
        encoder.encodeVInt(registryOrdinal, values);
        encoder.encodeInt(vector.segments().getInt(position), values);
        encoder.encodeInt(vector.docs().getInt(position), values);
    }

    @Override
    public String toString() {
        return "ValueExtractorForDocRef";
    }
}
