/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Defines the named writables for vector functions in ESQL.
 */
public final class VectorWritables {

    private VectorWritables() {
        // Utility class
        throw new UnsupportedOperationException();
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWritables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();

        entries.add(Knn.ENTRY);
        if (EsqlCapabilities.Cap.COSINE_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            entries.add(CosineSimilarity.ENTRY);
        }
        if (EsqlCapabilities.Cap.DOT_PRODUCT_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            entries.add(DotProduct.ENTRY);
        }
        if (EsqlCapabilities.Cap.L1_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            entries.add(L1Norm.ENTRY);
        }
        if (EsqlCapabilities.Cap.L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            entries.add(L2Norm.ENTRY);
        }
        if (EsqlCapabilities.Cap.MAGNITUDE_SCALAR_VECTOR_FUNCTION.isEnabled()) {
            entries.add(Magnitude.ENTRY);
        }
        if (EsqlCapabilities.Cap.HAMMING_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            entries.add(Hamming.ENTRY);
        }

        return Collections.unmodifiableList(entries);
    }
}
