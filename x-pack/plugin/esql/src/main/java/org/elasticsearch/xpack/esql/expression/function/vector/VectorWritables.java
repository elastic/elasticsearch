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
        entries.add(CosineSimilarity.ENTRY);
        entries.add(DotProduct.ENTRY);
        entries.add(L1Norm.ENTRY);
        entries.add(L2Norm.ENTRY);
        entries.add(Hamming.ENTRY);

        if (EsqlCapabilities.Cap.MAGNITUDE_SCALAR_VECTOR_FUNCTION.isEnabled()) {
            entries.add(Magnitude.ENTRY);
        }

        return Collections.unmodifiableList(entries);
    }
}
