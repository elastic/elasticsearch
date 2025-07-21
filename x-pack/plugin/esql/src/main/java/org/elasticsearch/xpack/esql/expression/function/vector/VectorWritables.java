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

        if (EsqlCapabilities.Cap.KNN_FUNCTION_V3.isEnabled()) {
            entries.add(Knn.ENTRY);
        }
        if (EsqlCapabilities.Cap.COSINE_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            entries.add(CosineSimilarity.ENTRY);
        }

        return Collections.unmodifiableList(entries);
    }
}
