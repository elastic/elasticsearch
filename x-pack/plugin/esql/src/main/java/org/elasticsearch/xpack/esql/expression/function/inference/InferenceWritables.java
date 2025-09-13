/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Defines the named writables for inference functions in ESQL.
 */
public final class InferenceWritables {

    private InferenceWritables() {
        // Utility class
        throw new UnsupportedOperationException();
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWritables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();

        if (EsqlCapabilities.Cap.TEXT_EMBEDDING_FUNCTION.isEnabled()) {
            entries.add(TextEmbedding.ENTRY);
        }

        return Collections.unmodifiableList(entries);
    }
}
