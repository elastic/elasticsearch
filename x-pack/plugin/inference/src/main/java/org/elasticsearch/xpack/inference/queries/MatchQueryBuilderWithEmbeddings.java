/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.inference.InferenceResults;

import java.io.IOException;
import java.util.Map;

public class MatchQueryBuilderWithEmbeddings extends MatchQueryBuilder implements PreComputedEmbeddingsProvider {
    public MatchQueryBuilderWithEmbeddings(String fieldName, Object value) {
        super(fieldName, value);
    }

    public MatchQueryBuilderWithEmbeddings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Map<String, InferenceResults> getEmbeddingsForField(String fieldName) {
        return Map.of();
    }
}
