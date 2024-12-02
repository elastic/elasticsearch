/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query;

import org.elasticsearch.common.TriFunction;

public class InferenceQueryBuilderService {
    private final TriFunction<String, String, Boolean, AbstractQueryBuilder<?>> defaultInferenceQueryBuilder;

    InferenceQueryBuilderService(TriFunction<String, String, Boolean, AbstractQueryBuilder<?>> defaultInferenceQueryBuilder) {
        this.defaultInferenceQueryBuilder = defaultInferenceQueryBuilder;
    }

    public AbstractQueryBuilder<?> getDefaultInferenceQueryBuilder(String fieldName, String query, boolean throwOnUnsupportedQueries) {
        return defaultInferenceQueryBuilder != null
            ? defaultInferenceQueryBuilder.apply(fieldName, query, throwOnUnsupportedQueries)
            : null;
    }
}
