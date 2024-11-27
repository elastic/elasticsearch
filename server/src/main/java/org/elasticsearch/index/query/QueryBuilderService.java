/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import java.util.function.BiFunction;

public class QueryBuilderService {
    private final BiFunction<String, String, AbstractQueryBuilder<?>> defaultInferenceQueryBuilder;

    QueryBuilderService(BiFunction<String, String, AbstractQueryBuilder<?>> defaultInferenceQueryBuilder) {
        this.defaultInferenceQueryBuilder = defaultInferenceQueryBuilder;
    }

    public AbstractQueryBuilder<?> getDefaultInferenceQueryBuilder(String fieldName, String query) {
        return defaultInferenceQueryBuilder.apply(fieldName, query);
    }
}
