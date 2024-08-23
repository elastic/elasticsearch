/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;
import java.util.function.BiFunction;

public class QueryBuilderService {
    private final Map<String, BiFunction<String, String, AbstractQueryBuilder<?>>> functionMap;

    QueryBuilderService(Map<String, BiFunction<String, String, AbstractQueryBuilder<?>>> functionMap) {
        this.functionMap = functionMap;
    }

    public AbstractQueryBuilder<?> getQueryBuilder(String queryName, String fieldName, String query) {
        BiFunction<String, String, AbstractQueryBuilder<?>> function = functionMap.get(queryName);
        if (function == null) {
            throw new ElasticsearchStatusException(
                "No query builder registered for query [" + queryName + "]",
                RestStatus.INTERNAL_SERVER_ERROR
            );
        }

        return function.apply(fieldName, query);
    }
}
