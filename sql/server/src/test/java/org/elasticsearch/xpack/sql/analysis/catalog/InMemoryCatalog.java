/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * In memory implementation of catalog designed for testing.
 */
class InMemoryCatalog implements Catalog {
    private final Map<String, EsIndex> indices;

    InMemoryCatalog(List<EsIndex> indices) {
        this.indices = indices.stream().collect(toMap(EsIndex::name, Function.identity()));
    }

    @Override
    public GetIndexResult getIndex(String index) {
        EsIndex result = indices.get(index);
        return result == null ? GetIndexResult.notFound(index) : GetIndexResult.valid(result);
    }
}