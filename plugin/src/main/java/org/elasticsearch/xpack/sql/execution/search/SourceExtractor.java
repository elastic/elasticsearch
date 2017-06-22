/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import java.util.Map;

import org.elasticsearch.search.SearchHit;

class SourceExtractor implements HitExtractor {
    private final String fieldName;

    SourceExtractor(String name) {
        this.fieldName = name;
    }

    @Override
    public Object get(SearchHit hit) {
        Map<String, Object> source = hit.getSourceAsMap();
        return source != null ? source.get(fieldName) : null;
    }

    @Override
    public String toString() {
        return fieldName;
    }
}