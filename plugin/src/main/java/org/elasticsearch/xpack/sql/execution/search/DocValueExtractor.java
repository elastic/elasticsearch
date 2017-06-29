/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

class DocValueExtractor implements HitExtractor {
    private final String fieldName;

    DocValueExtractor(String name) {
        this.fieldName = name;
    }

    @Override
    public Object get(SearchHit hit) {
        DocumentField field = hit.field(fieldName);
        return field != null ? field.getValue() : null;
    }

    @Override
    public String toString() {
        return fieldName;
    }
}