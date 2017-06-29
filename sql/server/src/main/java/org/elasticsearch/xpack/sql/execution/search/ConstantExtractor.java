/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.search.SearchHit;

class ConstantExtractor implements HitExtractor {

    private final Object constant;

    ConstantExtractor(Object constant) {
        this.constant = constant;
    }

    @Override
    public Object get(SearchHit hit) {
        return constant;
    }

    @Override
    public String toString() {
        return "^" + constant;
    }
}