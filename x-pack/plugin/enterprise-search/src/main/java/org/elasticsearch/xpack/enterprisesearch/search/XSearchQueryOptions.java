/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.search;

public class XSearchQueryOptions {

    private final String query;
    private final String[] fieldNames;

    public XSearchQueryOptions(String query, String... fieldNames) {
        this.query = query;
        this.fieldNames = fieldNames;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public String getQuery() {
        return query;
    }
}
