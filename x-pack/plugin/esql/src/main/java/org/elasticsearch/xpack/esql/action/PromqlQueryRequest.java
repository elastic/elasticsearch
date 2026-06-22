/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.esql.plan.EsqlStatement;

public class PromqlQueryRequest extends PreparedEsqlQueryRequest {
    private final String index;

    public PromqlQueryRequest(String index, EsqlStatement statement, String queryDescription) {
        super(false, statement, queryDescription);
        this.index = index;
    }

    public String getIndex() {
        return index;
    }

    public String originalQuery() {
        return getDescription().substring(PreparedEsqlQueryRequest.PREPARED_QUERY_PREFIX.length());
    }

}
