/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.action.PreparedEsqlQueryRequest;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class PromqlQueryRequest extends PreparedEsqlQueryRequest {
    private final String index;
    public static final String PROMQL_TYPE = "promql";
    private final QueryParams params;

    public PromqlQueryRequest(String index, EsqlStatement statement, String queryDescription, Object... params) {
        super(false, statement, queryDescription);
        this.index = index;
        if (params != null && params.length > 0) {
            this.params = new QueryParams(getParams(params));
        } else {
            this.params = null;
        }
    }

    private static List<QueryParam> getParams(Object[] params) {
        List<QueryParam> queryParams = new ArrayList<>();
        for (int i = 0; i < params.length; i += 2) {
            if (i + 1 >= params.length) {
                throw new IllegalArgumentException("Missing value for parameter: " + params[i]);
            }
            String paramName = String.valueOf(params[i]);
            Object paramValue = params[i + 1];
            if (paramValue == null || (paramValue instanceof Collection<?> collection && collection.isEmpty())) {
                continue;
            }
            queryParams.add(
                new QueryParam(
                    paramName,
                    paramValue,
                    Objects.requireNonNullElse(DataType.fromJava(paramValue), DataType.KEYWORD),
                    ParserUtils.ParamClassification.VALUE
                )
            );
        }
        return queryParams;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public String queryDescription() {
        return super.queryDescription().substring(PreparedEsqlQueryRequest.PREPARED_QUERY_PREFIX.length());
    }

    @Override
    public String getType() {
        return PROMQL_TYPE;
    }

    @Override
    public QueryParams params() {
        if (params != null && params.size() > 0) {
            return params;
        }
        return super.params();
    }

}
