/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.action.PreparedEsqlQueryRequest;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.prometheus.rest.PromqlQueryPlanBuilder.PromqlStatementResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The ES|QL request behind a Prometheus endpoint. Its plan is built from the PromQL query only in {@link #parse}, which the
 * query action calls where it parses a query string: once the request is authorized. Parsing a query can be costly, so a
 * caller that may not run ES|QL mustn't be able to make the node do it. Until then, there is no plan to read.
 */
public class PromqlQueryRequest extends PreparedEsqlQueryRequest {
    private final String index;
    public static final String PROMQL_TYPE = "promql";
    private final QueryParams params;
    private final Supplier<PromqlStatementResult> builder;
    private volatile PromqlStatementResult result;

    /**
     * @param builder builds the statement and the Prometheus result type of its response when the request is parsed
     */
    PromqlQueryRequest(String index, Supplier<PromqlStatementResult> builder, String queryDescription, Object... params) {
        super(false, null, queryDescription, false);
        this.index = index;
        this.builder = builder;
        if (params != null && params.length > 0) {
            this.params = new QueryParams(getParams(params));
        } else {
            this.params = null;
        }
    }

    /**
     * The statement and the Prometheus result type of its response, built when the request is parsed. A response listener
     * reads it once the query has run.
     *
     * @throws IllegalStateException if the request isn't parsed yet
     */
    PromqlStatementResult result() {
        PromqlStatementResult parsed = result;
        if (parsed == null) {
            throw new IllegalStateException("the [" + PROMQL_TYPE + "] request isn't parsed yet");
        }
        return parsed;
    }

    /**
     * @throws IllegalStateException if the request isn't parsed yet
     */
    @Override
    public EsqlStatement statement() {
        return result().esqlStatement();
    }

    /**
     * Builds the statement, which is where the PromQL query is parsed, validates its settings as the prepared one is, and
     * keeps it for {@link #statement} and {@link #result}.
     */
    @Override
    public EsqlStatement parse(
        EsqlParser parser,
        SettingsValidationContext settingsValidationCtx,
        InferenceSettings inferenceSettings,
        EsqlFlags flags
    ) {
        PromqlStatementResult built = builder.get();
        QuerySettings.validate(built.esqlStatement(), settingsValidationCtx);
        result = built;
        return built.esqlStatement();
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
