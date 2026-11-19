/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.Token;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class QueryParams {

    private final List<QueryParam> params; // This matches the named or unnamed parameters specified in an EsqlQueryRequest.params
    private final Map<String, QueryParam> nameToParam; // This matches the named parameters specified in an EsqlQueryRequest.params
    private Map<Token, QueryParam> tokenToParam; // This is populated by EsqlParser, each parameter marker has an entry
    private List<ParsingException> parsingErrors;
    private final int paramsCount;
    /**
     * The value that stands for "Any" in the statement being parsed, set by the statement builder, which is also what applies
     * a {@code SET param_any_value}. It lives here because the PromQL parser has the parameters, not the builder of the statement.
     */
    @Nullable
    private String anyParamValue;

    public QueryParams() {
        this(null);
    }

    public QueryParams(List<QueryParam> params) {
        this.tokenToParam = new HashMap<>();
        this.parsingErrors = new ArrayList<>();

        if (params == null || params.isEmpty()) {
            this.params = List.of();
            this.nameToParam = Map.of();
            this.paramsCount = 0;
        } else {
            this.paramsCount = params.size();
            this.params = new ArrayList<>(paramsCount);
            Map<String, QueryParam> tempNameToParam = new HashMap<>(paramsCount);
            for (QueryParam p : params) {
                this.params.add(p);
                String name = p.name();
                if (name != null) {
                    tempNameToParam.put(name, p);
                }
            }
            this.nameToParam = Collections.unmodifiableMap(tempNameToParam);
        }
    }

    public int size() {
        return this.paramsCount;
    }

    public QueryParam get(int index) {
        return (index <= 0 || index > this.paramsCount) ? null : params.get(index - 1);
    }

    public Map<String, QueryParam> namedParams() {
        return this.nameToParam;
    }

    public boolean contains(String paramName) {
        return this.nameToParam.containsKey(paramName);
    }

    public QueryParam get(String paramName) {
        return nameToParam.get(paramName);
    }

    public boolean contains(Token token) {
        return this.tokenToParam.containsKey(token);
    }

    public QueryParam get(Token tokenLocation) {
        return this.tokenToParam.get(tokenLocation);
    }

    public void addTokenParam(Token token, QueryParam param) {
        this.tokenToParam.put(token, param);
    }

    public Iterator<ParsingException> parsingErrors() {
        return this.parsingErrors.iterator();
    }

    public void addParsingError(ParsingException e) {
        this.parsingErrors.add(e);
    }

    /** The value that stands for "Any" in the statement being parsed, or {@code null} if none is recognized. */
    @Nullable
    public String anyParamValue() {
        return anyParamValue;
    }

    public void anyParamValue(@Nullable String anyParamValue) {
        this.anyParamValue = anyParamValue;
    }

    /**
     * Whether {@code param} stands for "Any": a string with the {@link #anyParamValue() reserved value}, or a list of just that
     * one string. A list with more elements is a normal value, even if all are the reserved value. A parameter declared as a
     * pattern is a normal one too, as the declaration says that the string is a pattern to match, and so is a literal of the
     * reserved value in the query.
     */
    public boolean isAny(@Nullable QueryParam param) {
        if (anyParamValue == null || param == null || param.classification() == ParserUtils.ParamClassification.PATTERN) {
            return false;
        }
        Object value = param.value();
        if (value instanceof List<?> list) {
            value = list.size() == 1 ? list.getFirst() : null;
        }
        return anyParamValue.equals(value);
    }

    public List<QueryParam> params() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryParams that = (QueryParams) o;
        return paramsCount == that.paramsCount
            && params.equals(that.params)
            && nameToParam.equals(that.nameToParam)
            && tokenToParam.equals(that.tokenToParam)
            && parsingErrors.equals(that.parsingErrors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(params, nameToParam, tokenToParam, parsingErrors, paramsCount);
    }
}
