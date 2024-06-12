/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryParams {

    public static final QueryParams EMPTY = new QueryParams();

    // This matches the named or unnamed parameters specified in an EsqlQueryRequest.params.
    private List<QueryParam> params = new ArrayList<>();

    // This matches the named parameters specified in an EsqlQueryRequest.params.
    private Map<String, QueryParam> nameToParam = new HashMap<>();

    // This is populated by EsqlParser, each parameter marker has an entry.
    private Map<Token, QueryParam> tokenToParam = new HashMap<>();

    private List<ParsingException> parsingErrors = new ArrayList<>();

    public QueryParams() {}

    public QueryParams(List<QueryParam> params) {
        for (QueryParam p : params) {
            this.params.add(p);
            String name = p.name();
            if (name != null) {
                nameToParam.put(name, p);
            }
        }
    }

    public List<QueryParam> positionalParams() {
        return this.params;
    }

    public QueryParam get(int index) {
        return (index <= 0 || index > params.size()) ? null : params.get(index - 1);
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

    public Map<Token, QueryParam> positionalParamTokens() {
        return this.tokenToParam;
    }

    public boolean contains(Token token) {
        return this.tokenToParam.containsKey(token);
    }

    public QueryParam get(Token tokenLocation) {
        return this.tokenToParam.get(tokenLocation);
    }

    public List<ParsingException> parsingErrors() {
        return this.parsingErrors;
    }

    public void addParsingError(ParsingException e) {
        this.parsingErrors.add(e);
    }
}
