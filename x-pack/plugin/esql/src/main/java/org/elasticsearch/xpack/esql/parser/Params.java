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

public class Params {

    private static final String UNNAMED = "unnamed";

    // This matches the named or unnamed parameters specified in an EsqlQueryRequest.params.
    private List<TypedParamValue> paramsByPosition = new ArrayList<>();

    // This matches the named parameters specified in an EsqlQueryRequest.params.
    private HashMap<String, TypedParamValue> paramsByName = new HashMap<>();

    // This is populated by EsqlParser, each parameter marker has an entry in paramsByToken
    private Map<Token, TypedParamValue> paramsByToken = new HashMap<>();

    public Params() {}

    public Params(List<TypedParamValue> params) {
        this.paramsByPosition.addAll(params);
        populateNamedParams();
    }

    private void populateNamedParams() {
        for (TypedParamValue p : this.paramsByPosition) {
            if (p.name.equalsIgnoreCase(UNNAMED) != true) {
                paramsByName.put(p.name, p);
            }
        }
    }

    public List<TypedParamValue> positionalParams() {
        return this.paramsByPosition;
    }

    public TypedParamValue paramByPosition(int index) {
        return paramsByPosition.get(index - 1);
    }

    public HashMap<String, TypedParamValue> namedParams() {
        return this.paramsByName;
    }

    public boolean isNamedParam() {
        return this.paramsByName.size() > 0;
    }

    public boolean containsParamName(String paramName) {
        return this.paramsByName.containsKey(paramName);
    }

    public TypedParamValue getParamByName(String paramName) {
        return paramsByName.get(paramName);
    }

    public void positionalParamTokens(Map<Token, TypedParamValue> paramsByToken) {
        this.paramsByToken = paramsByToken;
    }

    public Map<Token, TypedParamValue> positionalParamTokens() {
        return this.paramsByToken;
    }

    public boolean containsTokenLocation(Token token) {
        return this.paramsByToken.containsKey(token);
    }

    public TypedParamValue getParamByTokenLocation(Token tokenLocation) {
        return this.paramsByToken.get(tokenLocation);
    }
}
