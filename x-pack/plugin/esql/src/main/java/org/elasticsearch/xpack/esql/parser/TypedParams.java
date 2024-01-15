/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.Token;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypedParams {
    private List<TypedParamValue> positionalParams = List.of();
    private HashMap<String, TypedParamValue> namedParams = new HashMap<>();

    private Map<Token, TypedParamValue> positionalParamTokens = new HashMap<>();

    public TypedParams() {}

    public void positionalParams(List<TypedParamValue> params) {
        this.positionalParams = params;
    }

    public List<TypedParamValue> positionalParams () {
        return this.positionalParams;
    }

    public void namedParams(HashMap<String, TypedParamValue> namedParams) {
        this.namedParams = namedParams;
    }

    public HashMap<String, TypedParamValue> namedParams() {
        return this.namedParams;
    }

    public void positionalParamTokens(Map<Token, TypedParamValue> positionalParamTokens) {
        this.positionalParamTokens = positionalParamTokens;
    }

    public TypedParamValue getParamAt(int index) {
        return null;
    }

    public boolean containsTokenLocation(Token token) {
        return this.positionalParamTokens.containsKey(token);
    }
    public TypedParamValue getParamByTokenLocation(Token tokenLocation) {
        return this.positionalParamTokens.get(tokenLocation);
    }

    public boolean containsParamName(String paramName) {
        return this.namedParams.containsKey(paramName);
    }

    public TypedParamValue getParamByName(String paramName) {
        return namedParams.get(paramName);
    }
}
