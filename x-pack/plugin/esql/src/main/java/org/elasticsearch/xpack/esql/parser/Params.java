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

    // This matches the named or unnamed parameters specified in an EsqlQueryRequest.params.
    private List<Param> paramsByPosition = new ArrayList<>();

    // This matches the named parameters specified in an EsqlQueryRequest.params.
    private HashMap<String, Param> paramsByName = new HashMap<>();

    // This is populated by EsqlParser, each parameter marker has an entry in paramsByToken
    private Map<Token, Param> paramsByToken = new HashMap<>();

    public Params() {}

    public Params(List<Param> params) {
        this.paramsByPosition.addAll(params);
        populateNamedParams();
    }

    private void populateNamedParams() {
        for (Param p : this.paramsByPosition) {
            if (p.name != null) {
                paramsByName.put(p.name, p);
            }
        }
    }

    public List<Param> positionalParams() {
        return this.paramsByPosition;
    }

    public Param paramByPosition(int index) {
        return paramsByPosition.get(index - 1);
    }

    public HashMap<String, Param> namedParams() {
        return this.paramsByName;
    }

    public boolean isNamedParam() {
        return this.paramsByName.size() > 0;
    }

    public boolean containsParamName(String paramName) {
        return this.paramsByName.containsKey(paramName);
    }

    public Param getParamByName(String paramName) {
        return paramsByName.get(paramName);
    }

    public void positionalParamTokens(Map<Token, Param> paramsByToken) {
        this.paramsByToken = paramsByToken;
    }

    public Map<Token, Param> positionalParamTokens() {
        return this.paramsByToken;
    }

    public boolean containsTokenLocation(Token token) {
        return this.paramsByToken.containsKey(token);
    }

    public Param getParamByTokenLocation(Token tokenLocation) {
        return this.paramsByToken.get(tokenLocation);
    }
}
