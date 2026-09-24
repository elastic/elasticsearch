/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;

public abstract class ParserConfig extends Parser {

    // is null when running inside the IDEA plugin
    private EsqlConfig config;

    public ParserConfig(TokenStream input) {
        super(input);
    }

    boolean isDevVersion() {
        return config == null || config.isDevVersion();
    }

    /**
     * True when lookahead is an unquoted identifier spelling {@code keyword}.
     * Used by GRAPH EXPAND so STATS / SORT / UNTIL can follow a clause that
     * already switched the lexer into {@code EXPRESSION_MODE}, where those
     * words are not command tokens.
     */
    boolean isIdent(String keyword) {
        Token t = _input.LT(1);
        return t != null && keyword.equalsIgnoreCase(t.getText());
    }

    void setEsqlConfig(EsqlConfig config) {
        this.config = config;
    }
}
