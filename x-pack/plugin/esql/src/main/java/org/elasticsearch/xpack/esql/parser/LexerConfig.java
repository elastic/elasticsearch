/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Token;

/**
 * Base class for hooking versioning information into the ANTLR parser.
 */
public abstract class LexerConfig extends Lexer {

    // is null when running inside the IDEA plugin
    EsqlConfig config;
    private int promqlDepth = 0;

    // Type of the last token emitted on the default channel. Used by IN_SUBQUERY_LP in
    // EXPRESSION_MODE to recognize that '(' immediately follows the IN keyword (across
    // any number of hidden whitespace/comment tokens) so the subquery body can be lexed
    // in DEFAULT_MODE rather than as an IN value list.
    int lastDefaultChannelType = -1;

    public LexerConfig() {}

    public LexerConfig(CharStream input) {
        super(input);
    }

    boolean isDevVersion() {
        return config == null || config.isDevVersion();
    }

    boolean isExternalDataSourcesEnabled() {
        return config == null || config.isExternalDataSourcesEnabled();
    }

    void setEsqlConfig(EsqlConfig config) {
        this.config = config;
    }

    // Needed by the Promql command
    void incPromqlDepth() {
        promqlDepth++;
    }

    void decPromqlDepth() {
        if (promqlDepth == 0) {
            throw new ParsingException("Invalid PromQL command, unexpected '('");
        }
        promqlDepth--;
    }

    void resetPromqlDepth() {
        if (promqlDepth != 0) {
            throw new ParsingException(
                "Invalid PromQL declaration, missing [{}] [{}] parenthesis",
                Math.absExact(promqlDepth),
                promqlDepth > 0 ? '(' : ')'
            );
        }
    }

    boolean isPromqlQuery() {
        return promqlDepth > 0;
    }

    @Override
    public void emit(Token token) {
        if (token.getChannel() == Token.DEFAULT_CHANNEL) {
            lastDefaultChannelType = token.getType();
        }
        super.emit(token);
    }
}
