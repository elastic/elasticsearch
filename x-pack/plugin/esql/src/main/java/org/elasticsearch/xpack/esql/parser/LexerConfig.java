/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;

/**
 * Base class for hooking versioning information into the ANTLR parser.
 */
public abstract class LexerConfig extends Lexer {

    // is null when running inside the IDEA plugin
    EsqlConfig config;
    private int promqlDepth = 0;

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

    /**
     * Rewinds the lexer back to the position of the token currently being matched,
     * keeping the first {@code charsToKeep} characters as part of the emitted token
     * and exposing the remaining input to the next {@link #nextToken()} call.
     *
     * <p>Used by lookahead-style lexer rules (see {@code IN_SUBQUERY_LP} and
     * {@code IN_EXPR_FALLBACK} in {@code InExpression.g4}) that match more text than
     * they want to consume — e.g. peek at {@code '(' KEYWORD WS*} but only emit
     * {@code '('}, leaving the keyword to be re-lexed in a different mode.
     *
     * <p>{@code _input.seek} alone is not enough: {@code consume()} has already
     * advanced the lexer's line and column counters for every matched character,
     * and those counters drive the columns reported in error messages for
     * subsequent tokens. This method rewinds all three (input, line, column) in
     * lockstep.
     *
     * <p>The kept prefix must not contain a newline; this is true at both current
     * call sites (the kept prefix is either empty or a single {@code '('}). If
     * that ever changes, the line counter would need to be advanced by however
     * many newlines the prefix contains.
     */
    void rewindToTokenStart(int charsToKeep) {
        _input.seek(_tokenStartCharIndex + charsToKeep);
        getInterpreter().setLine(_tokenStartLine);
        getInterpreter().setCharPositionInLine(_tokenStartCharPositionInLine + charsToKeep);
    }
}
