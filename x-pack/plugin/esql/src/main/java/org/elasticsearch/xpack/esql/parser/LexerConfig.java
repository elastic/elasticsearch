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
     * <p>Used by lookahead-style lexer rules that match more text than they want to
     * consume. There are three: {@code IN_SUBQUERY_LP} and {@code IN_EXPR_FALLBACK}
     * in {@code InExpression.g4}, which peek at {@code '(' KEYWORD WS*} and emit only
     * {@code '('}; and {@code DEV_TABLE_FUNCTION_NAME} in {@code From.g4}, which peeks
     * at {@code name '('} and emits only the name. In each case the text given back is
     * re-lexed in a different mode.
     *
     * <p>{@code _input.seek} alone is not enough: {@code consume()} has already
     * advanced the lexer's line and column counters for every matched character,
     * and those counters drive the columns reported in error messages for
     * subsequent tokens. This method rewinds all three (input, line, column) in
     * lockstep.
     *
     * <p><strong>The kept prefix must not contain a newline</strong>, since the line
     * counter is restored to {@code _tokenStartLine} unconditionally. It holds at all
     * three call sites: the prefix is empty, a single {@code '('}, or a
     * {@code LETTER UNQUOTED_ID_BODY*} identifier, and none of those fragments admits
     * {@code \r} or {@code \n}. A caller whose prefix could span lines would have to
     * advance the line counter by however many newlines it contains.
     *
     * <p>{@code charsToKeep} is counted in {@code CharStream} positions, so a caller
     * deriving it from {@code getText().length()} — a UTF-16 count — is only correct
     * while the kept prefix cannot contain a character outside the BMP. That holds for
     * the identifier prefix above, which is ASCII by construction.
     */
    void rewindToTokenStart(int charsToKeep) {
        _input.seek(_tokenStartCharIndex + charsToKeep);
        getInterpreter().setLine(_tokenStartLine);
        getInterpreter().setCharPositionInLine(_tokenStartCharPositionInLine + charsToKeep);
    }
}
