/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser;

import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

/**
 * Arithmetic operations supported in PromQL scalar expressions.
 */
public enum ArithmeticOperation {
    ADD("+"),
    SUB("-"),
    MUL("*"),
    DIV("/"),
    MOD("%"),
    POW("^");

    private final String symbol;

    ArithmeticOperation(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    /**
     * Convert ANTLR token type to ArithmeticOperation enum.
     */
    public static ArithmeticOperation fromTokenType(Source source, int tokenType) {
        return switch (tokenType) {
            case PromqlBaseParser.PLUS -> ADD;
            case PromqlBaseParser.MINUS -> SUB;
            case PromqlBaseParser.ASTERISK -> MUL;
            case PromqlBaseParser.SLASH -> DIV;
            case PromqlBaseParser.PERCENT -> MOD;
            case PromqlBaseParser.CARET -> POW;
            default -> throw new ParsingException(source, "Unknown token type: {}", tokenType);
        };
    }
}
