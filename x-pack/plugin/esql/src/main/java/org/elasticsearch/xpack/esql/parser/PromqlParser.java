/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DiagnosticErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.parser.promql.PromqlAstBuilder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.BitSet;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;

public class PromqlParser {

    private static final Logger log = LogManager.getLogger(PromqlParser.class);

    /**
     * Maximum number of characters in a PromQL expression. Mirrors {@link EsqlParser#MAX_LENGTH}: ANTLR buffers
     * the input and builds the full parse tree in heap before any depth guard can run, so the input itself
     * must be bounded. Note the inner query of a PROMQL source command is a substring of the enclosing ES|QL
     * query (already capped), hence the operator/depth pre-scan below is the effective guard on that path.
     */
    public static final int MAX_LENGTH = EsqlParser.MAX_LENGTH;

    /**
     * Maximum number of binary operators allowed in a single PromQL expression. Retained heap grows as
     * 7*n^2 bytes with chain length n (each nested binary node keeps a full-span copy of its source text),
     * so the cap bounds the worst case at about 7MB.
     * See <a href="https://github.com/elastic/security/issues/12593">security#12593</a>.
     */
    public static final int MAX_BINARY_OPERATORS = 1000;

    private final boolean DEBUG = false;

    /**
     * Parses an PromQL expression into execution plan
     */
    public LogicalPlan createStatement(String query) {
        return createStatement(query, null, null, 0, 0, new QueryParams());
    }

    public LogicalPlan createStatement(String query, Literal start, Literal end, int startLine, int startColumn) {
        return createStatement(query, start, end, startLine, startColumn, new QueryParams());
    }

    public LogicalPlan createStatement(String query, Literal start, Literal end, int startLine, int startColumn, QueryParams params) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as expression: {}", query);
        }

        return invokeParser(query, start, end, startLine, startColumn, params, PromqlBaseParser::singleStatement, PromqlAstBuilder::plan);
    }

    private <T> T invokeParser(
        String query,
        Literal start,
        Literal end,
        int startLine,
        int startColumn,
        QueryParams params,
        Function<PromqlBaseParser, ParserRuleContext> parseFunction,
        BiFunction<PromqlAstBuilder, ParserRuleContext, T> visitor
    ) {
        if (query.length() > MAX_LENGTH) {
            throw new ParsingException("PromQL statement is too large [{} characters > {}]", query.length(), MAX_LENGTH);
        }
        CommonTokenStream tokenStream = createTokenStream(query);
        // Check nesting depth and operator count on the token stream BEFORE invoking ANTLR's
        // recursive-descent parser, mirroring EsqlParser. Without this, a long chain of binary
        // operators exhausts the heap while building the parse tree, before any post-parse guard runs.
        try {
            tokenStream.fill();
            List<Token> tokens = tokenStream.getTokens();
            checkExpressionDepth(tokens);
            checkOperatorCount(tokens);
        } catch (ParsingException pe) {
            if (pe.getMessage() != null && pe.getMessage().contains("exceeded the maximum")) {
                throw pe;
            }
            // Lexer error during fill() - rebuild the token stream from scratch so the
            // parser runs lazily and reports the same error as without this pre-scan.
            tokenStream = createTokenStream(query);
        }
        try {
            PromqlBaseParser parser = new PromqlBaseParser(tokenStream);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            if (DEBUG) {
                debug(parser);
                tokenStream.fill();

                for (Token t : tokenStream.getTokens()) {
                    String symbolicName = PromqlBaseLexer.VOCABULARY.getSymbolicName(t.getType());
                    String literalName = PromqlBaseLexer.VOCABULARY.getLiteralName(t.getType());
                    log.info(format(Locale.ROOT, "  %-15s '%s'", symbolicName == null ? literalName : symbolicName, t.getText()));
                }
            }

            ParserRuleContext tree = parseFunction.apply(parser);

            if (log.isTraceEnabled()) {
                log.trace("Parse tree: {}", tree.toStringTree());
            }
            return visitor.apply(new PromqlAstBuilder(start, end, startLine, startColumn, params), tree);
        } catch (StackOverflowError e) {
            throw new ParsingException(
                "PromQL statement is too large, causing stack overflow when generating the parsing tree: [{}]",
                query
            );
        } catch (EmptyStackException ese) {
            throw new ParsingException("Invalid query [{}]", query);
        }
    }

    private static CommonTokenStream createTokenStream(String query) {
        PromqlBaseLexer lexer = new PromqlBaseLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);
        return new CommonTokenStream(lexer);
    }

    private static void checkExpressionDepth(List<Token> tokens) {
        int depth = 0;
        for (Token token : tokens) {
            if (token.getType() == PromqlBaseLexer.LP) {
                depth++;
            } else if (token.getType() == PromqlBaseLexer.RP) {
                depth--;
            }
            if (depth > PromqlAstBuilder.MAX_EXPRESSION_DEPTH) {
                throw new ParsingException(
                    "PromQL statement exceeded the maximum expression depth allowed ({})",
                    PromqlAstBuilder.MAX_EXPRESSION_DEPTH
                );
            }
        }
    }

    private static void checkOperatorCount(List<Token> tokens) {
        // NB: unary PLUS/MINUS are conservatively counted as binary operators here; the bound
        // is generous enough that legitimate queries are unaffected.
        if (countBinaryOperators(tokens) > MAX_BINARY_OPERATORS) {
            throw new ParsingException(
                "PromQL statement exceeded the maximum number of binary operators allowed ({})",
                MAX_BINARY_OPERATORS
            );
        }
    }

    private static int countBinaryOperators(List<Token> tokens) {
        int count = 0;
        for (Token token : tokens) {
            if (isBinaryOperator(token.getType())) {
                count++;
            }
        }
        return count;
    }

    /**
     * Validates a batch of PromQL expressions (e.g. repeated {@code match[]} selectors of the native
     * Prometheus endpoints) against the same bounds as a single expression, applied to the batch total.
     * Each expression is still guarded individually when parsed; this additionally prevents splitting a
     * single oversized expression across many individually-valid inputs.
     *
     * @throws ParsingException if the batched inputs exceed the limits in total
     */
    public static void validateBatch(List<String> queries) {
        long totalLength = 0;
        long totalBinaryOperators = 0;
        for (String query : queries) {
            totalLength += query.length();
            if (totalLength > MAX_LENGTH) {
                throw new ParsingException("PromQL statements are too large in total [{} characters > {}]", totalLength, MAX_LENGTH);
            }
            totalBinaryOperators += countBinaryOperators(query);
            if (totalBinaryOperators > MAX_BINARY_OPERATORS) {
                throw new ParsingException(
                    "PromQL statements exceeded the maximum number of binary operators allowed in total ({})",
                    MAX_BINARY_OPERATORS
                );
            }
        }
    }

    private static int countBinaryOperators(String query) {
        try {
            CommonTokenStream tokenStream = createTokenStream(query);
            tokenStream.fill();
            return countBinaryOperators(tokenStream.getTokens());
        } catch (RuntimeException e) {
            // Lexer errors surface when the query is actually parsed; don't fail validation on them here.
            return 0;
        }
    }

    private static boolean isBinaryOperator(int tokenType) {
        return switch (tokenType) {
            case PromqlBaseLexer.PLUS, PromqlBaseLexer.MINUS, PromqlBaseLexer.ASTERISK, PromqlBaseLexer.SLASH, PromqlBaseLexer.PERCENT,
                PromqlBaseLexer.CARET, PromqlBaseLexer.EQ, PromqlBaseLexer.NEQ, PromqlBaseLexer.GT, PromqlBaseLexer.GTE, PromqlBaseLexer.LT,
                PromqlBaseLexer.LTE, PromqlBaseLexer.AND, PromqlBaseLexer.OR, PromqlBaseLexer.UNLESS -> true;
            default -> false;
        };
    }

    private static void debug(PromqlBaseParser parser) {

        // when debugging, use the exact prediction mode (needed for diagnostics as well)
        parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);

        parser.addParseListener(parser.new TraceListener());

        parser.addErrorListener(new DiagnosticErrorListener(false) {
            @Override
            public void reportAttemptingFullContext(
                Parser recognizer,
                DFA dfa,
                int startIndex,
                int stopIndex,
                BitSet conflictingAlts,
                ATNConfigSet configs
            ) {}

            @Override
            public void reportContextSensitivity(
                Parser recognizer,
                DFA dfa,
                int startIndex,
                int stopIndex,
                int prediction,
                ATNConfigSet configs
            ) {}
        });
    }

    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
        @Override
        public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String message,
            RecognitionException e
        ) {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };
}
