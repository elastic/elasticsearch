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
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;

public class PromqlParser {

    private static final Logger log = LogManager.getLogger(PromqlParser.class);

    private final boolean DEBUG = false;

    /**
     * Parses an PromQL expression into execution plan
     */
    public LogicalPlan createStatement(String query) {
        return createStatement(query, null, null, 0, 0);
    }

    public LogicalPlan createStatement(String query, Literal start, Literal end, int startLine, int startColumn) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as expression: {}", query);
        }

        return invokeParser(query, start, end, startLine, startColumn, PromqlBaseParser::singleStatement, PromqlAstBuilder::plan);
    }

    private <T> T invokeParser(
        String query,
        Literal start,
        Literal end,
        int startLine,
        int startColumn,
        Function<PromqlBaseParser, ParserRuleContext> parseFunction,
        BiFunction<PromqlAstBuilder, ParserRuleContext, T> visitor
    ) {
        try {
            PromqlBaseLexer lexer = new PromqlBaseLexer(CharStreams.fromString(query));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
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
            return visitor.apply(new PromqlAstBuilder(start, end, startLine, startColumn), tree);
        } catch (StackOverflowError e) {
            throw new ParsingException(
                "PromQL statement is too large, causing stack overflow when generating the parsing tree: [{}]",
                query
            );
        } catch (EmptyStackException ese) {
            throw new ParsingException("Invalid query [{}]", query);
        }
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
