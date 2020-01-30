/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonToken;
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
import org.antlr.v4.runtime.misc.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;

public class EqlParser {

    private static final Logger log = LogManager.getLogger();

    private final boolean DEBUG = true;

    /**
     * Parses an EQL statement into execution plan
     * @param eql - the EQL statement
     */
    public Expression createStatement(String eql) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as statement: {}", eql);
        }
        return invokeParser(eql, EqlBaseParser::singleStatement, AstBuilder::expression);
    }

    public Expression createExpression(String expression) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as expression: {}", expression);
        }

        return invokeParser(expression, EqlBaseParser::singleExpression, AstBuilder::expression);
    }

    private <T> T invokeParser(String sql,
            Function<EqlBaseParser, ParserRuleContext> parseFunction,
                               BiFunction<AstBuilder, ParserRuleContext, T> visitor) {
        try {
            EqlBaseLexer lexer = new EqlBaseLexer(new CaseInsensitiveStream(sql));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            EqlBaseParser parser = new EqlBaseParser(tokenStream);

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames())));

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            if (DEBUG) {
                debug(parser);
                tokenStream.fill();

                for (Token t : tokenStream.getTokens()) {
                    String symbolicName = EqlBaseLexer.VOCABULARY.getSymbolicName(t.getType());
                    String literalName = EqlBaseLexer.VOCABULARY.getLiteralName(t.getType());
                    log.info(format(Locale.ROOT, "  %-15s '%s'",
                        symbolicName == null ? literalName : symbolicName,
                        t.getText()));
                }
            }

            ParserRuleContext tree = parseFunction.apply(parser);

            if (DEBUG) {
                log.info("Parse tree {} " + tree.toStringTree());
            }

            return visitor.apply(new AstBuilder(), tree);
        } catch (StackOverflowError e) {
            throw new ParsingException("SQL statement is too large, " +
                "causing stack overflow when generating the parsing tree: [{}]", sql);
        }
    }

    private static void debug(EqlBaseParser parser) {

        // when debugging, use the exact prediction mode (needed for diagnostics as well)
        parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);

        parser.addParseListener(parser.new TraceListener());

        parser.addErrorListener(new DiagnosticErrorListener(false) {
            @Override
            public void reportAttemptingFullContext(Parser recognizer, DFA dfa,
                    int startIndex, int stopIndex, BitSet conflictingAlts, ATNConfigSet configs) {}

            @Override
            public void reportContextSensitivity(Parser recognizer, DFA dfa,
                    int startIndex, int stopIndex, int prediction, ATNConfigSet configs) {}
        });
    }

    private class PostProcessor extends EqlBaseBaseListener {
        private final List<String> ruleNames;

        PostProcessor(List<String> ruleNames) {
            this.ruleNames = ruleNames;
        }

        @Override
        public void exitDigitIdentifier(EqlBaseParser.DigitIdentifierContext context) {
            Token token = context.DIGIT_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "identifiers must not start with a digit; please use double quotes",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine());
        }

        @Override
        public void exitQuotedIdentifier(EqlBaseParser.QuotedIdentifierContext context) {
            // Remove quotes
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            context.getParent().addChild(new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    EqlBaseLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex() + 1,
                    token.getStopIndex() - 1));
        }
    }

    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                int charPositionInLine, String message, RecognitionException e) {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };
}