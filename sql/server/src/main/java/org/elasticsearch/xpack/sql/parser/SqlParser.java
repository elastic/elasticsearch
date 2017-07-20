/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;

import java.util.function.Function;

public class SqlParser {

    private static final Logger log = Loggers.getLogger(SqlParser.class);

    public LogicalPlan createStatement(String sql) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as statement: {}", sql);
        }
        return invokeParser("statement", sql, SqlBaseParser::singleStatement);
    }

    public Expression createExpression(String expression) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as expression: {}", expression);
        }

        return invokeParser("expression", expression, SqlBaseParser::singleExpression);
    }

    @SuppressWarnings("unchecked")
    private <T> T invokeParser(String name, String sql, Function<SqlBaseParser, ParserRuleContext> parseFunction) {
        try {
            SqlBaseLexer lexer = new SqlBaseLexer(new CaseInsensitiveStream(sql));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            SqlBaseParser parser = new SqlBaseParser(tokenStream);

            parser.addParseListener(new PostProcessor());
            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply(parser);
            }
            catch (Exception ex) {
                // if we fail, parse with LL mode
                tokenStream.reset(); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
                tree = parseFunction.apply(parser);
            }

            postProcess(lexer, parser, tree);

            return (T) new AstBuilder().visit(tree);
        }

        catch (StackOverflowError e) {
            throw new ParsingException(name + " is too large (stack overflow while parsing)");
        }
    }

    protected void postProcess(SqlBaseLexer lexer, SqlBaseParser parser, ParserRuleContext tree) {
        // no-op
    }

    private class PostProcessor extends SqlBaseBaseListener {
        @Override
        public void exitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext context) {
            Token token = context.BACKQUOTED_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "backquoted identifiers are not supported; use double quotes to quote identifiers",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine());
        }

        @Override
        public void exitDigitIdentifier(SqlBaseParser.DigitIdentifierContext context) {
            Token token = context.DIGIT_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "identifiers must not start with a digit; surround the identifier with double quotes",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine());
        }

        @Override
        public void exitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context) {
            // Remove quotes
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            context.getParent().addChild(new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    SqlBaseLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex() + 1,
                    token.getStopIndex() - 1));
        }

        @Override
        public void exitNonReserved(SqlBaseParser.NonReservedContext context) {
            // replace nonReserved words with IDENT tokens
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            context.getParent().addChild(new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    SqlBaseLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex(),
                    token.getStopIndex()));
        }
    }

    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e) {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };
}