/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DiagnosticErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenFactory;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class SqlParser {

    private static final Logger log = LogManager.getLogger();

    private final boolean DEBUG = false;

    /**
     * Used only in tests
     */
    public LogicalPlan createStatement(String sql) {
        return createStatement(sql, Collections.emptyList(), UTC);
    }

    /**
     * Used only in tests
     */
    public LogicalPlan createStatement(String sql, ZoneId zoneId) {
        return createStatement(sql, Collections.emptyList(), zoneId);
    }

    /**
     * Parses an SQL statement into execution plan
     * @param sql - the SQL statement
     * @param params - a list of parameters for the statement if the statement is parametrized
     * @return logical plan
     */
    public LogicalPlan createStatement(String sql, List<SqlTypedParamValue> params, ZoneId zoneId) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as statement: {}", sql);
        }
        return invokeParser(sql, params, zoneId, SqlBaseParser::singleStatement, AstBuilder::plan);
    }

    /**
     * Parses an expression - used only in tests
     */
    public Expression createExpression(String expression) {
        return createExpression(expression, Collections.emptyList());
    }

    /**
     * Parses an expression - Used only in tests
     */
    public Expression createExpression(String expression, List<SqlTypedParamValue> params) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as expression: {}", expression);
        }

        return invokeParser(expression, params, UTC, SqlBaseParser::singleExpression, AstBuilder::expression);
    }

    private <T> T invokeParser(String sql,
                               List<SqlTypedParamValue> params,
                               ZoneId zoneId,
                               Function<SqlBaseParser, ParserRuleContext> parseFunction,
                               BiFunction<AstBuilder, ParserRuleContext, T> visitor) {
        try {
            SqlBaseLexer lexer = new SqlBaseLexer(new CaseInsensitiveStream(sql));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            Map<Token, SqlTypedParamValue> paramTokens = new HashMap<>();
            TokenSource tokenSource = new ParametrizedTokenSource(lexer, paramTokens, params);

            CommonTokenStream tokenStream = new CommonTokenStream(tokenSource);
            SqlBaseParser parser = new SqlBaseParser(tokenStream);

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames())));

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            if (DEBUG) {
                debug(parser);
                tokenStream.fill();

                for (Token t : tokenStream.getTokens()) {
                    String symbolicName = SqlBaseLexer.VOCABULARY.getSymbolicName(t.getType());
                    String literalName = SqlBaseLexer.VOCABULARY.getLiteralName(t.getType());
                    log.info(format(Locale.ROOT, "  %-15s '%s'",
                        symbolicName == null ? literalName : symbolicName,
                        t.getText()));
                }
            }

            ParserRuleContext tree = parseFunction.apply(parser);

            if (DEBUG) {
                log.info("Parse tree {} " + tree.toStringTree());
            }

            return visitor.apply(new AstBuilder(paramTokens, zoneId), tree);
        } catch (StackOverflowError e) {
            throw new ParsingException("SQL statement is too large, " +
                "causing stack overflow when generating the parsing tree: [{}]", sql);
        }
    }

    private static void debug(SqlBaseParser parser) {

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

    private class PostProcessor extends SqlBaseBaseListener {
        private final List<String> ruleNames;

        PostProcessor(List<String> ruleNames) {
            this.ruleNames = ruleNames;
        }

        @Override
        public void exitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext context) {
            Token token = context.BACKQUOTED_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "backquoted identifiers not supported; please use double quotes instead",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine());
        }

        @Override
        public void exitDigitIdentifier(SqlBaseParser.DigitIdentifierContext context) {
            Token token = context.DIGIT_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "identifiers must not start with a digit; please use double quotes",
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
            // tree cannot be modified during rule enter/exit _unless_ it's a terminal node
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new ParsingException("nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
            }

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
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                int charPositionInLine, String message, RecognitionException e) {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };

    /**
     * Finds all parameter tokens (?) and associates them with actual parameter values
     * <p>
     * Parameters are positional and we know where parameters occurred in the original stream in order to associate them
     * with actual values.
     */
    private static class ParametrizedTokenSource implements TokenSource {

        private TokenSource delegate;
        private Map<Token, SqlTypedParamValue> paramTokens;
        private int param;
        private List<SqlTypedParamValue> params;

        ParametrizedTokenSource(TokenSource delegate, Map<Token, SqlTypedParamValue> paramTokens, List<SqlTypedParamValue> params) {
            this.delegate = delegate;
            this.paramTokens = paramTokens;
            this.params = params;
            param = 0;
        }

        @Override
        public Token nextToken() {
            Token token = delegate.nextToken();
            if (token.getType() == SqlBaseLexer.PARAM) {
                if (param >= params.size()) {
                    throw new ParsingException("Not enough actual parameters {} ", params.size());
                }
                paramTokens.put(token, params.get(param));
                param++;
            }
            return token;
        }

        @Override
        public int getLine() {
            return delegate.getLine();
        }

        @Override
        public int getCharPositionInLine() {
            return delegate.getCharPositionInLine();
        }

        @Override
        public CharStream getInputStream() {
            return delegate.getInputStream();
        }

        @Override
        public String getSourceName() {
            return delegate.getSourceName();
        }

        @Override
        public void setTokenFactory(TokenFactory<?> factory) {
            delegate.setTokenFactory(factory);
        }

        @Override
        public TokenFactory<?> getTokenFactory() {
            return delegate.getTokenFactory();
        }
    }
}
