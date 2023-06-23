/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenFactory;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ql.parser.CaseChangingCharStream;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class EsqlParser {

    private static final Logger log = LogManager.getLogger(EsqlParser.class);

    public LogicalPlan createStatement(String query) {
        return createStatement(query, List.of());
    }

    public LogicalPlan createStatement(String query, List<TypedParamValue> params) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as statement: {}", query);
        }
        return invokeParser(query, params, EsqlBaseParser::singleStatement, AstBuilder::plan);
    }

    private <T> T invokeParser(
        String query,
        List<TypedParamValue> params,
        Function<EsqlBaseParser, ParserRuleContext> parseFunction,
        BiFunction<AstBuilder, ParserRuleContext, T> result
    ) {
        try {
            EsqlBaseLexer lexer = new EsqlBaseLexer(new CaseChangingCharStream(CharStreams.fromString(query), false));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            Map<Token, TypedParamValue> paramTokens = new HashMap<>();
            TokenSource tokenSource = new ParametrizedTokenSource(lexer, paramTokens, params);

            CommonTokenStream tokenStream = new CommonTokenStream(tokenSource);
            EsqlBaseParser parser = new EsqlBaseParser(tokenStream);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            ParserRuleContext tree = parseFunction.apply(parser);

            if (log.isDebugEnabled()) {
                log.debug("Parse tree: {}", tree.toStringTree());
            }

            return result.apply(new AstBuilder(paramTokens), tree);
        } catch (StackOverflowError e) {
            throw new ParsingException("ESQL statement is too large, causing stack overflow when generating the parsing tree: [{}]", query);
        }
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

    /**
     * Finds all parameter tokens (?) and associates them with actual parameter values
     * <p>
     * Parameters are positional and we know where parameters occurred in the original stream in order to associate them
     * with actual values.
     */
    private static class ParametrizedTokenSource implements TokenSource {

        private TokenSource delegate;
        private Map<Token, TypedParamValue> paramTokens;
        private int param;
        private List<TypedParamValue> params;

        ParametrizedTokenSource(TokenSource delegate, Map<Token, TypedParamValue> paramTokens, List<TypedParamValue> params) {
            this.delegate = delegate;
            this.paramTokens = paramTokens;
            this.params = params;
            param = 0;
        }

        @Override
        public Token nextToken() {
            Token token = delegate.nextToken();
            if (token.getType() == EsqlBaseLexer.PARAM) {
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
