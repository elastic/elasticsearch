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
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.BitSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.isInteger;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

public class EsqlParser {

    private static final Logger log = LogManager.getLogger(EsqlParser.class);

    private EsqlConfig config = new EsqlConfig();

    public EsqlConfig config() {
        return config;
    }

    public void setEsqlConfig(EsqlConfig config) {
        this.config = config;
    }

    public LogicalPlan createStatement(String query) {
        return createStatement(query, new QueryParams());
    }

    public LogicalPlan createStatement(String query, QueryParams params) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as statement: {}", query);
        }
        return invokeParser(query, params, EsqlBaseParser::singleStatement, AstBuilder::plan);
    }

    private <T> T invokeParser(
        String query,
        QueryParams params,
        Function<EsqlBaseParser, ParserRuleContext> parseFunction,
        BiFunction<AstBuilder, ParserRuleContext, T> result
    ) {
        try {
            // new CaseChangingCharStream()
            EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(query));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            lexer.setEsqlConfig(config);

            TokenSource tokenSource = new ParametrizedTokenSource(lexer, params);
            CommonTokenStream tokenStream = new CommonTokenStream(tokenSource);
            EsqlBaseParser parser = new EsqlBaseParser(tokenStream);

            parser.addParseListener(new PostProcessor());

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            parser.setEsqlConfig(config);

            ParserRuleContext tree = parseFunction.apply(parser);

            if (log.isTraceEnabled()) {
                log.trace("Parse tree: {}", tree.toStringTree());
            }

            return result.apply(new AstBuilder(params), tree);
        } catch (StackOverflowError e) {
            throw new ParsingException("ESQL statement is too large, causing stack overflow when generating the parsing tree: [{}]", query);
        }
    }

    private class PostProcessor extends EsqlBaseParserBaseListener {
        @Override
        public void exitFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx) {
            // TODO remove this at some point
            EsqlBaseParser.IdentifierContext identifier = ctx.identifier();
            if (identifier.getText().equalsIgnoreCase("is_null")) {
                throw new ParsingException(
                    source(ctx),
                    "is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
                );
            }
        }
    }

    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
        // replace entries that start with <comma?><space?>DEV_<space?>
        private final Pattern REPLACE_DEV = Pattern.compile(",*\\s*DEV_\\w+\\s*");

        @Override
        public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String message,
            RecognitionException e
        ) {
            if (recognizer instanceof EsqlBaseParser parser && parser.isDevVersion() == false) {
                Matcher m = REPLACE_DEV.matcher(message);
                message = m.replaceAll(StringUtils.EMPTY);
            }

            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };

    /**
     * Finds all parameter tokens (?) and associates them with actual parameter values.
     * <p>
     * Parameters are positional and we know where parameters occurred in the original stream in order to associate them
     * with actual values.
     */
    private static class ParametrizedTokenSource extends DelegatingTokenSource {
        private static String message = "Inconsistent parameter declaration, "
            + "use one of positional, named or anonymous params but not a combination of ";

        private QueryParams params;
        private BitSet paramTypes = new BitSet(3);
        private int param = 1;

        ParametrizedTokenSource(TokenSource delegate, QueryParams params) {
            super(delegate);
            this.params = params;
        }

        @Override
        public Token nextToken() {
            Token token = delegate.nextToken();
            if (token.getType() == EsqlBaseLexer.PARAM) {
                checkAnonymousParam(token);
                if (param > params.size()) {
                    throw new ParsingException(source(token), "Not enough actual parameters {}", params.size());
                }
                params.addTokenParam(token, params.get(param));
                param++;
            }

            if (token.getType() == EsqlBaseLexer.NAMED_OR_POSITIONAL_PARAM) {
                if (isInteger(token.getText().substring(1))) {
                    checkPositionalParam(token);
                } else {
                    checkNamedParam(token);
                }
            }
            return token;
        }

        private void checkAnonymousParam(Token token) {
            paramTypes.set(0);
            if (paramTypes.cardinality() > 1) {
                throw new ParsingException(source(token), message + "anonymous and " + (paramTypes.get(1) ? "named" : "positional"));
            }
        }

        private void checkNamedParam(Token token) {
            paramTypes.set(1);
            if (paramTypes.cardinality() > 1) {
                throw new ParsingException(source(token), message + "named and " + (paramTypes.get(0) ? "anonymous" : "positional"));
            }
        }

        private void checkPositionalParam(Token token) {
            paramTypes.set(2);
            if (paramTypes.cardinality() > 1) {
                throw new ParsingException(source(token), message + "positional and " + (paramTypes.get(0) ? "anonymous" : "named"));
            }
        }
    }
}
