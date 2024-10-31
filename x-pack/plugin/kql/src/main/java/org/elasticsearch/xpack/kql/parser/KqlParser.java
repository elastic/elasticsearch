/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.function.BiFunction;
import java.util.function.Function;

public class KqlParser {
    private static final Logger log = LogManager.getLogger(KqlParser.class);

    public QueryBuilder parseKqlQuery(String kqlQuery, SearchExecutionContext searchExecutionContext) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing KQL query: {}", kqlQuery);
        }

        return invokeParser(kqlQuery, searchExecutionContext, KqlBaseParser::topLevelQuery, KqlAstBuilder::toQueryBuilder);
    }

    private <T> T invokeParser(
        String kqlQuery,
        SearchExecutionContext searchExecutionContext,
        Function<KqlBaseParser, ParserRuleContext> parseFunction,
        BiFunction<KqlAstBuilder, ParserRuleContext, T> visitor
    ) {
        KqlBaseLexer lexer = new KqlBaseLexer(CharStreams.fromString(kqlQuery));

        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);

        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        KqlBaseParser parser = new KqlBaseParser(tokenStream);

        parser.removeErrorListeners();
        parser.addErrorListener(ERROR_LISTENER);

        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

        ParserRuleContext tree = parseFunction.apply(parser);

        if (log.isTraceEnabled()) {
            log.trace("Parse tree: {}", tree.toStringTree());
        }

        return visitor.apply(new KqlAstBuilder(searchExecutionContext), tree);
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
            throw new KqlParsingException(message, line, charPositionInLine, e);
        }
    };
}
