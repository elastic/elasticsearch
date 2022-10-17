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
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ql.parser.CaseChangingCharStream;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.function.BiFunction;
import java.util.function.Function;

public class EsqlParser {

    private static final Logger log = LogManager.getLogger(EsqlParser.class);

    public LogicalPlan createStatement(String query) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as statement: {}", query);
        }
        return invokeParser(query, EsqlBaseParser::singleStatement, AstBuilder::plan);
    }

    private <T> T invokeParser(
        String query,
        Function<EsqlBaseParser, ParserRuleContext> parseFunction,
        BiFunction<AstBuilder, ParserRuleContext, T> result
    ) {
        try {
            EsqlBaseLexer lexer = new EsqlBaseLexer(new CaseChangingCharStream(CharStreams.fromString(query), false));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            EsqlBaseParser parser = new EsqlBaseParser(tokenStream);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            ParserRuleContext tree = parseFunction.apply(parser);

            if (log.isDebugEnabled()) {
                log.debug("Parse tree: {}", tree.toStringTree());
            }

            return result.apply(new AstBuilder(), tree);
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
}
