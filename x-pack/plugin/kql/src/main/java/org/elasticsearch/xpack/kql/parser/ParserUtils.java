/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;


import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.LoggerMessageFormat;

public final class ParserUtils {

    private ParserUtils() {}

    @SuppressWarnings("unchecked")
    public static <T> T typedParsing(ParseTreeVisitor<?> visitor, ParserRuleContext ctx, Class<T> type) {
        Object result = ctx.accept(visitor);
        if (type.isInstance(result)) {
            return (T) result;
        }

        throw new ParsingException(ctx.start.getLine(), ctx.start.getCharPositionInLine(),
            LoggerMessageFormat.format(
            "Invalid query '{}'[{}] given; expected {} but found {}", ctx.getText(),
                ctx.getClass().getSimpleName(),
                type.getSimpleName(),
                (result != null ? result.getClass().getSimpleName() : "null")
            ),
            null
        );
    }

}
