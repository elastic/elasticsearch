/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.typedParsing;

public class ExpressionBuilder extends IdentifierBuilder {
    protected Expression expression(ParseTree ctx) {
        return typedParsing(this, ctx, Expression.class);
    }

    @Override
    public Expression visitExpression(EsqlBaseParser.ExpressionContext ctx) {
        Source source = source(ctx);
        try {
            int value = Integer.parseInt(ctx.getText());
            return new Literal(source, value, DataTypes.INTEGER);
        } catch (NumberFormatException nfe) {
            throw new ParsingException(source, nfe.getMessage());
        }
    }

    @Override
    public Alias visitField(EsqlBaseParser.FieldContext ctx) {
        String id = ctx.identifier() == null ? ctx.getText() : ctx.identifier().getText();
        return new Alias(source(ctx), id, visitExpression(ctx.expression()));
    }
}
