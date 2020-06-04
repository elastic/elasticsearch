/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ArithmeticUnaryContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ComparisonContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.DereferenceContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.FunctionExpressionContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinKeysContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.LogicalBinaryContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.LogicalNotContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.PredicateContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ValueExpressionDefaultContext;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.time.ZoneId;
import java.util.List;

import static java.util.Collections.emptyList;


public class ExpressionBuilder extends IdentifierBuilder {

    protected final ParserParams params;

    public ExpressionBuilder(ParserParams params) {
        this.params = params;
    }

    protected Expression expression(ParseTree ctx) {
        return typedParsing(ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(contexts, Expression.class);
    }

    @Override
    public Expression visitSingleExpression(EqlBaseParser.SingleExpressionContext ctx) {
        return expression(ctx.expression());
    }

    @Override
    public List<Attribute> visitJoinKeys(JoinKeysContext ctx) {
        return ctx != null ? visitList(ctx.expression(), Attribute.class) : emptyList();
    }

    @Override
    public Expression visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        Expression expr = expression(ctx.valueExpression());
        Source source = source(ctx);
        int type = ctx.operator.getType();

        return type == EqlBaseParser.MINUS ? new Neg(source, expr) : expr;
    }

    @Override
    public Expression visitArithmeticBinary(EqlBaseParser.ArithmeticBinaryContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        Source source = source(ctx);
        int type = ctx.operator.getType();

        switch (type) {
            case EqlBaseParser.ASTERISK:
                return new Mul(source, left, right);
            case EqlBaseParser.SLASH:
                return new Div(source, left, right);
            case EqlBaseParser.PERCENT:
                return new Mod(source, left, right);
            case EqlBaseParser.PLUS:
                return new Add(source, left, right);
            case EqlBaseParser.MINUS:
                return new Sub(source, left, right);
            default:
                throw new ParsingException(source, "Unknown arithmetic {}", source.text());
        }
    }

    @Override
    public Literal visitBooleanValue(EqlBaseParser.BooleanValueContext ctx) {
        Source source = source(ctx);
        return new Literal(source, ctx.TRUE() != null, DataTypes.BOOLEAN);
    }

    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        TerminalNode op = (TerminalNode) ctx.comparisonOperator().getChild(0);

        Source source = source(ctx);
        ZoneId zoneId = params.zoneId();

        switch (op.getSymbol().getType()) {
            case EqlBaseParser.EQ:
                return new Equals(source, left, right, zoneId);
            case EqlBaseParser.NEQ:
                return new NotEquals(source, left, right, zoneId);
            case EqlBaseParser.LT:
                return new LessThan(source, left, right, zoneId);
            case EqlBaseParser.LTE:
                return new LessThanOrEqual(source, left, right, zoneId);
            case EqlBaseParser.GT:
                return new GreaterThan(source, left, right, zoneId);
            case EqlBaseParser.GTE:
                return new GreaterThanOrEqual(source, left, right, zoneId);
            default:
                throw new ParsingException(source, "Unknown operator {}", source.text());
        }
    }

    @Override
    public Expression visitValueExpressionDefault(ValueExpressionDefaultContext ctx) {
        Expression expr = expression(ctx.primaryExpression());
        Source source = source(ctx);

        PredicateContext predicate = ctx.predicate();

        if (predicate == null) {
            return expr;
        }

        List<Expression> container = expressions(predicate.expression());
        Expression checkInSet = new In(source, expr, container);

        return predicate.NOT() != null ? new Not(source, checkInSet) : checkInSet;
    }

    @Override
    public Literal visitDecimalLiteral(EqlBaseParser.DecimalLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        try {
            return new Literal(source, Double.valueOf(StringUtils.parseDouble(text)), DataTypes.DOUBLE);
        } catch (QlIllegalArgumentException siae) {
            throw new ParsingException(source, siae.getMessage());
        }
    }

    @Override
    public UnresolvedAttribute visitDereference(DereferenceContext ctx) {
        return new UnresolvedAttribute(source(ctx), visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Function visitFunctionExpression(FunctionExpressionContext ctx) {
        Source source = source(ctx);
        String name = ctx.name.getText();
        List<Expression> arguments = expressions(ctx.expression());

        return new UnresolvedFunction(source, name, UnresolvedFunction.ResolutionType.STANDARD, arguments);
    }

    @Override
    public Literal visitIntegerLiteral(EqlBaseParser.IntegerLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        long value;

        try {
            value = Long.valueOf(StringUtils.parseLong(text));
        } catch (QlIllegalArgumentException siae) {
            // if it's too large, then quietly try to parse as a float instead
            try {
                return new Literal(source, Double.valueOf(StringUtils.parseDouble(text)), DataTypes.DOUBLE);
            } catch (QlIllegalArgumentException ignored) {
            }

            throw new ParsingException(source, siae.getMessage());
        }

        Object val = Long.valueOf(value);
        DataType type = DataTypes.LONG;

        // try to downsize to int if possible (since that's the most common type)
        if ((int) value == value) {
            type = DataTypes.INTEGER;
            val = Integer.valueOf((int) value);
        }
        return new Literal(source, val, type);
    }

    @Override
    public Expression visitLogicalBinary(LogicalBinaryContext ctx) {
        int type = ctx.operator.getType();
        Source source = source(ctx);
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        if (type == EqlBaseParser.AND) {
            return new And(source, left, right);
        } else {
            return new Or(source, left, right);
        }
    }

    @Override
    public Not visitLogicalNot(LogicalNotContext ctx) {
        return new Not(source(ctx), expression(ctx.booleanExpression()));
    }

    @Override
    public Literal visitNullLiteral(EqlBaseParser.NullLiteralContext ctx) {
        Source source = source(ctx);
        return new Literal(source, null, DataTypes.NULL);
    }

    @Override
    public Expression visitParenthesizedExpression(EqlBaseParser.ParenthesizedExpressionContext ctx) {
        return expression(ctx.expression());
    }

    @Override
    public Literal visitString(EqlBaseParser.StringContext ctx) {
        return new Literal(source(ctx), unquoteString(ctx.getText()), DataTypes.KEYWORD);
    }
}
