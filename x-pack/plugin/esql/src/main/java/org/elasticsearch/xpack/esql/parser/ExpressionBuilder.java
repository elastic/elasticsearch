/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
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
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DateUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.text;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.typedParsing;

public class ExpressionBuilder extends IdentifierBuilder {
    protected Expression expression(ParseTree ctx) {
        return typedParsing(this, ctx, Expression.class);
    }

    @Override
    public Expression visitSingleExpression(EsqlBaseParser.SingleExpressionContext ctx) {
        return expression(ctx.booleanExpression());
    }

    @Override
    public Literal visitBooleanValue(EsqlBaseParser.BooleanValueContext ctx) {
        Source source = source(ctx);
        return new Literal(source, ctx.TRUE() != null, DataTypes.BOOLEAN);
    }

    @Override
    public Literal visitDecimalLiteral(EsqlBaseParser.DecimalLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        try {
            return new Literal(source, Double.valueOf(StringUtils.parseDouble(text)), DataTypes.DOUBLE);
        } catch (QlIllegalArgumentException siae) {
            throw new ParsingException(source, siae.getMessage());
        }
    }

    @Override
    public Literal visitIntegerLiteral(EsqlBaseParser.IntegerLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();
        long value;

        try {
            value = Long.valueOf(StringUtils.parseLong(text));
        } catch (QlIllegalArgumentException siae) {
            // if it's too large, then quietly try to parse as a float instead
            try {
                return new Literal(source, Double.valueOf(StringUtils.parseDouble(text)), DataTypes.DOUBLE);
            } catch (QlIllegalArgumentException ignored) {}

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
    public Literal visitNullLiteral(EsqlBaseParser.NullLiteralContext ctx) {
        Source source = source(ctx);
        return new Literal(source, null, DataTypes.NULL);
    }

    @Override
    public Literal visitStringLiteral(EsqlBaseParser.StringLiteralContext ctx) {
        Source source = source(ctx.string());
        return new Literal(source, unquoteString(source), DataTypes.KEYWORD);
    }

    @Override
    public Expression visitArithmeticUnary(EsqlBaseParser.ArithmeticUnaryContext ctx) {
        Expression expr = expression(ctx.operatorExpression());
        Source source = source(ctx);
        int type = ctx.operator.getType();

        // TODO we could handle this a bit better (like ES SQL does it) so that -(-(-123)) results in the -123 the Literal
        return type == EsqlBaseParser.MINUS ? new Neg(source, expr) : expr;
    }

    @Override
    public Expression visitArithmeticBinary(EsqlBaseParser.ArithmeticBinaryContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        Source source = source(ctx);
        int type = ctx.operator.getType();

        return switch (type) {
            case EsqlBaseParser.ASTERISK -> new Mul(source, left, right);
            case EsqlBaseParser.SLASH -> new Div(source, left, right);
            case EsqlBaseParser.PERCENT -> new Mod(source, left, right);
            case EsqlBaseParser.PLUS -> new Add(source, left, right);
            case EsqlBaseParser.MINUS -> new Sub(source, left, right);
            default -> throw new ParsingException(source, "Unknown arithmetic operator {}", source.text());
        };
    }

    @Override
    public Expression visitComparison(EsqlBaseParser.ComparisonContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        TerminalNode op = (TerminalNode) ctx.comparisonOperator().getChild(0);

        Source source = source(ctx);
        ZoneId zoneId = DateUtils.UTC;

        return switch (op.getSymbol().getType()) {
            case EsqlBaseParser.EQ -> new Equals(source, left, right, zoneId);
            case EsqlBaseParser.NEQ -> new Not(source, new Equals(source, left, right, zoneId));
            case EsqlBaseParser.LT -> new LessThan(source, left, right, zoneId);
            case EsqlBaseParser.LTE -> new LessThanOrEqual(source, left, right, zoneId);
            case EsqlBaseParser.GT -> new GreaterThan(source, left, right, zoneId);
            case EsqlBaseParser.GTE -> new GreaterThanOrEqual(source, left, right, zoneId);
            default -> throw new ParsingException(source, "Unknown comparison operator {}", source.text());
        };
    }

    @Override
    public Not visitLogicalNot(EsqlBaseParser.LogicalNotContext ctx) {
        return new Not(source(ctx), expression(ctx.booleanExpression()));
    }

    @Override
    public Expression visitParenthesizedExpression(EsqlBaseParser.ParenthesizedExpressionContext ctx) {
        return expression(ctx.booleanExpression());
    }

    @Override
    public Expression visitOperatorExpressionDefault(EsqlBaseParser.OperatorExpressionDefaultContext ctx) {
        return expression(ctx.primaryExpression());
    }

    @Override
    public UnresolvedAttribute visitDereference(EsqlBaseParser.DereferenceContext ctx) {
        Source source = source(ctx);
        EsqlBaseParser.QualifiedNameContext qContext = ctx.qualifiedName();
        String name = visitQualifiedName(qContext);
        return new UnresolvedAttribute(source, name);
    }

    @Override
    public Expression visitLogicalBinary(EsqlBaseParser.LogicalBinaryContext ctx) {
        int type = ctx.operator.getType();
        Source source = source(ctx);
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        return type == EsqlBaseParser.AND ? new And(source, left, right) : new Or(source, left, right);
    }

    private static String unquoteString(Source source) {
        String text = source.text();
        if (text == null) {
            return null;
        }

        // unescaped strings can be interpreted directly
        if (text.startsWith("\"\"\"")) {
            return text.substring(3, text.length() - 3);
        }

        text = text.substring(1, text.length() - 1);
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < text.length();) {
            if (text.charAt(i) == '\\') {
                // ANTLR4 Grammar guarantees there is always a character after the `\`
                switch (text.charAt(++i)) {
                    case 't' -> sb.append('\t');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case '"' -> sb.append('\"');
                    case '\\' -> sb.append('\\');

                    // will be interpreted as regex, so we have to escape it
                    default ->
                        // unknown escape sequence, pass through as-is, e.g: `...\w...`
                        sb.append('\\').append(text.charAt(i));
                }
                i++;
            } else {
                sb.append(text.charAt(i++));
            }
        }
        return sb.toString();
    }
}
