/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;


import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.eql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ArithmeticBinaryContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ArithmeticUnaryContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.BooleanDefaultContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.BooleanLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ComparisonContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ConstantDefaultContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.DecimalLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.DereferenceContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ExpressionContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.LogicalBinaryContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.LogicalNotContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.NullLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.NumberContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.NumericLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ParenthesizedExpressionContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.PredicateContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.PredicatedContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.StringContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.StringLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ValueExpressionDefaultContext;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.predicate.Range;
import org.elasticsearch.xpack.sql.expression.predicate.logical.And;
import org.elasticsearch.xpack.sql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.sql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.List;
import java.util.Locale;

public class ExpressionBuilder extends IdentifierBuilder {

    @Override
    protected Expression expression(ParseTree ctx) {
        return typedParsing(ctx, Expression.class);
    }

    @Override
    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(contexts, Expression.class);
    }

    @Override
    public Object visitDereference(DereferenceContext ctx) {
        return new UnresolvedAttribute(source(ctx), visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        TerminalNode op = (TerminalNode) ctx.comparisonOperator().getChild(0);

        Source source = source(ctx);

        switch (op.getSymbol().getType()) {
            case EqlBaseParser.EQ:
                return new Equals(source, left, right);
            case EqlBaseParser.NEQ:
                return new NotEquals(source, left, right);
            case EqlBaseParser.LT:
                return new LessThan(source, left, right);
            case EqlBaseParser.LTE:
                return new LessThanOrEqual(source, left, right);
            case EqlBaseParser.GT:
                return new GreaterThan(source, left, right);
            case EqlBaseParser.GTE:
                return new GreaterThanOrEqual(source, left, right);
            default:
                throw new ParsingException(source, "Unknown operator {}", source.text());
        }
    }

    @Override
    public Expression visitPredicated(PredicatedContext ctx) {
        Expression exp = expression(ctx.valueExpression());

        // no predicate, quick exit
        if (ctx.predicate() == null) {
            return exp;
        }

        PredicateContext pCtx = ctx.predicate();
        Source source = source(ctx.valueExpression(), ctx);

        Expression e = null;
        switch (pCtx.kind.getType()) {
            case EqlBaseParser.BETWEEN:
                e = new Range(source, exp, expression(pCtx.lower), true, expression(pCtx.upper), true);
                break;
            case EqlBaseParser.IN:
                if (pCtx.query() != null) {
                    throw new ParsingException(source, "IN query not supported yet");
                }
                e = new In(source, exp, expressions(pCtx.valueExpression()));
                break;
            case EqlBaseParser.NULL:
                // shortcut to avoid double negation later on (since there's no IsNull (missing in ES is a negated exists))
                if (pCtx.NOT() != null) {
                    return new IsNotNull(source, exp);
                } else {
                    return new IsNull(source, exp);
                }
            default:
                throw new ParsingException(source, "Unknown predicate {}", source.text());
        }

        return pCtx.NOT() != null ? new Not(source, e) : e;
    }

    //
    // Arithmetic
    //
    @Override
    public Object visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        Expression value = expression(ctx.valueExpression());
        Source source = source(ctx);

        switch (ctx.operator.getType()) {
            case EqlBaseParser.PLUS:
                return value;
            case EqlBaseParser.MINUS:
                if (value instanceof Literal) { // Minus already processed together with literal number
                    return value;
                }
                return new Neg(source(ctx), value);
            default:
                throw new ParsingException(source, "Unknown arithmetic {}", source.text());
        }
    }

    @Override
    public Object visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        Source source = source(ctx);

        switch (ctx.operator.getType()) {
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
    public Expression visitParenthesizedExpression(ParenthesizedExpressionContext ctx) {
        return expression(ctx.expression());
    }

    //
    // Logical constructs
    //

    @Override
    public Object visitLogicalNot(LogicalNotContext ctx) {
        return new Not(source(ctx), expression(ctx.booleanExpression()));
    }

    @Override
    public Object visitLogicalBinary(LogicalBinaryContext ctx) {
        int type = ctx.operator.getType();
        Source source = source(ctx);
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        if (type == EqlBaseLexer.AND) {
            return new And(source, left, right);
        }
        if (type == EqlBaseLexer.OR) {
            return new Or(source, left, right);
        }
        throw new ParsingException(source, "Don't know how to parse {}", ctx);
    }

    //
    // Literal
    //

    @Override
    public Expression visitNullLiteral(NullLiteralContext ctx) {
        return new Literal(source(ctx), null, DataType.NULL);
    }

    @Override
    public Expression visitBooleanLiteral(BooleanLiteralContext ctx) {
        boolean value;
        try {
            value = Booleans.parseBoolean(ctx.getText().toLowerCase(Locale.ROOT), false);
        } catch (IllegalArgumentException iae) {
            throw new ParsingException(source(ctx), iae.getMessage());
        }
        return new Literal(source(ctx), Boolean.valueOf(value), DataType.BOOLEAN);
    }

    @Override
    public Expression visitStringLiteral(StringLiteralContext ctx) {
        StringBuilder sb = new StringBuilder();
        for (TerminalNode node : ctx.STRING()) {
            sb.append(unquoteString(text(node)));
        }
        return new Literal(source(ctx), sb.toString(), DataType.KEYWORD);
    }

    @Override
    public Literal visitDecimalLiteral(DecimalLiteralContext ctx) {
        Tuple<Source, String> tuple = withMinus(ctx);

        try {
            return new Literal(tuple.v1(), Double.valueOf(StringUtils.parseDouble(tuple.v2())), DataType.DOUBLE);
        } catch (SqlIllegalArgumentException siae) {
            throw new ParsingException(tuple.v1(), siae.getMessage());
        }
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        Tuple<Source, String> tuple = withMinus(ctx);

        long value;
        try {
            value = Long.valueOf(StringUtils.parseLong(tuple.v2()));
        } catch (SqlIllegalArgumentException siae) {
            throw new ParsingException(tuple.v1(), siae.getMessage());
        }

        Object val = Long.valueOf(value);
        DataType type = DataType.LONG;
        // try to downsize to int if possible (since that's the most common type)
        if ((int) value == value) {
            type = DataType.INTEGER;
            val = Integer.valueOf((int) value);
        }
        return new Literal(tuple.v1(), val, type);
    }

    @Override
    public String visitString(StringContext ctx) {
        return string(ctx);
    }

    /**
     * Extracts the string (either as unescaped literal) or parameter.
     */
    String string(StringContext ctx) {
        if (ctx == null) {
            return null;
        }
        return unquoteString(ctx.getText());
    }

    /**
     * Return the source and the value of the given number,
     * taking into account MINUS (-) if needed.
     */
    private static Tuple<Source, String> withMinus(NumberContext ctx) {
        String string = ctx.getText();
        Source source = minusAwareSource(ctx);

        if (source != null) {
            string = "-" + string;
        } else {
            source = source(ctx);
        }

        return new Tuple<>(source, string);
    }

    /**
     * Checks the presence of MINUS (-) in the parent and if found,
     * returns the parent source or null otherwise.
     * Parsing of the value should not depend on the returned source
     * as it might contain extra spaces.
     */
    private static Source minusAwareSource(NumberContext ctx) {
        ParserRuleContext parentCtx = ctx.getParent();
        if (parentCtx != null) {
            if (parentCtx instanceof NumericLiteralContext) {
                parentCtx = parentCtx.getParent();
                if (parentCtx instanceof ConstantDefaultContext) {
                    parentCtx = parentCtx.getParent();
                    if (parentCtx instanceof ValueExpressionDefaultContext) {
                        parentCtx = parentCtx.getParent();

                        // Skip parentheses, e.g.: - (( (2.15) ) )
                        while (parentCtx instanceof PredicatedContext) {
                            parentCtx = parentCtx.getParent();
                            if (parentCtx instanceof BooleanDefaultContext) {
                                parentCtx = parentCtx.getParent();
                            }
                            if (parentCtx instanceof ExpressionContext) {
                                parentCtx = parentCtx.getParent();
                            }
                            if (parentCtx instanceof ParenthesizedExpressionContext) {
                                parentCtx = parentCtx.getParent();
                            }
                            if (parentCtx instanceof ValueExpressionDefaultContext) {
                                parentCtx = parentCtx.getParent();
                            }
                        }
                        if (parentCtx instanceof ArithmeticUnaryContext) {
                            if (((ArithmeticUnaryContext) parentCtx).MINUS() != null) {
                                return source(parentCtx);
                            }
                        }
                    }
                }
            }
        }
        return null;
    }
}
