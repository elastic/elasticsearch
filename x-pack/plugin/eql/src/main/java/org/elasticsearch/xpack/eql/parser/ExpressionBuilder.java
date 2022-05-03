/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.eql.expression.OptionalUnresolvedAttribute;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionResolution;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.Match;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveWildcardEquals;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ArithmeticUnaryContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.ComparisonContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.DereferenceContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.FunctionExpressionContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinKeysContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.LogicalBinaryContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.LogicalNotContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.PredicateContext;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
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
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.time.ZoneId;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.eql.util.StringUtils.toLikePattern;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.visitList;

public class ExpressionBuilder extends IdentifierBuilder {

    protected final ParserParams params;

    public ExpressionBuilder(ParserParams params) {
        this.params = params;
    }

    protected Expression expression(ParseTree ctx) {
        return typedParsing(this, ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(this, contexts, Expression.class);
    }

    @Override
    public Expression visitSingleExpression(EqlBaseParser.SingleExpressionContext ctx) {
        return expression(ctx.expression());
    }

    @Override
    public List<Attribute> visitJoinKeys(JoinKeysContext ctx) {
        try {
            return ctx != null ? visitList(this, ctx.expression(), Attribute.class) : emptyList();
        } catch (ClassCastException ex) {
            Source source = source(ctx);
            throw new ParsingException(source, "Unsupported join key ", source.text());
        }
    }

    @Override
    public Expression visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        Expression expr = expression(ctx.operatorExpression());
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

        return switch (type) {
            case EqlBaseParser.ASTERISK -> new Mul(source, left, right);
            case EqlBaseParser.SLASH -> new Div(source, left, right);
            case EqlBaseParser.PERCENT -> new Mod(source, left, right);
            case EqlBaseParser.PLUS -> new Add(source, left, right);
            case EqlBaseParser.MINUS -> new Sub(source, left, right);
            default -> throw new ParsingException(source, "Unknown arithmetic {}", source.text());
        };
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

        return switch (op.getSymbol().getType()) {
            case EqlBaseParser.EQ -> new Equals(source, left, right, zoneId);
            case EqlBaseParser.NEQ -> new Not(source, new Equals(source, left, right, zoneId));
            case EqlBaseParser.LT -> new LessThan(source, left, right, zoneId);
            case EqlBaseParser.LTE -> new LessThanOrEqual(source, left, right, zoneId);
            case EqlBaseParser.GT -> new GreaterThan(source, left, right, zoneId);
            case EqlBaseParser.GTE -> new GreaterThanOrEqual(source, left, right, zoneId);
            default -> throw new ParsingException(source, "Unknown operator {}", source.text());
        };
    }

    @Override
    public Expression visitOperatorExpressionDefault(EqlBaseParser.OperatorExpressionDefaultContext ctx) {
        Expression expr = expression(ctx.primaryExpression());
        Source source = source(ctx);
        ZoneId zoneId = params.zoneId();

        PredicateContext predicate = ctx.predicate();

        if (predicate == null) {
            return expr;
        }

        switch (predicate.kind.getType()) {
            case EqlBaseParser.SEQ:
                return combineExpressions(predicate.constant(), c -> new InsensitiveWildcardEquals(source, expr, c, zoneId));
            case EqlBaseParser.LIKE:
            case EqlBaseParser.LIKE_INSENSITIVE:
                return combineExpressions(
                    predicate.constant(),
                    e -> new Like(
                        source,
                        expr,
                        toLikePattern(e.fold().toString()),
                        predicate.kind.getType() == EqlBaseParser.LIKE_INSENSITIVE
                    )
                );
            case EqlBaseParser.REGEX:
            case EqlBaseParser.REGEX_INSENSITIVE:
                return new Match(
                    source,
                    expr,
                    expressions(predicate.constant()),
                    predicate.kind.getType() == EqlBaseParser.REGEX_INSENSITIVE
                );
            case EqlBaseParser.IN_INSENSITIVE:
                Expression insensitiveIn = combineExpressions(predicate.expression(), c -> new InsensitiveEquals(source, expr, c, zoneId));
                return predicate.NOT() != null ? new Not(source, insensitiveIn) : insensitiveIn;
            case EqlBaseParser.IN:
                List<Expression> container = expressions(predicate.expression());
                Expression checkInSet = new In(source, expr, container, zoneId);
                return predicate.NOT() != null ? new Not(source, checkInSet) : checkInSet;
            default:
                throw new ParsingException(source, "Unknown predicate {}", source.text());
        }
    }

    private Expression combineExpressions(
        List<? extends ParserRuleContext> expressions,
        java.util.function.Function<Expression, Expression> mapper
    ) {
        return Predicates.combineOr(expressions(expressions).stream().map(mapper).collect(toList()));
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
        Source source = source(ctx);
        EqlBaseParser.QualifiedNameContext qContext = ctx.qualifiedName();
        String name = visitQualifiedName(qContext);
        return qContext.OPTIONAL() != null ? new OptionalUnresolvedAttribute(source, name) : new UnresolvedAttribute(source, name);
    }

    @Override
    public Function visitFunctionExpression(FunctionExpressionContext ctx) {
        Source source = source(ctx);
        String name = ctx.name.getText();
        List<Expression> arguments = expressions(ctx.expression());

        FunctionResolutionStrategy strategy = FunctionResolutionStrategy.DEFAULT;

        if (name.endsWith("~")) {
            name = name.substring(0, name.length() - 1);
            strategy = EqlFunctionResolution.CASE_INSENSITIVE;
        }

        return new UnresolvedFunction(source, name, strategy, arguments);
    }

    @Override
    public Literal visitIntegerLiteral(EqlBaseParser.IntegerLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        try {
            Number value = StringUtils.parseIntegral(text);
            return new Literal(source, value, DataTypes.fromJava(value));
        } catch (QlIllegalArgumentException siae) {
            // if it's too large, then quietly try to parse as a float instead
            try {
                return new Literal(source, Double.valueOf(StringUtils.parseDouble(text)), DataTypes.DOUBLE);
            } catch (QlIllegalArgumentException ignored) {}

            throw new ParsingException(source, siae.getMessage());
        }
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
        Source source = source(ctx);
        return new Literal(source, unquoteString(source), DataTypes.KEYWORD);
    }
}
