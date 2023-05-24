/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy;
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
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardLike;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DateUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.TIME_DURATION;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.ql.util.StringUtils.WILDCARD;

abstract class ExpressionBuilder extends IdentifierBuilder {
    protected Expression expression(ParseTree ctx) {
        return typedParsing(this, ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(this, contexts, Expression.class);
    }

    @Override
    public Literal visitBooleanValue(EsqlBaseParser.BooleanValueContext ctx) {
        Source source = source(ctx);
        return new Literal(source, ctx.TRUE() != null, DataTypes.BOOLEAN);
    }

    @Override
    public Literal visitDecimalValue(EsqlBaseParser.DecimalValueContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        try {
            return new Literal(source, Double.valueOf(StringUtils.parseDouble(text)), DataTypes.DOUBLE);
        } catch (QlIllegalArgumentException siae) {
            throw new ParsingException(source, siae.getMessage());
        }
    }

    @Override
    public Literal visitIntegerValue(EsqlBaseParser.IntegerValueContext ctx) {
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
    public Object visitNumericArrayLiteral(EsqlBaseParser.NumericArrayLiteralContext ctx) {
        Source source = source(ctx);
        List<Literal> numbers = visitList(this, ctx.numericValue(), Literal.class);
        if (numbers.stream().anyMatch(l -> l.dataType() == DataTypes.DOUBLE)) {
            return new Literal(source, mapNumbers(numbers, Number::doubleValue), DataTypes.DOUBLE);
        }
        if (numbers.stream().anyMatch(l -> l.dataType() == DataTypes.LONG)) {
            return new Literal(source, mapNumbers(numbers, Number::longValue), DataTypes.LONG);
        }
        return new Literal(source, mapNumbers(numbers, Number::intValue), DataTypes.INTEGER);
    }

    private List<Object> mapNumbers(List<Literal> numbers, Function<Number, Object> map) {
        return numbers.stream().map(l -> map.apply((Number) l.value())).toList();
    }

    @Override
    public Object visitBooleanArrayLiteral(EsqlBaseParser.BooleanArrayLiteralContext ctx) {
        return visitArrayLiteral(ctx, ctx.booleanValue(), DataTypes.BOOLEAN);
    }

    @Override
    public Object visitStringArrayLiteral(EsqlBaseParser.StringArrayLiteralContext ctx) {
        return visitArrayLiteral(ctx, ctx.string(), DataTypes.KEYWORD);
    }

    private Object visitArrayLiteral(ParserRuleContext ctx, List<? extends ParserRuleContext> contexts, DataType dataType) {
        Source source = source(ctx);
        List<Literal> literals = visitList(this, contexts, Literal.class);
        return new Literal(source, literals.stream().map(Literal::value).toList(), dataType);
    }

    @Override
    public Literal visitNullLiteral(EsqlBaseParser.NullLiteralContext ctx) {
        Source source = source(ctx);
        return new Literal(source, null, DataTypes.NULL);
    }

    @Override
    public Literal visitStringLiteral(EsqlBaseParser.StringLiteralContext ctx) {
        return visitString(ctx.string());
    }

    @Override
    public Literal visitString(EsqlBaseParser.StringContext ctx) {
        Source source = source(ctx);
        return new Literal(source, unquoteString(source), DataTypes.KEYWORD);
    }

    @Override
    public UnresolvedAttribute visitQualifiedName(EsqlBaseParser.QualifiedNameContext ctx) {
        if (ctx == null) {
            return null;
        }

        return new UnresolvedAttribute(
            source(ctx),
            Strings.collectionToDelimitedString(visitList(this, ctx.identifier(), String.class), ".")
        );
    }

    @Override
    public Object visitQualifiedIntegerLiteral(EsqlBaseParser.QualifiedIntegerLiteralContext ctx) {
        Source source = source(ctx);
        Literal intLit = typedParsing(this, ctx.integerValue(), Literal.class);
        Integer value = (Integer) intLit.value();
        String qualifier = ctx.UNQUOTED_IDENTIFIER().getText().toLowerCase(Locale.ROOT);

        return switch (qualifier) {
            case "millisecond", "milliseconds" -> new Literal(source, Duration.ofMillis(value), TIME_DURATION);
            case "second", "seconds" -> new Literal(source, Duration.ofSeconds(value), TIME_DURATION);
            case "minute", "minutes" -> new Literal(source, Duration.ofMinutes(value), TIME_DURATION);
            case "hour", "hours" -> new Literal(source, Duration.ofHours(value), TIME_DURATION);

            case "day", "days" -> new Literal(source, Period.ofDays(value), DATE_PERIOD);
            case "week", "weeks" -> new Literal(source, Period.ofDays(value * 7), DATE_PERIOD);
            case "month", "months" -> new Literal(source, Period.ofMonths(value), DATE_PERIOD);
            case "year", "years" -> new Literal(source, Period.ofYears(value), DATE_PERIOD);
            default -> throw new ParsingException(source, "Unexpected numeric qualifier '{}'", qualifier);
        };
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
        return visitQualifiedName(ctx.qualifiedName());
    }

    @Override
    public Expression visitFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx) {
        return new UnresolvedFunction(
            source(ctx),
            visitIdentifier(ctx.identifier()),
            FunctionResolutionStrategy.DEFAULT,
            ctx.booleanExpression().stream().map(this::expression).toList()
        );
    }

    @Override
    public Expression visitLogicalBinary(EsqlBaseParser.LogicalBinaryContext ctx) {
        int type = ctx.operator.getType();
        Source source = source(ctx);
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        return type == EsqlBaseParser.AND ? new And(source, left, right) : new Or(source, left, right);
    }

    @Override
    public Expression visitLogicalIn(EsqlBaseParser.LogicalInContext ctx) {
        List<Expression> expressions = ctx.valueExpression().stream().map(this::expression).toList();
        Source source = source(ctx);
        Expression in = new In(source, expressions.get(0), expressions.subList(1, expressions.size()));
        return ctx.NOT() == null ? in : new Not(source, in);
    }

    @Override
    public Expression visitRegexBooleanExpression(EsqlBaseParser.RegexBooleanExpressionContext ctx) {
        int type = ctx.kind.getType();
        Source source = source(ctx);
        Expression left = expression(ctx.valueExpression());
        Literal pattern = visitString(ctx.pattern);
        RegexMatch<?> result = switch (type) {
            case EsqlBaseParser.LIKE -> new WildcardLike(source, left, new WildcardPattern(pattern.fold().toString()));
            case EsqlBaseParser.RLIKE -> new RLike(source, left, new RLikePattern(pattern.fold().toString()));
            default -> throw new ParsingException("Invalid predicate type for [{}]", source.text());
        };
        return ctx.NOT() == null ? result : new Not(source, result);
    }

    @Override
    public Order visitOrderExpression(EsqlBaseParser.OrderExpressionContext ctx) {
        return new Order(
            source(ctx),
            expression(ctx.booleanExpression()),
            ctx.DESC() != null ? Order.OrderDirection.DESC : Order.OrderDirection.ASC,
            (ctx.NULLS() != null && ctx.LAST() != null || ctx.NULLS() == null && ctx.DESC() == null)
                ? Order.NullsPosition.LAST
                : Order.NullsPosition.FIRST
        );
    }

    public NamedExpression visitProjectExpression(EsqlBaseParser.SourceIdentifierContext ctx) {
        Source src = source(ctx);
        String identifier = visitSourceIdentifier(ctx);
        return identifier.equals(WILDCARD) ? new UnresolvedStar(src, null) : new UnresolvedAttribute(src, identifier);
    }

    @Override
    public Alias visitRenameClause(EsqlBaseParser.RenameClauseContext ctx) {
        Source src = source(ctx);
        String newName = visitSourceIdentifier(ctx.newName);
        String oldName = visitSourceIdentifier(ctx.oldName);
        if (newName.contains(WILDCARD) || oldName.contains(WILDCARD)) {
            throw new ParsingException(src, "Using wildcards (*) in renaming projections is not allowed [{}]", src.text());
        }

        return new Alias(src, newName, new UnresolvedAttribute(source(ctx.oldName), oldName));
    }

    @Override
    public Alias visitField(EsqlBaseParser.FieldContext ctx) {
        UnresolvedAttribute id = visitQualifiedName(ctx.qualifiedName());
        Expression value = expression(ctx.booleanExpression());
        String name = id == null ? ctx.getText() : id.qualifiedName();
        return new Alias(source(ctx), name, value);
    }

    @Override
    public List<NamedExpression> visitGrouping(EsqlBaseParser.GroupingContext ctx) {
        return ctx != null ? visitList(this, ctx.qualifiedName(), NamedExpression.class) : emptyList();
    }
}
