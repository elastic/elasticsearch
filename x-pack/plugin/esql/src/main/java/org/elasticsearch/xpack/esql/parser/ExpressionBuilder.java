/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.RLike;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DateUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.math.BigInteger;
import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.parseTemporalAmout;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.TIME_DURATION;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.ql.util.StringUtils.WILDCARD;

public abstract class ExpressionBuilder extends IdentifierBuilder {

    private final Map<Token, TypedParamValue> params;

    ExpressionBuilder(Map<Token, TypedParamValue> params) {
        this.params = params;
    }

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
        Number number;

        try {
            number = StringUtils.parseIntegral(text);
        } catch (InvalidArgumentException siae) {
            // if it's too large, then quietly try to parse as a float instead
            try {
                return new Literal(source, StringUtils.parseDouble(text), DataTypes.DOUBLE);
            } catch (QlIllegalArgumentException ignored) {}

            throw new ParsingException(source, siae.getMessage());
        }

        Object val;
        DataType type;
        if (number instanceof BigInteger bi) {
            val = asLongUnsigned(bi);
            type = DataTypes.UNSIGNED_LONG;
        } else if (number.intValue() == number.longValue()) { // try to downsize to int if possible (since that's the most common type)
            val = number.intValue();
            type = DataTypes.INTEGER;
        } else {
            val = number.longValue();
            type = DataTypes.LONG;
        }
        return new Literal(source, val, type);
    }

    @Override
    public Object visitNumericArrayLiteral(EsqlBaseParser.NumericArrayLiteralContext ctx) {
        Source source = source(ctx);
        List<Literal> numbers = visitList(this, ctx.numericValue(), Literal.class);
        if (numbers.stream().anyMatch(l -> l.dataType() == DataTypes.DOUBLE)) {
            return new Literal(source, mapNumbers(numbers, (no, dt) -> no.doubleValue()), DataTypes.DOUBLE);
        }
        if (numbers.stream().anyMatch(l -> l.dataType() == DataTypes.UNSIGNED_LONG)) {
            return new Literal(
                source,
                mapNumbers(
                    numbers,
                    (no, dt) -> dt == DataTypes.UNSIGNED_LONG ? no.longValue() : asLongUnsigned(BigInteger.valueOf(no.longValue()))
                ),
                DataTypes.UNSIGNED_LONG
            );
        }
        if (numbers.stream().anyMatch(l -> l.dataType() == DataTypes.LONG)) {
            return new Literal(source, mapNumbers(numbers, (no, dt) -> no.longValue()), DataTypes.LONG);
        }
        return new Literal(source, mapNumbers(numbers, (no, dt) -> no.intValue()), DataTypes.INTEGER);
    }

    private List<Object> mapNumbers(List<Literal> numbers, BiFunction<Number, DataType, Object> map) {
        return numbers.stream().map(l -> map.apply((Number) l.value(), l.dataType())).toList();
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
        Number value = (Number) intLit.value();
        if (intLit.dataType() == DataTypes.UNSIGNED_LONG) {
            value = unsignedLongAsNumber(value.longValue());
        }
        String qualifier = ctx.UNQUOTED_IDENTIFIER().getText().toLowerCase(Locale.ROOT);

        try {
            TemporalAmount quantity = parseTemporalAmout(value, qualifier, source);
            return new Literal(source, quantity, quantity instanceof Duration ? TIME_DURATION : DATE_PERIOD);
        } catch (InvalidArgumentException | ArithmeticException e) {
            // the range varies by unit: Duration#ofMinutes(), #ofHours() will Math#multiplyExact() to reduce the unit to seconds;
            // and same for Period#ofWeeks()
            throw new ParsingException(source, "Number [{}] outside of [{}] range", ctx.integerValue().getText(), qualifier);
        }
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
        String name = visitIdentifier(ctx.identifier());
        List<Expression> args = expressions(ctx.booleanExpression());
        if ("count".equals(EsqlFunctionRegistry.normalizeName(name))) {
            // to simplify the registration, handle in the parser the special count cases
            if (args.isEmpty() || ctx.ASTERISK() != null) {
                args = singletonList(new Literal(source(ctx), "*", DataTypes.KEYWORD));
            }
        }
        return new UnresolvedFunction(source(ctx), name, FunctionResolutionStrategy.DEFAULT, args);
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
        Expression e = expressions.size() == 2
            ? new Equals(source, expressions.get(0), expressions.get(1))
            : new In(source, expressions.get(0), expressions.subList(1, expressions.size()));
        return ctx.NOT() == null ? e : new Not(source, e);
    }

    @Override
    public Object visitIsNull(EsqlBaseParser.IsNullContext ctx) {
        Expression exp = expression(ctx.valueExpression());
        Source source = source(ctx.valueExpression(), ctx);
        return ctx.NOT() != null ? new IsNotNull(source, exp) : new IsNull(source, exp);
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

    @Override
    public Object visitInputParam(EsqlBaseParser.InputParamContext ctx) {
        TypedParamValue param = param(ctx.PARAM());
        DataType dataType = EsqlDataTypes.fromTypeName(param.type);
        Source source = source(ctx);
        if (dataType == null) {
            throw new ParsingException(source, "Invalid parameter data type [{}]", param.type);
        }
        if (param.value == null) {
            // no conversion is required for null values
            return new Literal(source, null, dataType);
        }
        final DataType sourceType;
        try {
            sourceType = DataTypes.fromJava(param.value);
        } catch (QlIllegalArgumentException ex) {
            throw new ParsingException(
                ex,
                source,
                "Unexpected actual parameter type [{}] for type [{}]",
                param.value.getClass().getName(),
                param.type
            );
        }
        if (sourceType == dataType) {
            // no conversion is required if the value is already have correct type
            return new Literal(source, param.value, dataType);
        }
        // otherwise we need to make sure that xcontent-serialized value is converted to the correct type
        try {

            if (EsqlDataTypeConverter.canConvert(sourceType, dataType) == false) {
                throw new ParsingException(
                    source,
                    "Cannot cast value [{}] of type [{}] to parameter type [{}]",
                    param.value,
                    sourceType,
                    dataType
                );
            }
            return new Literal(source, EsqlDataTypeConverter.converterFor(sourceType, dataType).convert(param.value), dataType);
        } catch (QlIllegalArgumentException ex) {
            throw new ParsingException(ex, source, "Unexpected actual parameter type [{}] for type [{}]", sourceType, param.type);
        }
    }

    private TypedParamValue param(TerminalNode node) {
        if (node == null) {
            return null;
        }

        Token token = node.getSymbol();

        if (params.containsKey(token) == false) {
            throw new ParsingException(source(node), "Unexpected parameter");
        }

        return params.get(token);
    }

}
