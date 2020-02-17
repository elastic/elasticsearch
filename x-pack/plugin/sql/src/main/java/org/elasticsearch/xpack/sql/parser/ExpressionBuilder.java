/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction.ResolutionType;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.expression.Exists;
import org.elasticsearch.xpack.sql.expression.ScalarSubquery;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.literal.interval.Interval;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalYearMonth;
import org.elasticsearch.xpack.sql.expression.literal.interval.Intervals;
import org.elasticsearch.xpack.sql.expression.literal.interval.Intervals.TimeUnit;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Case;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfConditional;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ArithmeticBinaryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ArithmeticUnaryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.BooleanLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.BuiltinDateTimeFunctionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.CastExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.CastTemplateContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ComparisonContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ConstantDefaultContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ConvertTemplateContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.DateEscapedLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.DecimalLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.DereferenceContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ExistsContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ExtractExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ExtractTemplateContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.FunctionExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.FunctionTemplateContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.GuidEscapedLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.IntervalContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.IntervalFieldContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.IntervalLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LikePatternContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LogicalBinaryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LogicalNotContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.MatchQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.MatchQueryOptionsContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.MultiMatchQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.NullLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.NumberContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.OrderByContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ParamLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ParenthesizedExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PatternContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PatternEscapeContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PredicateContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PredicatedContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PrimitiveDataTypeContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SelectExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SingleExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StarContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StringContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StringLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StringQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SubqueryExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SysTypesContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.TimeEscapedLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.TimestampEscapedLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ValueExpressionDefaultContext;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.time.Duration;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.converterFor;
import static org.elasticsearch.xpack.sql.util.DateUtils.asDateOnly;
import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeOnly;
import static org.elasticsearch.xpack.sql.util.DateUtils.ofEscapedLiteral;

abstract class ExpressionBuilder extends IdentifierBuilder {

    private final Map<Token, SqlTypedParamValue> params;

    ExpressionBuilder(Map<Token, SqlTypedParamValue> params) {
        this.params = params;
    }

    protected Expression expression(ParseTree ctx) {
        return typedParsing(ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(contexts, Expression.class);
    }

    @Override
    public Expression visitSingleExpression(SingleExpressionContext ctx) {
        return expression(ctx.expression());
    }

    @Override
    public Expression visitSelectExpression(SelectExpressionContext ctx) {
        Expression exp = expression(ctx.expression());
        String alias = visitIdentifier(ctx.identifier());
        Source source = source(ctx);
        return alias != null ? new Alias(source, alias, exp) : new UnresolvedAlias(source, exp);
    }

    @Override
    public Expression visitStar(StarContext ctx) {
        return new UnresolvedStar(source(ctx), ctx.qualifiedName() != null ?
                new UnresolvedAttribute(source(ctx.qualifiedName()), visitQualifiedName(ctx.qualifiedName())) : null);
    }

    @Override
    public Object visitDereference(DereferenceContext ctx) {
        return new UnresolvedAttribute(source(ctx), visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Expression visitExists(ExistsContext ctx) {
        return new Exists(source(ctx), plan(ctx.query()));
    }

    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        TerminalNode op = (TerminalNode) ctx.comparisonOperator().getChild(0);

        Source source = source(ctx);

        switch (op.getSymbol().getType()) {
            case SqlBaseParser.EQ:
                return new Equals(source, left, right);
            case SqlBaseParser.NULLEQ:
                return new NullEquals(source, left, right);
            case SqlBaseParser.NEQ:
                return new NotEquals(source, left, right);
            case SqlBaseParser.LT:
                return new LessThan(source, left, right);
            case SqlBaseParser.LTE:
                return new LessThanOrEqual(source, left, right);
            case SqlBaseParser.GT:
                return new GreaterThan(source, left, right);
            case SqlBaseParser.GTE:
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
            case SqlBaseParser.BETWEEN:
                e = new Range(source, exp, expression(pCtx.lower), true, expression(pCtx.upper), true);
                break;
            case SqlBaseParser.IN:
                if (pCtx.query() != null) {
                    throw new ParsingException(source, "IN query not supported yet");
                }
                e = new In(source, exp, expressions(pCtx.valueExpression()));
                break;
            case SqlBaseParser.LIKE:
                e = new Like(source, exp, visitPattern(pCtx.pattern()));
                break;
            case SqlBaseParser.RLIKE:
                e = new RLike(source, exp, string(pCtx.regex));
                break;
            case SqlBaseParser.NULL:
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

    @Override
    public LikePattern visitLikePattern(LikePatternContext ctx) {
        return ctx == null ? null : visitPattern(ctx.pattern());
    }

    @Override
    public LikePattern visitPattern(PatternContext ctx) {
        if (ctx == null) {
            return null;
        }

        String pattern = string(ctx.value);
        int pos = pattern.indexOf('*');
        if (pos >= 0) {
            throw new ParsingException(source(ctx.value),
                    "Invalid char [*] found in pattern [{}] at position {}; use [%] or [_] instead",
                    pattern, pos);
        }

        char escape = 0;
        PatternEscapeContext escapeCtx = ctx.patternEscape();
        String escapeString = escapeCtx == null ? null : string(escapeCtx.escape);

        if (Strings.hasText(escapeString)) {
            // shouldn't happen but adding validation in case the string parsing gets wonky
            if (escapeString.length() > 1) {
                throw new ParsingException(source(escapeCtx), "A character not a string required for escaping; found [{}]", escapeString);
            } else if (escapeString.length() == 1) {
                escape = escapeString.charAt(0);
                // these chars already have a meaning
                if (escape == '*' || escape == '%' || escape == '_') {
                    throw new ParsingException(source(escapeCtx.escape), "Char [{}] cannot be used for escaping", escape);
                }
                // lastly validate that escape chars (if present) are followed by special chars
                for (int i = 0; i < pattern.length(); i++) {
                    char current = pattern.charAt(i);
                    if (current == escape) {
                        if (i + 1 == pattern.length()) {
                            throw new ParsingException(source(ctx.value),
                                    "Pattern [{}] is invalid as escape char [{}] at position {} does not escape anything", pattern, escape,
                                    i);
                        }
                        char next = pattern.charAt(i + 1);
                        if (next != '%' && next != '_') {
                            throw new ParsingException(source(ctx.value),
                                    "Pattern [{}] is invalid as escape char [{}] at position {} can only escape wildcard chars; found [{}]",
                                    pattern, escape, i, next);
                        }
                    }
                }
            }
        }

        return new LikePattern(pattern, escape);
    }


    //
    // Arithmetic
    //
    @Override
    public Object visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        Expression value = expression(ctx.valueExpression());
        Source source = source(ctx);

        switch (ctx.operator.getType()) {
            case SqlBaseParser.PLUS:
                return value;
            case SqlBaseParser.MINUS:
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
            case SqlBaseParser.ASTERISK:
                return new Mul(source, left, right);
            case SqlBaseParser.SLASH:
                return new Div(source, left, right);
            case SqlBaseParser.PERCENT:
                return new Mod(source, left, right);
            case SqlBaseParser.PLUS:
                return new Add(source, left, right);
            case SqlBaseParser.MINUS:
                return new Sub(source, left, right);
            default:
                throw new ParsingException(source, "Unknown arithmetic {}", source.text());
        }
    }

    //
    // Full-text search predicates
    //
    @Override
    public Object visitStringQuery(StringQueryContext ctx) {
        return new StringQueryPredicate(source(ctx), string(ctx.queryString), getQueryOptions(ctx.matchQueryOptions()));
    }

    @Override
    public Object visitMatchQuery(MatchQueryContext ctx) {
        return new MatchQueryPredicate(source(ctx), new UnresolvedAttribute(source(ctx.singleField),
                visitQualifiedName(ctx.singleField)), string(ctx.queryString), getQueryOptions(ctx.matchQueryOptions()));
    }

    @Override
    public Object visitMultiMatchQuery(MultiMatchQueryContext ctx) {
        return new MultiMatchQueryPredicate(source(ctx), string(ctx.multiFields), string(ctx.queryString),
            getQueryOptions(ctx.matchQueryOptions()));
    }

    private String getQueryOptions(MatchQueryOptionsContext optionsCtx) {
        StringJoiner sj = new StringJoiner(";");
        for (StringContext sc: optionsCtx.string()) {
            sj.add(string(sc));
        }
        return sj.toString();
    }

    @Override
    public Order visitOrderBy(OrderByContext ctx) {
        return new Order(source(ctx), expression(ctx.expression()),
                ctx.DESC() != null ? Order.OrderDirection.DESC : Order.OrderDirection.ASC,
                ctx.NULLS() != null ? (ctx.FIRST() != null ? NullsPosition.FIRST : NullsPosition.LAST) : null);
    }

    @Override
    public DataType visitPrimitiveDataType(PrimitiveDataTypeContext ctx) {
        return dataType(source(ctx), visitIdentifier(ctx.identifier()));
    }

    //
    // Functions template
    //
    @Override
    public Cast visitCastExpression(CastExpressionContext ctx) {
        CastTemplateContext castTc = ctx.castTemplate();
        if (castTc != null) {
            return new Cast(source(castTc), expression(castTc.expression()), typedParsing(castTc.dataType(), DataType.class));
        } else {
            ConvertTemplateContext convertTc = ctx.convertTemplate();
            DataType dataType = dataType(source(convertTc.dataType()), convertTc.dataType().getText());
            return new Cast(source(convertTc), expression(convertTc.expression()), dataType);
        }
    }

    private static DataType dataType(Source ctx, String string) {
        String type = string.toUpperCase(Locale.ROOT);
        DataType dataType = type.startsWith("SQL_") ? SqlDataTypes.fromOdbcType(type) : SqlDataTypes.fromSqlOrEsType(type);
        if (dataType == null) {
            throw new ParsingException(ctx, "Does not recognize type [{}]", string);
        }
        return dataType;
    }

    @Override
    public Object visitCastOperatorExpression(SqlBaseParser.CastOperatorExpressionContext ctx) {
        return new Cast(source(ctx), expression(ctx.primaryExpression()), typedParsing(ctx.dataType(), DataType.class));
    }

    @Override
    public Function visitExtractExpression(ExtractExpressionContext ctx) {
        ExtractTemplateContext template = ctx.extractTemplate();
        String fieldString = visitIdentifier(template.field);
        return new UnresolvedFunction(source(template), fieldString,
                UnresolvedFunction.ResolutionType.EXTRACT, singletonList(expression(template.valueExpression())));
    }

    @Override
    public Object visitBuiltinDateTimeFunction(BuiltinDateTimeFunctionContext ctx) {
        // maps CURRENT_XXX to its respective function e.g: CURRENT_TIMESTAMP()
        // since the functions need access to the Configuration, the parser only registers the definition and not the actual function
        Source source = source(ctx);
        String functionName = ctx.name.getText();

        switch (ctx.name.getType()) {
            case SqlBaseLexer.CURRENT_TIMESTAMP:
                return new UnresolvedFunction(source, functionName, ResolutionType.STANDARD, emptyList());
            case SqlBaseLexer.CURRENT_DATE:
                return new UnresolvedFunction(source, functionName, ResolutionType.STANDARD, emptyList());
            case SqlBaseLexer.CURRENT_TIME:
                return new UnresolvedFunction(source, functionName, ResolutionType.STANDARD, emptyList());
            default:
                throw new ParsingException(source, "Unknown function [{}]", functionName);
        }
    }

    @Override
    public Function visitFunctionExpression(FunctionExpressionContext ctx) {
        FunctionTemplateContext template = ctx.functionTemplate();
        String name = template.functionName().getText();
        boolean isDistinct = template.setQuantifier() != null && template.setQuantifier().DISTINCT() != null;
        UnresolvedFunction.ResolutionType resolutionType =
                isDistinct ? UnresolvedFunction.ResolutionType.DISTINCT : UnresolvedFunction.ResolutionType.STANDARD;
        return new UnresolvedFunction(source(ctx), name, resolutionType, expressions(template.expression()));
    }

    @Override
    public Expression visitSubqueryExpression(SubqueryExpressionContext ctx) {
        return new ScalarSubquery(source(ctx), plan(ctx.query()));
    }

    @Override
    public Object visitCase(SqlBaseParser.CaseContext ctx) {
        List<Expression> expressions = new ArrayList<>(ctx.whenClause().size());
        for (SqlBaseParser.WhenClauseContext when : ctx.whenClause()) {
            if (ctx.operand != null) {
                expressions.add(new IfConditional(source(when),
                    new Equals(source(when), expression(ctx.operand), expression(when.condition)), expression(when.result)));
            } else {
                expressions.add(new IfConditional(source(when), expression(when.condition), expression(when.result)));
            }
        }
        if (ctx.elseClause != null) {
            expressions.add(expression(ctx.elseClause));
        } else {
            expressions.add(Literal.NULL);
        }
        return new Case(source(ctx), expressions);
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

        if (type == SqlBaseParser.AND) {
            return new And(source, left, right);
        }
        if (type == SqlBaseParser.OR) {
            return new Or(source, left, right);
        }
        throw new ParsingException(source, "Don't know how to parse {}", ctx);
    }


    //
    // Literal
    //


    @Override
    public Expression visitNullLiteral(NullLiteralContext ctx) {
        return new Literal(source(ctx), null, DataTypes.NULL);
    }

    @Override
    public Literal visitInterval(IntervalContext interval) {

        TimeUnit leading = visitIntervalField(interval.leading);
        TimeUnit trailing = visitIntervalField(interval.trailing);

        // only YEAR TO MONTH or DAY TO HOUR/MINUTE/SECOND are valid declaration
        if (trailing != null) {
            if (leading == TimeUnit.YEAR && trailing != TimeUnit.MONTH) {
                throw new ParsingException(source(interval.trailing),
                        "Invalid interval declaration; YEAR trailing unit required to be MONTH, received {}", trailing);
            } else {
                if (trailing.ordinal() <= leading.ordinal()) {
                    EnumSet<TimeUnit> range = EnumSet.range(leading, TimeUnit.SECOND);
                    range.remove(leading);
                    throw new ParsingException(source(interval.trailing),
                            "Invalid interval declaration; trailing unit [{}] needs to be smaller than leading unit[{}], "
                            + "expected one of {}", trailing, leading, range);
                }
            }
        }

        DataType intervalType = Intervals.intervalType(source(interval), leading, trailing);

        // negation outside the interval - use xor
        boolean negative = false;

        ParserRuleContext parentCtx = interval.getParent();
        if (parentCtx != null) {
            if (parentCtx instanceof IntervalLiteralContext) {
                parentCtx = parentCtx.getParent();
                if (parentCtx instanceof ConstantDefaultContext) {
                    parentCtx = parentCtx.getParent();
                    if (parentCtx instanceof ValueExpressionDefaultContext) {
                        parentCtx = parentCtx.getParent();
                        if (parentCtx instanceof ArithmeticUnaryContext) {
                            ArithmeticUnaryContext auc = (ArithmeticUnaryContext) parentCtx;
                            negative = auc.MINUS() != null;
                        }
                    }
                }
            }
        }


        // negation inside the interval
        negative ^= interval.sign != null && interval.sign.getType() == SqlBaseParser.MINUS;

        TemporalAmount value = null;

        if (interval.valueNumeric != null) {
            if (trailing != null) {
                throw new ParsingException(source(interval.trailing),
                        "Invalid interval declaration; trailing unit [{}] specified but the value is with numeric (single unit), "
                        + "use the string notation instead", trailing);
            }
            value = of(interval.valueNumeric, leading);
        } else {
            value = of(interval.valuePattern, negative, intervalType);
        }

        Interval<?> timeInterval = value instanceof Period ? new IntervalYearMonth((Period) value,
                intervalType) : new IntervalDayTime((Duration) value, intervalType);

        return new Literal(source(interval), timeInterval, timeInterval.dataType());
    }

    private TemporalAmount of(NumberContext valueNumeric, TimeUnit unit) {
        // expect numbers for now
        // as the number parsing handles the -, there's no need to look at that
        Literal value = (Literal) visit(valueNumeric);
        Number numeric = (Number) value.fold();

        if (Math.rint(numeric.doubleValue()) != numeric.longValue()) {
            throw new ParsingException(source(valueNumeric), "Fractional values are not supported for intervals");
        }

        return Intervals.of(source(valueNumeric), numeric.longValue(), unit);
    }

    private TemporalAmount of(StringContext valuePattern, boolean negative, DataType intervalType) {
        String valueString = string(valuePattern);
        Source source = source(valuePattern);
        TemporalAmount interval = Intervals.parseInterval(source, valueString, intervalType);
        if (negative) {
            interval = Intervals.negate(interval);
        }
        return interval;
    }

    @Override
    public Intervals.TimeUnit visitIntervalField(IntervalFieldContext ctx) {
        if (ctx == null) {
            return null;
        }

        switch (ctx.getChild(TerminalNode.class, 0).getSymbol().getType()) {
            case SqlBaseParser.YEAR:
            case SqlBaseParser.YEARS:
                return Intervals.TimeUnit.YEAR;
            case SqlBaseParser.MONTH:
            case SqlBaseParser.MONTHS:
                return Intervals.TimeUnit.MONTH;
            case SqlBaseParser.DAY:
            case SqlBaseParser.DAYS:
                return Intervals.TimeUnit.DAY;
            case SqlBaseParser.HOUR:
            case SqlBaseParser.HOURS:
                return Intervals.TimeUnit.HOUR;
            case SqlBaseParser.MINUTE:
            case SqlBaseParser.MINUTES:
                return Intervals.TimeUnit.MINUTE;
            case SqlBaseParser.SECOND:
            case SqlBaseParser.SECONDS:
                return Intervals.TimeUnit.SECOND;
        }

        throw new IllegalArgumentException("Unsupported interval field: " + ctx.getText());
    }

    @Override
    public Expression visitBooleanLiteral(BooleanLiteralContext ctx) {
        boolean value;
        try {
            value = Booleans.parseBoolean(ctx.getText().toLowerCase(Locale.ROOT), false);
        } catch(IllegalArgumentException iae) {
            throw new ParsingException(source(ctx), iae.getMessage());
    }
        return new Literal(source(ctx), Boolean.valueOf(value), DataTypes.BOOLEAN);
    }

    @Override
    public Expression visitStringLiteral(StringLiteralContext ctx) {
        StringBuilder sb = new StringBuilder();
        for (TerminalNode node : ctx.STRING()) {
            sb.append(unquoteString(text(node)));
        }
        return new Literal(source(ctx), sb.toString(), DataTypes.KEYWORD);
    }

    @Override
    public Literal visitDecimalLiteral(DecimalLiteralContext ctx) {
        Tuple<Source, String> tuple = withMinus(ctx);

        try {
            return new Literal(tuple.v1(), Double.valueOf(StringUtils.parseDouble(tuple.v2())), DataTypes.DOUBLE);
        } catch (QlIllegalArgumentException siae) {
            throw new ParsingException(tuple.v1(), siae.getMessage());
        }
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        Tuple<Source, String> tuple = withMinus(ctx);

        long value;
        try {
            value = Long.valueOf(StringUtils.parseLong(tuple.v2()));
        } catch (QlIllegalArgumentException siae) {
            throw new ParsingException(tuple.v1(), siae.getMessage());
        }

        Object val = Long.valueOf(value);
        DataType type = DataTypes.LONG;
        // try to downsize to int if possible (since that's the most common type)
        if ((int) value == value) {
            type = DataTypes.INTEGER;
            val = Integer.valueOf((int) value);
        }
        return new Literal(tuple.v1(), val, type);
    }

    @Override
    public Literal visitParamLiteral(ParamLiteralContext ctx) {
        SqlTypedParamValue param = param(ctx.PARAM());
        DataType dataType = SqlDataTypes.fromTypeName(param.type);
        Source source = source(ctx);
        if (param.value == null) {
            // no conversion is required for null values
            return new Literal(source, null, dataType);
        }
        final DataType sourceType;
        try {
            sourceType = DataTypes.fromJava(param.value);
        } catch (QlIllegalArgumentException ex) {
            throw new ParsingException(ex, source, "Unexpected actual parameter type [{}] for type [{}]", param.value.getClass().getName(),
                    param.type);
        }
        if (sourceType == dataType) {
            // no conversion is required if the value is already have correct type
            return new Literal(source, param.value, dataType);
        }
        // otherwise we need to make sure that xcontent-serialized value is converted to the correct type
        try {
            return new Literal(source, converterFor(sourceType, dataType).convert(param.value), dataType);
        } catch (QlIllegalArgumentException ex) {
            throw new ParsingException(ex, source, "Unexpected actual parameter type [{}] for type [{}]", sourceType, param.type);
        }
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
        SqlTypedParamValue param = param(ctx.PARAM());
        if (param != null) {
            return param.value != null ? param.value.toString() : null;
        } else {
            return unquoteString(ctx.getText());
        }
    }

    private SqlTypedParamValue param(TerminalNode node) {
        if (node == null) {
            return null;
        }

        Token token = node.getSymbol();

        if (params.containsKey(token) == false) {
            throw new ParsingException(source(node), "Unexpected parameter");
        }

        return params.get(token);
    }

    @Override
    public Literal visitDateEscapedLiteral(DateEscapedLiteralContext ctx) {
        String string = string(ctx.string());
        Source source = source(ctx);
        // parse yyyy-MM-dd
        try {
            return new Literal(source, asDateOnly(string), SqlDataTypes.DATE);
        } catch(DateTimeParseException ex) {
            throw new ParsingException(source, "Invalid date received; {}", ex.getMessage());
        }
    }

    @Override
    public Literal visitTimeEscapedLiteral(TimeEscapedLiteralContext ctx) {
        String string = string(ctx.string());
        Source source = source(ctx);

        // parse HH:mm:ss
        try {
            return new Literal(source, asTimeOnly(string), SqlDataTypes.TIME);
        } catch (DateTimeParseException ex) {
            throw new ParsingException(source, "Invalid time received; {}", ex.getMessage());
        }
    }

    @Override
    public Literal visitTimestampEscapedLiteral(TimestampEscapedLiteralContext ctx) {
        String string = string(ctx.string());

        Source source = source(ctx);
        // parse yyyy-mm-dd hh:mm:ss(.f...)
        try {
            return new Literal(source, ofEscapedLiteral(string), DataTypes.DATETIME);
        } catch (DateTimeParseException ex) {
            throw new ParsingException(source, "Invalid timestamp received; {}", ex.getMessage());
        }
    }

    @Override
    public Literal visitGuidEscapedLiteral(GuidEscapedLiteralContext ctx) {
        String string = string(ctx.string());

        Source source = source(ctx.string());
        // basic validation
        String lowerCase = string.toLowerCase(Locale.ROOT);
        // needs to be format nnnnnnnn-nnnn-nnnn-nnnn-nnnnnnnnnnnn
        // since the length is fixed, the validation happens on absolute values
        // not pretty but it's fast and doesn't create any extra objects

        String errorPrefix = "Invalid GUID, ";

        if (lowerCase.length() != 36) {
            throw new ParsingException(source, "{}too {}", errorPrefix, lowerCase.length() > 36 ? "long" : "short");
        }

        int[] separatorPos = { 8, 13, 18, 23 };
        for (int pos : separatorPos) {
            if (lowerCase.charAt(pos) != '-') {
                throw new ParsingException(source, "{}expected group separator at offset [{}], found [{}]",
                        errorPrefix, pos, string.charAt(pos));
            }
        }

        String HEXA = "0123456789abcdef";

        for (int i = 0; i < lowerCase.length(); i++) {
            // skip separators
            boolean inspect = true;
            for (int pos : separatorPos) {
                if (i == pos) {
                    inspect = false;
                    break;
                } else if (pos > i) {
                    break;
                }
            }
            if (inspect && HEXA.indexOf(lowerCase.charAt(i)) < 0) {
                throw new ParsingException(source, "{}expected hexadecimal at offset[{}], found [{}]", errorPrefix, i, string.charAt(i));
            }
        }

        return new Literal(source(ctx), string, DataTypes.KEYWORD);
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
    private static Source minusAwareSource(SqlBaseParser.NumberContext ctx) {
        ParserRuleContext parentCtx = ctx.getParent();
        if (parentCtx != null) {
            if (parentCtx instanceof SqlBaseParser.NumericLiteralContext) {
                parentCtx = parentCtx.getParent();
                if (parentCtx instanceof ConstantDefaultContext) {
                    parentCtx = parentCtx.getParent();
                    if (parentCtx instanceof ValueExpressionDefaultContext) {
                        parentCtx = parentCtx.getParent();

                        // Skip parentheses, e.g.: - (( (2.15) ) )
                        while (parentCtx instanceof PredicatedContext) {
                            parentCtx = parentCtx.getParent();
                            if (parentCtx instanceof SqlBaseParser.BooleanDefaultContext) {
                                parentCtx = parentCtx.getParent();
                            }
                            if (parentCtx instanceof SqlBaseParser.ExpressionContext) {
                                parentCtx = parentCtx.getParent();
                            }
                            if (parentCtx instanceof SqlBaseParser.ParenthesizedExpressionContext) {
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
            } else if (parentCtx instanceof SqlBaseParser.IntervalContext) {
                IntervalContext ic = (IntervalContext) parentCtx;
                if (ic.sign != null && ic.sign.getType() == SqlBaseParser.MINUS) {
                    return source(ic);
                }
            } else if (parentCtx instanceof SqlBaseParser.SysTypesContext) {
                if (((SysTypesContext) parentCtx).MINUS() != null) {
                    return source(parentCtx);
                }
            }
        }
        return null;
    }
}
