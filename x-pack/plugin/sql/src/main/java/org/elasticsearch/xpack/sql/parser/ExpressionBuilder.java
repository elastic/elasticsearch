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
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Exists;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.ScalarSubquery;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.UnresolvedStar;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.predicate.And;
import org.elasticsearch.xpack.sql.expression.predicate.In;
import org.elasticsearch.xpack.sql.expression.predicate.IsNotNull;
import org.elasticsearch.xpack.sql.expression.predicate.Not;
import org.elasticsearch.xpack.sql.expression.predicate.Or;
import org.elasticsearch.xpack.sql.expression.predicate.Range;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.sql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ArithmeticBinaryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ArithmeticUnaryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.BooleanLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.CastExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.CastTemplateContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ComparisonContext;
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
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LikePatternContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LogicalBinaryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LogicalNotContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.MatchQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.MatchQueryOptionsContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.MultiMatchQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.NullLiteralContext;
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
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.TimeEscapedLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.TimestampEscapedLiteralContext;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.type.DataTypeConversion.conversionFor;

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
        if (alias != null) {
            exp = new Alias(source(ctx), alias, exp);
        }
        return exp;
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

        Location loc = source(ctx);

        switch (op.getSymbol().getType()) {
            case SqlBaseParser.EQ:
                return new Equals(loc, left, right);
            case SqlBaseParser.NEQ:
                return new Not(loc, new Equals(loc, left, right));
            case SqlBaseParser.LT:
                return new LessThan(loc, left, right);
            case SqlBaseParser.LTE:
                return new LessThanOrEqual(loc, left, right);
            case SqlBaseParser.GT:
                return new GreaterThan(loc, left, right);
            case SqlBaseParser.GTE:
                return new GreaterThanOrEqual(loc, left, right);
            default:
                throw new ParsingException(loc, "Unknown operator {}", op.getSymbol().getText());
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
        Location loc = source(pCtx);

        Expression e = null;
        switch (pCtx.kind.getType()) {
            case SqlBaseParser.BETWEEN:
                e = new Range(loc, exp, expression(pCtx.lower), true, expression(pCtx.upper), true);
                break;
            case SqlBaseParser.IN:
                if (pCtx.query() != null) {
                    throw new ParsingException(loc, "IN query not supported yet");
                }
                e = new In(loc, exp, expressions(pCtx.expression()));
                break;
            case SqlBaseParser.LIKE:
                e = new Like(loc, exp, visitPattern(pCtx.pattern()));
                break;
            case SqlBaseParser.RLIKE:
                e = new RLike(loc, exp, new Literal(source(pCtx.regex), string(pCtx.regex), DataType.KEYWORD));
                break;
            case SqlBaseParser.NULL:
                // shortcut to avoid double negation later on (since there's no IsNull (missing in ES is a negated exists))
                e = new IsNotNull(loc, exp);
                return pCtx.NOT() != null ? e : new Not(loc, e);
            default:
                throw new ParsingException(loc, "Unknown predicate {}", pCtx.kind.getText());
        }

        return pCtx.NOT() != null ? new Not(loc, e) : e;
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

        return new LikePattern(source(ctx), pattern, escape);
    }


    //
    // Arithmetic
    //
    @Override
    public Object visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        Expression value = expression(ctx.valueExpression());
        Location loc = source(ctx);

        switch (ctx.operator.getType()) {
            case SqlBaseParser.PLUS:
                return value;
            case SqlBaseParser.MINUS:
                return new Neg(source(ctx.operator), value);
            default:
                throw new ParsingException(loc, "Unknown arithemtic {}", ctx.operator.getText());
        }
    }

    @Override
    public Object visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        Location loc = source(ctx.operator);

        switch (ctx.operator.getType()) {
            case SqlBaseParser.ASTERISK:
                return new Mul(loc, left, right);
            case SqlBaseParser.SLASH:
                return new Div(loc, left, right);
            case SqlBaseParser.PERCENT:
                return new Mod(loc, left, right);
            case SqlBaseParser.PLUS:
                return new Add(loc, left, right);
            case SqlBaseParser.MINUS:
                return new Sub(loc, left, right);
            default:
                throw new ParsingException(loc, "Unknown arithemtic {}", ctx.operator.getText());
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
            ctx.DESC() != null ? Order.OrderDirection.DESC : Order.OrderDirection.ASC);
    }

    @Override
    public DataType visitPrimitiveDataType(PrimitiveDataTypeContext ctx) {
        String type = visitIdentifier(ctx.identifier()).toLowerCase(Locale.ROOT);

        switch (type) {
            case "bit":
            case "bool":
            case "boolean":
                return DataType.BOOLEAN;
            case "tinyint":
            case "byte":
                return DataType.BYTE;
            case "smallint":
            case "short":
                return DataType.SHORT;
            case "int":
            case "integer":
                return DataType.INTEGER;
            case "long":
            case "bigint":
                return DataType.LONG;
            case "real":
                return DataType.FLOAT;
            case "float":
            case "double":
                return DataType.DOUBLE;
            case "date":
            case "timestamp":
                return DataType.DATE;
            case "char":
            case "varchar":
            case "string":
                return DataType.KEYWORD;
            default:
                throw new ParsingException(source(ctx), "Does not recognize type {}", type);
        }
    }

    //
    // Functions template
    //
    @Override
    public Cast visitCastExpression(CastExpressionContext ctx) {
        CastTemplateContext ctc = ctx.castTemplate();
        return new Cast(source(ctc), expression(ctc.expression()), typedParsing(ctc.dataType(), DataType.class));
    }

    @Override
    public Function visitExtractExpression(ExtractExpressionContext ctx) {
        ExtractTemplateContext template = ctx.extractTemplate();
        String fieldString = visitIdentifier(template.field);
        return new UnresolvedFunction(source(template), fieldString,
                UnresolvedFunction.ResolutionType.EXTRACT, singletonList(expression(template.valueExpression())));
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
        Location loc = source(ctx);
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        if (type == SqlBaseParser.AND) {
            return new And(loc, left, right);
        }
        if (type == SqlBaseParser.OR) {
            return new Or(loc, left, right);
        }
        throw new ParsingException(loc, "Don't know how to parse {}", ctx);
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
        } catch(IllegalArgumentException iae) {
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
        double value;
        try {
            value = Double.parseDouble(ctx.getText());
        } catch (NumberFormatException nfe) {
            throw new ParsingException(source(ctx), "Cannot parse number [{}]", ctx.getText());
        }
        if (Double.isInfinite(value)) {
            throw new ParsingException(source(ctx), "Number [{}] is too large", ctx.getText());
        }
        if (Double.isNaN(value)) {
            throw new ParsingException(source(ctx), "[{}] cannot be parsed as a number (NaN)", ctx.getText());
        }
        return new Literal(source(ctx), Double.valueOf(value), DataType.DOUBLE);
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        long value;
        try {
            value = Long.parseLong(ctx.getText());
        } catch (NumberFormatException nfe) {
            try {
                BigInteger bi = new BigInteger(ctx.getText());
                try {
                    bi.longValueExact();
                } catch (ArithmeticException ae) {
                    throw new ParsingException(source(ctx), "Number [{}] is too large", ctx.getText());
                }
            } catch (NumberFormatException ex) {
                // parsing fails, go through
            }
            throw new ParsingException(source(ctx), "Cannot parse number [{}]", ctx.getText());
        }

        DataType type = DataType.LONG;
        // try to downsize to int if possible (since that's the most common type)
        if ((int) value == value) {
            type = DataType.INTEGER;
        }
        return new Literal(source(ctx), value, type);
    }

    @Override
    public Literal visitParamLiteral(ParamLiteralContext ctx) {
        SqlTypedParamValue param = param(ctx.PARAM());
        Location loc = source(ctx);
        if (param.value == null) {
            // no conversion is required for null values
            return new Literal(loc, null, param.dataType);
        }
        final DataType sourceType;
        try {
            sourceType = DataTypes.fromJava(param.value);
        } catch (SqlIllegalArgumentException ex) {
            throw new ParsingException(ex, loc, "Unexpected actual parameter type [{}] for type [{}]", param.value.getClass().getName(),
                    param.dataType);
        }
        if (sourceType == param.dataType) {
            // no conversion is required if the value is already have correct type
            return new Literal(loc, param.value, param.dataType);
        }
        // otherwise we need to make sure that xcontent-serialized value is converted to the correct type
        try {
            return new Literal(loc, conversionFor(sourceType, param.dataType).convert(param.value), param.dataType);
        } catch (SqlIllegalArgumentException ex) {
            throw new ParsingException(ex, loc, "Unexpected actual parameter type [{}] for type [{}]", sourceType, param.dataType);
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
        Location loc = source(ctx);
        // parse yyyy-MM-dd
        DateTime dt = null;
        try {
            dt = ISODateTimeFormat.date().parseDateTime(string);
        } catch(IllegalArgumentException ex) {
            throw new ParsingException(loc, "Invalid date received; {}", ex.getMessage());
        }
        return new Literal(loc, dt, DataType.DATE);
    }

    @Override
    public Literal visitTimeEscapedLiteral(TimeEscapedLiteralContext ctx) {
        String string = string(ctx.string());
        Location loc = source(ctx);

        // parse HH:mm:ss
        DateTime dt = null;
        try {
            dt = ISODateTimeFormat.hourMinuteSecond().parseDateTime(string);
        } catch (IllegalArgumentException ex) {
            throw new ParsingException(loc, "Invalid time received; {}", ex.getMessage());
        }

        throw new SqlIllegalArgumentException("Time (only) literals are not supported; a date component is required as well");
    }

    @Override
    public Literal visitTimestampEscapedLiteral(TimestampEscapedLiteralContext ctx) {
        String string = string(ctx.string());

        Location loc = source(ctx);
        // parse yyyy-mm-dd hh:mm:ss(.f...)
        DateTime dt = null;
        try {
            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                    .append(ISODateTimeFormat.date())
                    .appendLiteral(" ")
                    .append(ISODateTimeFormat.hourMinuteSecondFraction())
                    .toFormatter();
            dt = formatter.parseDateTime(string);
        } catch (IllegalArgumentException ex) {
            throw new ParsingException(loc, "Invalid timestamp received; {}", ex.getMessage());
        }
        return new Literal(loc, dt, DataType.DATE);
    }

    @Override
    public Literal visitGuidEscapedLiteral(GuidEscapedLiteralContext ctx) {
        String string = string(ctx.string());

        Location loc = source(ctx.string());
        // basic validation
        String lowerCase = string.toLowerCase(Locale.ROOT);
        // needs to be format nnnnnnnn-nnnn-nnnn-nnnn-nnnnnnnnnnnn
        // since the length is fixed, the validation happens on absolute values
        // not pretty but it's fast and doesn't create any extra objects

        String errorPrefix = "Invalid GUID, ";

        if (lowerCase.length() != 36) {
            throw new ParsingException(loc, "{}too {}", errorPrefix, lowerCase.length() > 36 ? "long" : "short");
        }

        int[] separatorPos = { 8, 13, 18, 23 };
        for (int pos : separatorPos) {
            if (lowerCase.charAt(pos) != '-') {
                throw new ParsingException(loc, "{}expected group separator at offset [{}], found [{}]",
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
                throw new ParsingException(loc, "{}expected hexadecimal at offset[{}], found [{}]", errorPrefix, i, string.charAt(i));
            }
        }

        return new Literal(source(ctx), string, DataType.KEYWORD);
    }
}
