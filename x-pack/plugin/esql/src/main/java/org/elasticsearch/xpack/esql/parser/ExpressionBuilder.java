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
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigInteger;
import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.isInteger;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.PATTERN;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.VALUE;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.nameOrPosition;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.bigIntegerToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.parseTemporalAmount;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToIntegral;

public abstract class ExpressionBuilder extends IdentifierBuilder {

    private int expressionDepth = 0;

    /**
     * Maximum depth for nested expressions.
     * Avoids StackOverflowErrors at parse time with very convoluted expressions,
     * eg. EVAL x = sin(sin(sin(sin(sin(sin(sin(sin(sin(....sin(x)....)
     * ANTLR parser is recursive, so the only way to prevent a StackOverflow is to detect how
     * deep we are in the expression parsing and abort the query execution after a threshold
     *
     * This value is defined empirically, but the actual stack limit is highly
     * dependent on the JVM and on the JIT.
     *
     * A value of 500 proved to be right below the stack limit, but it still triggered
     * some CI failures (once every ~2000 iterations). see https://github.com/elastic/elasticsearch/issues/109846
     * Even though we didn't manage to reproduce the problem in real conditions, we decided
     * to reduce the max allowed depth to 400 (that is still a pretty reasonable limit for real use cases) and be more safe.
     *
     */
    public static final int MAX_EXPRESSION_DEPTH = 400;

    protected final ParsingContext context;

    public record ParsingContext(QueryParams params, PlanTelemetry telemetry) {}

    ExpressionBuilder(ParsingContext context) {
        this.context = context;
    }

    protected Expression expression(ParseTree ctx) {
        expressionDepth++;
        if (expressionDepth > MAX_EXPRESSION_DEPTH) {
            throw new ParsingException(
                "ESQL statement exceeded the maximum expression depth allowed ({}): [{}]",
                MAX_EXPRESSION_DEPTH,
                ctx.getParent().getText()
            );
        }
        try {
            return typedParsing(this, ctx, Expression.class);
        } finally {
            expressionDepth--;
        }
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(this, contexts, Expression.class);
    }

    @Override
    public Literal visitBooleanValue(EsqlBaseParser.BooleanValueContext ctx) {
        Source source = source(ctx);
        return new Literal(source, ctx.TRUE() != null, DataType.BOOLEAN);
    }

    @Override
    public Literal visitDecimalValue(EsqlBaseParser.DecimalValueContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        try {
            return new Literal(source, StringUtils.parseDouble(text), DataType.DOUBLE);
        } catch (InvalidArgumentException iae) {
            throw new ParsingException(source, iae.getMessage());
        }
    }

    @Override
    public Literal visitIntegerValue(EsqlBaseParser.IntegerValueContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();
        Number number;

        try {
            number = stringToIntegral(text);
        } catch (InvalidArgumentException siae) {
            // if it's too large, then quietly try to parse as a float instead
            try {
                return new Literal(source, EsqlDataTypeConverter.stringToDouble(text), DataType.DOUBLE);
            } catch (InvalidArgumentException ignored) {}

            throw new ParsingException(source, siae.getMessage());
        }

        Object val;
        DataType type;
        if (number instanceof BigInteger bi) {
            val = asLongUnsigned(bi);
            type = DataType.UNSIGNED_LONG;
        } else if (number.intValue() == number.longValue()) { // try to downsize to int if possible (since that's the most common type)
            val = number.intValue();
            type = DataType.INTEGER;
        } else {
            val = number.longValue();
            type = DataType.LONG;
        }
        return new Literal(source, val, type);
    }

    @Override
    public Object visitNumericArrayLiteral(EsqlBaseParser.NumericArrayLiteralContext ctx) {
        Source source = source(ctx);
        List<Literal> numbers = visitList(this, ctx.numericValue(), Literal.class);
        if (numbers.stream().anyMatch(l -> l.dataType() == DataType.DOUBLE)) {
            return new Literal(source, mapNumbers(numbers, (no, dt) -> no.doubleValue()), DataType.DOUBLE);
        }
        if (numbers.stream().anyMatch(l -> l.dataType() == DataType.UNSIGNED_LONG)) {
            return new Literal(
                source,
                mapNumbers(
                    numbers,
                    (no, dt) -> dt == DataType.UNSIGNED_LONG ? no.longValue() : bigIntegerToUnsignedLong(BigInteger.valueOf(no.longValue()))
                ),
                DataType.UNSIGNED_LONG
            );
        }
        if (numbers.stream().anyMatch(l -> l.dataType() == DataType.LONG)) {
            return new Literal(source, mapNumbers(numbers, (no, dt) -> no.longValue()), DataType.LONG);
        }
        return new Literal(source, mapNumbers(numbers, (no, dt) -> no.intValue()), DataType.INTEGER);
    }

    private List<Object> mapNumbers(List<Literal> numbers, BiFunction<Number, DataType, Object> map) {
        return numbers.stream().map(l -> map.apply((Number) l.value(), l.dataType())).toList();
    }

    @Override
    public Object visitBooleanArrayLiteral(EsqlBaseParser.BooleanArrayLiteralContext ctx) {
        return visitArrayLiteral(ctx, ctx.booleanValue(), DataType.BOOLEAN);
    }

    @Override
    public Object visitStringArrayLiteral(EsqlBaseParser.StringArrayLiteralContext ctx) {
        return visitArrayLiteral(ctx, ctx.string(), DataType.KEYWORD);
    }

    private Object visitArrayLiteral(ParserRuleContext ctx, List<? extends ParserRuleContext> contexts, DataType dataType) {
        Source source = source(ctx);
        List<Literal> literals = visitList(this, contexts, Literal.class);
        return new Literal(source, literals.stream().map(Literal::value).toList(), dataType);
    }

    @Override
    public Literal visitNullLiteral(EsqlBaseParser.NullLiteralContext ctx) {
        Source source = source(ctx);
        return new Literal(source, null, DataType.NULL);
    }

    @Override
    public Literal visitStringLiteral(EsqlBaseParser.StringLiteralContext ctx) {
        return visitString(ctx.string());
    }

    @Override
    public Literal visitString(EsqlBaseParser.StringContext ctx) {
        Source source = source(ctx);
        return new Literal(source, unquote(source), DataType.KEYWORD);
    }

    @Override
    public UnresolvedAttribute visitQualifiedName(EsqlBaseParser.QualifiedNameContext ctx) {
        if (ctx == null) {
            return null;
        }
        List<Object> items = visitList(this, ctx.identifierOrParameter(), Object.class);
        List<String> strings = new ArrayList<>(items.size());
        for (Object item : items) {
            if (item instanceof String s) {
                strings.add(s);
            } else if (item instanceof Expression e) {
                strings.add(unresolvedAttributeNameInParam(ctx, e));
            }
        }
        return new UnresolvedAttribute(source(ctx), Strings.collectionToDelimitedString(strings, "."));
    }

    @Override
    public List<NamedExpression> visitQualifiedNamePatterns(EsqlBaseParser.QualifiedNamePatternsContext ctx) {
        return visitQualifiedNamePatterns(ctx, ne -> {});
    }

    protected List<NamedExpression> visitQualifiedNamePatterns(
        EsqlBaseParser.QualifiedNamePatternsContext ctx,
        Consumer<NamedExpression> checker
    ) {
        if (ctx == null) {
            return emptyList();
        }
        List<EsqlBaseParser.QualifiedNamePatternContext> identifiers = ctx.qualifiedNamePattern();
        List<NamedExpression> names = new ArrayList<>(identifiers.size());

        for (EsqlBaseParser.QualifiedNamePatternContext patternContext : identifiers) {
            names.add(visitQualifiedNamePattern(patternContext, checker));
        }

        return names;
    }

    protected NamedExpression visitQualifiedNamePattern(
        EsqlBaseParser.QualifiedNamePatternContext patternContext,
        Consumer<NamedExpression> checker
    ) {
        NamedExpression ne = visitQualifiedNamePattern(patternContext);
        checker.accept(ne);
        return ne;
    }

    @Override
    public NamedExpression visitQualifiedNamePattern(EsqlBaseParser.QualifiedNamePatternContext ctx) {
        if (ctx == null) {
            return null;
        }

        var src = source(ctx);
        StringBuilder patternString = new StringBuilder();
        StringBuilder nameString = new StringBuilder();
        var patterns = ctx.identifierPattern();

        // check special wildcard case
        if (patterns.size() == 1) {
            var idCtx = patterns.get(0);
            boolean unresolvedStar = false;
            if (idCtx.ID_PATTERN() != null && idCtx.ID_PATTERN().getText().equals(WILDCARD)) {
                unresolvedStar = true;
            }
            if (idCtx.parameter() != null || idCtx.doubleParameter() != null) {
                ParseTree paramCtx = idCtx.parameter();
                ParseTree doubleParamsCtx = idCtx.doubleParameter();
                Expression exp = expression(paramCtx != null ? paramCtx : doubleParamsCtx);
                if (exp instanceof Literal lit) {
                    if (lit.value() != null) {
                        throw new ParsingException(
                            src,
                            "Query parameter [{}] with value [{}] declared as a constant, cannot be used as an identifier or pattern",
                            ctx.getText(),
                            lit.value()
                        );
                    }
                } else if (exp instanceof UnresolvedNamePattern up) {
                    if (up.name() != null && up.name().equals(WILDCARD)) {
                        unresolvedStar = true;
                    }
                }
            }
            if (unresolvedStar) {
                return new UnresolvedStar(src, null);
            }
        }

        boolean hasPattern = false;
        // Builds a list of either strings (which map verbatim) or Automatons which match any string
        List<Object> objects = new ArrayList<>(patterns.size());
        for (int i = 0, s = patterns.size(); i < s; i++) {
            if (i > 0) {
                patternString.append(".");
                nameString.append(".");
                objects.add(".");
            }

            String patternContext = "";
            EsqlBaseParser.IdentifierPatternContext pattern = patterns.get(i);
            if (pattern.ID_PATTERN() != null) {
                patternContext = pattern.ID_PATTERN().getText();
            } else if (pattern.parameter() != null || pattern.doubleParameter() != null) {
                ParseTree paramCtx = pattern.parameter();
                ParseTree doubleParamsCtx = pattern.doubleParameter();
                Expression exp = expression(paramCtx != null ? paramCtx : doubleParamsCtx);
                if (exp instanceof Literal lit) {
                    // only Literal.NULL can happen with missing params, params for constants are not allowed
                    if (lit.value() != null) {
                        throw new ParsingException(src, "Constant [{}] is unsupported for [{}]", pattern, ctx.getText());
                    }
                } else if (exp instanceof UnresolvedAttribute ua) { // identifier provided in QueryParam is treated as unquoted string
                    String unquotedIdentifier = ua.name();
                    String quotedIdentifier = quoteIdString(unquotedIdentifier);
                    patternString.append(quotedIdentifier);
                    objects.add(unquotedIdentifier);
                    nameString.append(unquotedIdentifier);
                    continue;
                } else if (exp instanceof UnresolvedNamePattern up) {
                    patternContext = up.name();
                }
            } else {
                throw new ParsingException(src, "Unsupported field name pattern [{}]", pattern);
            }
            if (patternContext.isEmpty()) { // empty pattern can happen with missing params
                continue;
            }
            // as this is one big string of fragments mashed together, break it manually into quoted vs unquoted
            // for readability reasons, do a first pass to break the string into fragments and then process each of them
            // to avoid doing a string allocation
            List<String> fragments = breakIntoFragments(patternContext);

            for (var fragment : fragments) {
                // unquoted fragment
                // a wildcard that matches can only appear in an unquoted section
                if (fragment.charAt(0) == '`' == false) {
                    patternString.append(fragment);
                    nameString.append(fragment);
                    // loop the string itself to extract any * and make them an automaton directly
                    // the code is somewhat messy but doesn't invoke the full blown Regex engine either
                    if (Regex.isSimpleMatchPattern(fragment)) {
                        hasPattern = true;
                        String str = fragment;
                        boolean keepGoing = false;
                        do {
                            int localIndex = str.indexOf('*');
                            // in case of match
                            if (localIndex != -1) {
                                keepGoing = true;
                                // copy any prefix string
                                if (localIndex > 0) {
                                    objects.add(str.substring(0, localIndex));
                                }
                                objects.add(Automata.makeAnyString());
                                localIndex++;
                                // trim the string
                                if (localIndex < str.length()) {
                                    str = str.substring(localIndex);
                                } else {
                                    keepGoing = false;
                                }
                            }
                            // no more matches, copy leftovers and end the loop
                            else {
                                keepGoing = false;
                                if (str.length() > 0) {
                                    objects.add(str);
                                }
                            }
                        } while (keepGoing);
                    } else {
                        objects.add(fragment);
                    }
                }
                // quoted - definitely no pattern
                else {
                    patternString.append(fragment);
                    var unquotedString = unquoteIdString(fragment);
                    objects.add(unquotedString);
                    nameString.append(unquotedString);
                }
            }
        }

        NamedExpression result;
        // need to combine automaton
        if (hasPattern) {
            // add . as optional matching
            List<Automaton> list = new ArrayList<>(objects.size());
            for (var o : objects) {
                list.add(o instanceof Automaton a ? a : Automata.makeString(o.toString()));
            }
            // use the fast run variant
            result = new UnresolvedNamePattern(
                src,
                new CharacterRunAutomaton(Operations.concatenate(list)),
                patternString.toString(),
                nameString.toString()
            );
        } else {
            result = new UnresolvedAttribute(src, Strings.collectionToDelimitedString(objects, ""));
        }
        return result;
    }

    static List<String> breakIntoFragments(String idPattern) {
        List<String> fragments = new ArrayList<>();
        char backtick = '`';
        boolean inQuotes = false;
        boolean keepGoing = true;
        int from = 0, offset = -1;

        do {
            offset = idPattern.indexOf(backtick, offset);
            String fragment = null;
            // unquoted fragment
            if (offset < 0) {
                keepGoing = false;
                // pick trailing string
                fragment = idPattern.substring(from);
            }
            // quoted fragment
            else {
                // if not in quotes
                // copy the string over
                // otherwise keep on going
                if (inQuotes == false) {
                    inQuotes = true;
                    if (offset != 0) {
                        fragment = idPattern.substring(from, offset);
                        from = offset;
                    }
                } // in quotes
                else {
                    // if double backtick keep on going
                    var ahead = offset + 1;
                    if (ahead < idPattern.length() && idPattern.charAt(ahead) == backtick) {
                        // move offset
                        offset = ahead;
                    }
                    // otherwise end the quote
                    else {
                        inQuotes = false;
                        // include the quote
                        offset++;
                        fragment = idPattern.substring(from, offset);
                        from = offset;
                    }
                }
                // keep moving the offset
                offset++;
            }
            if (fragment != null) {
                fragments.add(fragment);
            }
        } while (keepGoing && offset <= idPattern.length());
        return fragments;
    }

    @Override
    public Object visitQualifiedIntegerLiteral(EsqlBaseParser.QualifiedIntegerLiteralContext ctx) {
        Source source = source(ctx);
        Literal intLit = typedParsing(this, ctx.integerValue(), Literal.class);
        Number value = (Number) intLit.value();
        if (intLit.dataType() == DataType.UNSIGNED_LONG) {
            value = unsignedLongAsNumber(value.longValue());
        }
        String qualifier = ctx.UNQUOTED_IDENTIFIER().getText().toLowerCase(Locale.ROOT);

        try {
            TemporalAmount quantity = parseTemporalAmount(value, qualifier, source);
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
            case EsqlBaseParser.CIEQ -> new InsensitiveEquals(source, left, right);
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
        String name = visitFunctionName(ctx.functionName());
        List<Expression> args = expressions(ctx.booleanExpression());
        if (ctx.mapExpression() != null) {
            MapExpression mapArg = visitMapExpression(ctx.mapExpression());
            args.add(mapArg);
        }
        if ("is_null".equals(EsqlFunctionRegistry.normalizeName(name))) {
            throw new ParsingException(
                source(ctx),
                "is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
            );
        }
        if ("count".equals(EsqlFunctionRegistry.normalizeName(name))) {
            // to simplify the registration, handle in the parser the special count cases
            if (args.isEmpty() || ctx.ASTERISK() != null) {
                args = singletonList(new Literal(source(ctx), "*", DataType.KEYWORD));
            }
        }
        return new UnresolvedFunction(source(ctx), name, FunctionResolutionStrategy.DEFAULT, args);
    }

    @Override
    public String visitFunctionName(EsqlBaseParser.FunctionNameContext ctx) {
        var name = visitIdentifierOrParameter(ctx.identifierOrParameter());
        context.telemetry().function(name);
        return name;
    }

    @Override
    public MapExpression visitMapExpression(EsqlBaseParser.MapExpressionContext ctx) {
        List<Expression> namedArgs = new ArrayList<>(ctx.entryExpression().size());
        List<String> names = new ArrayList<>(ctx.entryExpression().size());
        List<EsqlBaseParser.EntryExpressionContext> kvCtx = ctx.entryExpression();
        for (EsqlBaseParser.EntryExpressionContext entry : kvCtx) {
            EsqlBaseParser.StringContext stringCtx = entry.string();
            String key = unquote(stringCtx.QUOTED_STRING().getText()); // key is case-sensitive
            if (key.isBlank()) {
                throw new ParsingException(
                    source(ctx),
                    "Invalid named function argument [{}], empty key is not supported",
                    entry.getText()
                );
            }
            if (names.contains(key)) {
                throw new ParsingException(source(ctx), "Duplicated function arguments with the same name [{}] is not supported", key);
            }
            Expression value = expression(entry.constant());
            String entryText = entry.getText();
            if (value instanceof Literal l) {
                if (l.dataType() == NULL) {
                    throw new ParsingException(source(ctx), "Invalid named function argument [{}], NULL is not supported", entryText);
                }
                namedArgs.add(new Literal(source(stringCtx), key, KEYWORD));
                namedArgs.add(l);
                names.add(key);
            } else {
                throw new ParsingException(
                    source(ctx),
                    "Invalid named function argument [{}], only constant value is supported",
                    entryText
                );
            }
        }
        return new MapExpression(Source.EMPTY, namedArgs);
    }

    @Override
    public String visitIdentifierOrParameter(EsqlBaseParser.IdentifierOrParameterContext ctx) {
        if (ctx.identifier() != null) {
            return visitIdentifier(ctx.identifier());
        }
        if (ctx.parameter() != null) {
            return unresolvedAttributeNameInParam(ctx.parameter(), expression(ctx.parameter()));
        }
        return unresolvedAttributeNameInParam(ctx.doubleParameter(), expression(ctx.doubleParameter()));
    }

    @Override
    public Expression visitInlineCast(EsqlBaseParser.InlineCastContext ctx) {
        return castToType(source(ctx), ctx.primaryExpression(), ctx.dataType());
    }

    private Expression castToType(Source source, ParseTree parseTree, EsqlBaseParser.DataTypeContext dataTypeCtx) {
        DataType dataType = typedParsing(this, dataTypeCtx, DataType.class);
        var converterToFactory = EsqlDataTypeConverter.converterFunctionFactory(dataType);
        if (converterToFactory == null) {
            throw new ParsingException(source, "Unsupported conversion to type [{}]", dataType);
        }
        Expression expr = expression(parseTree);
        var convertFunction = converterToFactory.apply(source, expr);
        context.telemetry().function(convertFunction.getClass());
        return convertFunction;
    }

    @Override
    public DataType visitToDataType(EsqlBaseParser.ToDataTypeContext ctx) {
        String typeName = visitIdentifier(ctx.identifier());
        DataType dataType = DataType.fromNameOrAlias(typeName);
        if (dataType == DataType.UNSUPPORTED) {
            throw new ParsingException(source(ctx), "Unknown data type named [{}]", typeName);
        }
        return dataType;
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
            case EsqlBaseParser.LIKE -> {
                try {
                    yield new WildcardLike(
                        source,
                        left,
                        new WildcardPattern(pattern.fold(FoldContext.small() /* TODO remove me */).toString())
                    );
                } catch (InvalidArgumentException e) {
                    throw new ParsingException(source, "Invalid pattern for LIKE [{}]: [{}]", pattern, e.getMessage());
                }
            }
            case EsqlBaseParser.RLIKE -> new RLike(
                source,
                left,
                new RLikePattern(pattern.fold(FoldContext.small() /* TODO remove me */).toString())
            );
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

    @Override
    public Alias visitRenameClause(EsqlBaseParser.RenameClauseContext ctx) {
        Source src = source(ctx);
        NamedExpression newName = visitQualifiedNamePattern(ctx.newName);
        NamedExpression oldName = visitQualifiedNamePattern(ctx.oldName);
        if (newName instanceof UnresolvedNamePattern
            || oldName instanceof UnresolvedNamePattern
            || newName instanceof UnresolvedStar
            || oldName instanceof UnresolvedStar) {
            throw new ParsingException(src, "Using wildcards [*] in RENAME is not allowed [{}]", src.text());
        }

        return new Alias(src, newName.name(), oldName);
    }

    @Override
    public NamedExpression visitEnrichWithClause(EsqlBaseParser.EnrichWithClauseContext ctx) {
        Source src = source(ctx);
        NamedExpression enrichField = enrichFieldName(ctx.enrichField);
        NamedExpression newName = enrichFieldName(ctx.newName);
        return newName == null ? enrichField : new Alias(src, newName.name(), enrichField);
    }

    private NamedExpression enrichFieldName(EsqlBaseParser.QualifiedNamePatternContext ctx) {
        return visitQualifiedNamePattern(ctx, ne -> {
            if (ne instanceof UnresolvedNamePattern || ne instanceof UnresolvedStar) {
                var src = ne.source();
                throw new ParsingException(src, "Using wildcards [*] in ENRICH WITH projections is not allowed, found [{}]", src.text());
            }
        });
    }

    @Override
    public Alias visitField(EsqlBaseParser.FieldContext ctx) {
        return visitField(ctx, source(ctx));
    }

    private Alias visitField(EsqlBaseParser.FieldContext ctx, Source source) {
        UnresolvedAttribute id = visitQualifiedName(ctx.qualifiedName());
        Expression value = expression(ctx.booleanExpression());
        String name = id == null ? source.text() : id.name();
        return new Alias(source, name, value);
    }

    @Override
    public List<Alias> visitFields(EsqlBaseParser.FieldsContext ctx) {
        return ctx != null ? visitList(this, ctx.field(), Alias.class) : new ArrayList<>();
    }

    @Override
    public Alias visitRerankField(EsqlBaseParser.RerankFieldContext ctx) {
        return visitRerankField(ctx, source(ctx));
    }

    private Alias visitRerankField(EsqlBaseParser.RerankFieldContext ctx, Source source) {
        UnresolvedAttribute id = visitQualifiedName(ctx.qualifiedName());
        assert id != null;

        var boolExprCtx = ctx.booleanExpression();
        Expression value = boolExprCtx == null ? id : expression(boolExprCtx);
        return new Alias(source, id.name(), value);
    }

    @Override
    public List<Alias> visitRerankFields(EsqlBaseParser.RerankFieldsContext ctx) {
        return ctx != null ? visitList(this, ctx.rerankField(), Alias.class) : new ArrayList<>();
    }

    @Override
    public NamedExpression visitAggField(EsqlBaseParser.AggFieldContext ctx) {
        Source source = source(ctx);
        Alias field = visitField(ctx.field(), source);
        var filterExpression = ctx.booleanExpression();

        if (filterExpression != null) {
            Expression condition = expression(filterExpression);
            Expression child = field.child();
            // basic check as the filter can be specified only on a function (should be an aggregate but we can't determine that yet)
            if (field.child().anyMatch(Function.class::isInstance)) {
                field = field.replaceChild(new FilteredExpression(field.source(), child, condition));
            }
            // allow condition only per aggregated function
            else {
                throw new ParsingException(
                    condition.source(),
                    "WHERE clause allowed only for aggregate functions [{}]",
                    field.sourceText()
                );
            }
        }
        return field;
    }

    @Override
    public List<Alias> visitAggFields(EsqlBaseParser.AggFieldsContext ctx) {
        return ctx != null ? visitList(this, ctx.aggField(), Alias.class) : new ArrayList<>();
    }

    /**
     * Similar to {@link #visitFields(EsqlBaseParser.FieldsContext)} however avoids wrapping the expression
     * into an Alias.
     */
    public List<NamedExpression> visitGrouping(EsqlBaseParser.FieldsContext ctx) {
        List<NamedExpression> list;
        if (ctx != null) {
            var fields = ctx.field();
            list = new ArrayList<>(fields.size());
            for (EsqlBaseParser.FieldContext field : fields) {
                NamedExpression ne = null;
                UnresolvedAttribute id = visitQualifiedName(field.qualifiedName());
                Expression value = expression(field.booleanExpression());
                String name = null;
                if (id == null) {
                    // when no alias has been specified, see if the underling one can be reused
                    if (value instanceof Attribute a) {
                        ne = a;
                    } else {
                        name = source(field).text();
                    }
                } else {
                    name = id.name();
                }
                // wrap when necessary - no alias and no underlying attribute
                if (ne == null) {
                    ne = new Alias(source(field), name, value);
                }
                list.add(ne);
            }
        } else {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public Expression visitInputParam(EsqlBaseParser.InputParamContext ctx) {
        QueryParam param = paramByToken(ctx.PARAM());
        return visitParam(ctx, param);
    }

    @Override
    public Expression visitInputNamedOrPositionalParam(EsqlBaseParser.InputNamedOrPositionalParamContext ctx) {
        QueryParam param = paramByNameOrPosition(ctx.NAMED_OR_POSITIONAL_PARAM());
        if (param == null) {
            return Literal.NULL;
        }
        return visitParam(ctx, param);
    }

    private Expression visitParam(EsqlBaseParser.ParameterContext ctx, QueryParam param) {
        Source source = source(ctx);
        DataType type = param.type();
        var value = param.value();
        ParserUtils.ParamClassification classification = param.classification();
        // RequestXContent does not allow null value for identifier or pattern
        if (value != null && classification != VALUE) {
            if (classification == PATTERN) {
                // let visitQualifiedNamePattern create a real UnresolvedNamePattern with Automaton
                return new UnresolvedNamePattern(source, null, value.toString(), value.toString());
            } else {
                return new UnresolvedAttribute(source, value.toString());
            }
        }
        return new Literal(source, value, type);
    }

    QueryParam paramByToken(TerminalNode node) {
        if (node == null) {
            return null;
        }
        Token token = node.getSymbol();
        if (context.params().contains(token) == false) {
            throw new ParsingException(source(node), "Unexpected parameter");
        }
        return context.params().get(token);
    }

    QueryParam paramByNameOrPosition(TerminalNode node) {
        if (node == null) {
            return null;
        }
        // The token could be a single parameter marker or double parameter markers
        Token token = node.getSymbol();
        String nameOrPosition = nameOrPosition(token);
        if (isInteger(nameOrPosition)) {
            int index = Integer.parseInt(nameOrPosition);
            if (context.params().get(index) == null) {
                String message = "";
                int np = context.params().size();
                if (np > 0) {
                    message = ", did you mean " + (np == 1 ? "position 1?" : "any position between 1 and " + np + "?");
                }
                context.params()
                    .addParsingError(new ParsingException(source(node), "No parameter is defined for position " + index + message));
            }
            return context.params().get(index);
        } else {
            if (context.params().contains(nameOrPosition) == false) {
                String message = "";
                List<String> potentialMatches = StringUtils.findSimilar(nameOrPosition, context.params().namedParams().keySet());
                if (potentialMatches.size() > 0) {
                    message = ", did you mean "
                        + (potentialMatches.size() == 1 ? "[" + potentialMatches.get(0) + "]?" : "any of " + potentialMatches + "?");
                }
                context.params()
                    .addParsingError(new ParsingException(source(node), "Unknown query parameter [" + nameOrPosition + "]" + message));
            }
            return context.params().get(nameOrPosition);
        }
    }

    String unresolvedAttributeNameInParam(ParserRuleContext ctx, Expression param) {
        String invalidParam = "Query parameter [{}]{}, cannot be used as an identifier";
        if (param instanceof Literal lit) {
            throw new ParsingException(
                source(ctx),
                invalidParam,
                ctx.getText(),
                lit.value() != null ? " with value [" + lit.value() + "] declared as a constant" : " is null or undefined"
            );
        } else if (param instanceof UnresolvedNamePattern up) {
            throw new ParsingException(source(ctx), invalidParam, ctx.getText(), "[" + up.name() + "] declared as a pattern");
        } else if (param instanceof UnresolvedAttribute ua) {
            if (ua.name() != null) {
                return ua.name();
            } else { // this should not happen
                throw new ParsingException(source(ctx), invalidParam, ctx.getText(), "[null]");
            }
        } else {
            throw new ParsingException(source(ctx), invalidParam, ctx.getText(), "[null]");
        }
    }

    @Override
    public Expression visitInputDoubleParams(EsqlBaseParser.InputDoubleParamsContext ctx) {
        QueryParam param = paramByToken(ctx.DOUBLE_PARAMS());
        return visitDoubleParam(ctx, param);
    }

    @Override
    public Expression visitInputNamedOrPositionalDoubleParams(EsqlBaseParser.InputNamedOrPositionalDoubleParamsContext ctx) {
        QueryParam param = paramByNameOrPosition(ctx.NAMED_OR_POSITIONAL_DOUBLE_PARAMS());
        if (param == null) {
            // We could come here when a named or positional double param is undefined
            // return an UnresolvedAttribute with name=null instead of null,
            // so that ParsingException will be collected and thrown at the end of EsqlParser
            return new UnresolvedAttribute(source(ctx), "null");
        }
        return visitDoubleParam(ctx, param);
    }

    /**
      * Double parameter markers represent identifiers, e.g. field or function names. An {@code UnresolvedAttribute}
      * is returned regardless how the param is specified in the request.
      */
    private Expression visitDoubleParam(EsqlBaseParser.DoubleParameterContext ctx, QueryParam param) {
        if (param.classification() == PATTERN) {
            context.params.addParsingError(
                new ParsingException(
                    source(ctx),
                    "Query parameter [{}]{}, cannot be used as an identifier",
                    ctx.getText(),
                    "[" + param.name() + "] declared as a pattern"
                )
            );
        }
        return new UnresolvedAttribute(source(ctx), param.value().toString());
    }

    @Override
    public Expression visitMatchBooleanExpression(EsqlBaseParser.MatchBooleanExpressionContext ctx) {

        final Expression matchFieldExpression;
        if (ctx.fieldType != null) {
            matchFieldExpression = castToType(source(ctx), ctx.fieldExp, ctx.fieldType);
        } else {
            matchFieldExpression = expression(ctx.fieldExp);
        }

        return new MatchOperator(source(ctx), matchFieldExpression, expression(ctx.matchQuery));
    }
}
