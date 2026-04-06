/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.EMPTY;

public final class ParserUtils {
    public enum ParamClassification {
        VALUE,
        IDENTIFIER,
        PATTERN
    }

    public static final Map<String, ParamClassification> paramClassifications = Maps.newMapWithExpectedSize(
        ParamClassification.values().length
    );

    static {
        for (ParamClassification e : ParamClassification.values()) {
            paramClassifications.put(e.name(), e);
        }
    }

    private static final int SINGLE_PARAM = "?".length();

    private static final int DOUBLE_PARAM = "??".length();

    private ParserUtils() {}

    public static Object visit(Function<ParseTree, Object> visitor, ParseTree tree) {
        Object result = visitor.apply(tree);
        Check.notNull(result, "Don't know how to handle context [{}] with value [{}]", tree.getClass(), tree.getText());
        return result;
    }

    public static <T> List<T> visitList(ParseTreeVisitor<?> visitor, List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        if (contexts == null || contexts.isEmpty()) {
            return emptyList();
        }

        List<T> results = new ArrayList<>(contexts.size());
        for (ParserRuleContext context : contexts) {
            results.add(clazz.cast(visitor.visit(context)));
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> T typedParsing(ParseTreeVisitor<?> visitor, ParseTree ctx, Class<T> type) {
        Object result = ctx.accept(visitor);
        if (type.isInstance(result)) {
            return (T) result;
        }

        throw new ParsingException(
            source(ctx),
            "Invalid query '{}'[{}] given; expected {} but found {}",
            ctx.getText(),
            ctx.getClass().getSimpleName(),
            type.getSimpleName(),
            (result != null ? result.getClass().getSimpleName() : "null")
        );
    }

    public static Source source(ParseTree ctx) {
        if (ctx instanceof ParserRuleContext) {
            return source((ParserRuleContext) ctx);
        }
        return Source.EMPTY;
    }

    public static Source source(TerminalNode terminalNode) {
        Check.notNull(terminalNode, "terminalNode is null");
        return source(terminalNode.getSymbol());
    }

    public static Source source(ParserRuleContext parserRuleContext) {
        Check.notNull(parserRuleContext, "parserRuleContext is null");
        Token start = parserRuleContext.start;
        Token stop = parserRuleContext.stop != null ? parserRuleContext.stop : start;
        return source(start, stop);
    }

    public static Source source(Token token) {
        Check.notNull(token, "token is null");
        String text = token.getInputStream().getText(new Interval(token.getStartIndex(), token.getStopIndex()));
        return new Source(new Location(token.getLine(), token.getCharPositionInLine()), text);
    }

    public static Source source(ParserRuleContext begin, ParserRuleContext end) {
        Check.notNull(begin, "begin is null");
        Check.notNull(end, "end is null");
        Token start = begin.start;
        Token stop = end.stop != null ? end.stop : begin.stop;
        return source(start, stop);
    }

    public static Source source(TerminalNode begin, ParserRuleContext end) {
        Check.notNull(begin, "begin is null");
        Check.notNull(end, "end is null");
        Token start = begin.getSymbol();
        Token stop = end.stop != null ? end.stop : start;
        return source(start, stop);
    }

    public static Source source(Token start, Token stop) {
        Check.notNull(start, "start is null");
        stop = stop == null ? start : stop;
        String text = start.getInputStream().getText(new Interval(start.getStartIndex(), stop.getStopIndex()));
        return new Source(new Location(start.getLine(), start.getCharPositionInLine()), text);
    }

    /**
     * Retrieves the raw text of the node (without interpreting it as a string literal).
     */
    public static String text(ParseTree node) {
        return node == null ? null : node.getText();
    }

    /**
     * Extract the name or the position of an ES|QL parameter.
     */
    public static String nameOrPosition(Token token) {
        int tokenType = token.getType();
        return switch (tokenType) {
            case EsqlBaseLexer.NAMED_OR_POSITIONAL_PARAM -> token.getText().substring(SINGLE_PARAM);
            case EsqlBaseLexer.NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> token.getText().substring(DOUBLE_PARAM);
            default -> EMPTY;
        };
    }

    /**
     * Extract the name or the position of a PromQL parameter.
     * Kept separate from {@link #nameOrPosition} since the two lexers assign independent token type IDs
     * that can collide after grammar changes.
     */
    public static String promqlNameOrPosition(Token token) {
        int tokenType = token.getType();
        return switch (tokenType) {
            case PromqlBaseLexer.NAMED_OR_POSITIONAL_PARAM -> token.getText().substring(SINGLE_PARAM);
            default -> EMPTY;
        };
    }

    public static String unquoteIdString(String quotedString) {
        return quotedString.substring(1, quotedString.length() - 1).replace("``", "`");
    }

    public static String quoteIdString(String unquotedString) {
        return "`" + unquotedString.replace("`", "``") + "`";
    }

    public record Stats(List<Expression> groupings, List<? extends NamedExpression> aggregates) {}

    public static Stats buildStats(Source source, List<Expression> groupings, List<NamedExpression> aggregates) {
        if (aggregates.isEmpty() && groupings.isEmpty()) {
            throw new ParsingException(source, "At least one aggregation or grouping expression required in [{}]", source.text());
        }
        // grouping keys are automatically added as aggregations however the user is not allowed to specify them
        if (groupings.isEmpty() == false && aggregates.isEmpty() == false) {
            var groupNames = new LinkedHashSet<>(Expressions.names(groupings));
            var groupRefNames = new LinkedHashSet<>(Expressions.names(Expressions.references(groupings)));

            for (Expression aggregate : aggregates) {
                Expression e = Alias.unwrap(aggregate);
                if (e.resolved() == false && e instanceof UnresolvedFunction == false) {
                    String name = e.sourceText();
                    if (groupNames.contains(name)) {
                        throw new VerificationException(
                            Collections.singletonList(Failure.fail(e, "grouping key [{}] already specified in the STATS BY clause", name))
                        );
                    } else if (groupRefNames.contains(name)) {
                        throw new VerificationException(
                            Collections.singletonList(Failure.fail(e, "Cannot specify grouping expression [{}] as an aggregate", name))
                        );
                    }
                }
            }
        }
        // since groupings are aliased, add refs to it in the aggregates
        for (Expression group : groupings) {
            aggregates.add(Expressions.attribute(group));
        }
        return new Stats(new ArrayList<>(groupings), aggregates);
    }
}
