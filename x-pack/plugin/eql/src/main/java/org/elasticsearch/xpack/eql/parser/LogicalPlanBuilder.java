/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.BooleanExpressionContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.EventFilterContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinKeysContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinTermContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.NumberContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.PipeContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceParamsContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceTermContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.StatementContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SubqueryContext;
import org.elasticsearch.xpack.eql.plan.logical.Head;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.logical.Tail;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.tree.Source.synthetic;

public abstract class LogicalPlanBuilder extends ExpressionBuilder {

    static final String FILTER_PIPE = "filter", HEAD_PIPE = "head", TAIL_PIPE = "tail";

    static final Set<String> SUPPORTED_PIPES = Sets.newHashSet("count", FILTER_PIPE, HEAD_PIPE, "sort", TAIL_PIPE, "unique",
            "unique_count");

    private final UnresolvedRelation RELATION = new UnresolvedRelation(synthetic("<relation>"), null, "", false, "");
    private final EmptyAttribute UNSPECIFIED_FIELD = new EmptyAttribute(synthetic("<unspecified>"));

    public LogicalPlanBuilder(ParserParams params) {
        super(params);
    }

    private Attribute fieldTimestamp() {
        return new UnresolvedAttribute(synthetic("<timestamp>"), params.fieldTimestamp());
    }

    private Attribute fieldTiebreaker() {
        return params.fieldTiebreaker() != null ?
                new UnresolvedAttribute(synthetic("<tiebreaker>"), params.fieldTiebreaker()) : UNSPECIFIED_FIELD;
    }

    private OrderDirection resultPosition() {
        return params.resultPosition();
    }

    @Override
    public Object visitStatement(StatementContext ctx) {
        LogicalPlan plan = plan(ctx.query());

        //
        // Add implicit blocks
        //

        // the first pipe will be the implicit order
        // declared here for resolving any possible tie-breakers
        boolean asc = resultPosition() == OrderDirection.ASC;
        NullsPosition position = asc ? NullsPosition.FIRST : NullsPosition.LAST;

        List<Order> orders = new ArrayList<>(2);
        Source defaultOrderSource = synthetic("<default-order>");
        orders.add(new Order(defaultOrderSource, fieldTimestamp(), resultPosition(), position));
        // make sure to add the tiebreaker as well
        Attribute tiebreaker = fieldTiebreaker();
        if (Expressions.isPresent(tiebreaker)) {
            orders.add(new Order(defaultOrderSource, tiebreaker, resultPosition(), position));
        }
        plan = new OrderBy(defaultOrderSource, plan, orders);

        // add the default limit only if specified
        Literal defaultSize = new Literal(synthetic("<default-size>"), params.size(), DataTypes.INTEGER);
        Source defaultLimitSource = synthetic("<default-limit>");

        LogicalPlan previous = plan;
        boolean missingLimit = true;

        for (PipeContext pipeCtx : ctx.pipe()) {
            plan = pipe(pipeCtx, previous);
            if (missingLimit && plan instanceof LimitWithOffset) {
                missingLimit = false;
                if (plan instanceof Head) {
                    previous = new Head(defaultLimitSource, defaultSize, previous);
                } else {
                    previous = new Tail(defaultLimitSource, defaultSize, previous);
                }
                plan = plan.replaceChildrenSameSize(singletonList(previous));
            }
            previous = plan;
        }

        // add limit based on the default order if no tail/head was specified
        if (missingLimit) {
            if (asc) {
                plan = new Head(defaultLimitSource, defaultSize, plan);
            } else {
                plan = new Tail(defaultLimitSource, defaultSize, plan);
            }
        }

        return plan;
    }

    @Override
    public LogicalPlan visitEventQuery(EqlBaseParser.EventQueryContext ctx) {
        return visitEventFilter(ctx.eventFilter());
    }

    @Override
    public LogicalPlan visitEventFilter(EventFilterContext ctx) {
        Source source = source(ctx);
        Expression condition = expression(ctx.expression());

        if (ctx.event != null) {
            Source eventSource = source(ctx.event);
            String eventName = ctx.event.getText();
            if (eventName.startsWith("\"") || eventName.startsWith("'") || eventName.startsWith("?")) {
                eventName = unquoteString(source(ctx.event));
            }
            Literal eventValue = new Literal(eventSource, eventName, DataTypes.KEYWORD);

            UnresolvedAttribute eventField = new UnresolvedAttribute(eventSource, params.fieldEventCategory());
            Expression eventMatch = new Equals(eventSource, eventField, eventValue, params.zoneId());

            condition = new And(source, eventMatch, condition);
        }

        return new Filter(source, RELATION, condition);
    }

    @Override
    public Join visitJoin(JoinContext ctx) {
        List<Attribute> parentJoinKeys = visitJoinKeys(ctx.by);

        Source source = source(ctx);

        KeyedFilter until;

        int numberOfKeys = -1;
        List<KeyedFilter> queries = new ArrayList<>(ctx.joinTerm().size());

        for (JoinTermContext joinTermCtx : ctx.joinTerm()) {
            KeyedFilter joinTerm = visitJoinTerm(joinTermCtx, parentJoinKeys);
            int keySize = joinTerm.keys().size();
            if (numberOfKeys < 0) {
                numberOfKeys = keySize;
            } else {
                if (numberOfKeys != keySize) {
                    Source src = source(joinTermCtx.by != null ? joinTermCtx.by : joinTermCtx);
                    int expected = numberOfKeys - parentJoinKeys.size();
                    int found = keySize - parentJoinKeys.size();
                    throw new ParsingException(src, "Inconsistent number of join keys specified; expected [{}] but found [{}]", expected,
                            found);
                }
            }
            queries.add(joinTerm);
        }

        // until is already parsed through joinTerm() above
        if (ctx.until != null) {
            until = queries.remove(queries.size() - 1);
        } else {
            until = defaultUntil(source);
        }

        return new Join(source, queries, until, fieldTimestamp(), fieldTiebreaker(), resultPosition());
    }

    private KeyedFilter defaultUntil(Source source) {
        // no until declared means no results
        return new KeyedFilter(source, new LocalRelation(source, emptyList()), emptyList(), UNSPECIFIED_FIELD, UNSPECIFIED_FIELD);
    }

    public KeyedFilter visitJoinTerm(JoinTermContext ctx, List<Attribute> joinKeys) {
        return keyedFilter(joinKeys, ctx, ctx.by, ctx.subquery());
    }

    private KeyedFilter keyedFilter(List<Attribute> joinKeys, ParseTree ctx, JoinKeysContext joinCtx, SubqueryContext subqueryCtx) {
        List<Attribute> keys = CollectionUtils.combine(joinKeys, visitJoinKeys(joinCtx));
        LogicalPlan eventQuery = visitEventFilter(subqueryCtx.eventFilter());
        return new KeyedFilter(source(ctx), eventQuery, keys, fieldTimestamp(), fieldTiebreaker());
    }

    @Override
    public Sequence visitSequence(SequenceContext ctx) {
        Source source = source(ctx);

        if (ctx.disallowed != null && ctx.sequenceParams() != null) {
            throw new ParsingException(source, "Please specify sequence [by] before [with] not after");
        }

        List<Attribute> parentJoinKeys = visitJoinKeys(ctx.by);
        TimeValue maxSpan = visitSequenceParams(ctx.sequenceParams());

        KeyedFilter until;
        int numberOfKeys = -1;
        List<KeyedFilter> queries = new ArrayList<>(ctx.sequenceTerm().size());

        // TODO: unify this with the code from Join if the grammar gets aligned
        for (SequenceTermContext sequenceTermCtx : ctx.sequenceTerm()) {
            KeyedFilter sequenceTerm = visitSequenceTerm(sequenceTermCtx, parentJoinKeys);
            int keySize = sequenceTerm.keys().size();
            if (numberOfKeys < 0) {
                numberOfKeys = keySize;
            } else {
                if (numberOfKeys != keySize) {
                    Source src = source(sequenceTermCtx.by != null ? sequenceTermCtx.by : sequenceTermCtx);
                    int expected = numberOfKeys - parentJoinKeys.size();
                    int found = keySize - parentJoinKeys.size();
                    throw new ParsingException(src, "Inconsistent number of join keys specified; expected [{}] but found [{}]", expected,
                            found);
                }
            }
            queries.add(sequenceTerm);
        }

        // until is already parsed through sequenceTerm() above
        if (ctx.until != null) {
            until = queries.remove(queries.size() - 1);
        } else {
            until = defaultUntil(source);
        }

        return new Sequence(source, queries, until, maxSpan, fieldTimestamp(), fieldTiebreaker(), resultPosition());
    }

    public KeyedFilter visitSequenceTerm(SequenceTermContext ctx, List<Attribute> joinKeys) {
        return keyedFilter(joinKeys, ctx, ctx.by, ctx.subquery());
    }

    @Override
    public TimeValue visitSequenceParams(SequenceParamsContext ctx) {
        if (ctx == null) {
            return TimeValue.MINUS_ONE;
        }

        NumberContext numberCtx = ctx.timeUnit().number();
        if (numberCtx instanceof IntegerLiteralContext) {
            Number number = (Number) visitIntegerLiteral((IntegerLiteralContext) numberCtx).fold();
            long value = number.longValue();

            if (value <= 0) {
                throw new ParsingException(source(numberCtx), "A positive maxspan value is required; found [{}]", value);
            }

            String timeString = text(ctx.timeUnit().IDENTIFIER());

            if (timeString == null) {
                throw new ParsingException(source(ctx.timeUnit()), "No time unit specified, did you mean [s] as in [{}s]?", text(ctx
                        .timeUnit()));
            }

            TimeUnit timeUnit = null;
            switch (timeString) {
                case "ms":
                    timeUnit = TimeUnit.MILLISECONDS;
                    break;
                case "s":
                    timeUnit = TimeUnit.SECONDS;
                    break;
                case "m":
                    timeUnit = TimeUnit.MINUTES;
                    break;
                case "h":
                    timeUnit = TimeUnit.HOURS;
                    break;
                case "d":
                    timeUnit = TimeUnit.DAYS;
                    break;
                default:
                    throw new ParsingException(source(ctx.timeUnit().IDENTIFIER()),
                            "Unrecognized time unit [{}] in [{}], please specify one of [ms, s, m, h, d]",
                            timeString, text(ctx.timeUnit()));
            }

            return new TimeValue(value, timeUnit);

        } else {
            throw new ParsingException(source(numberCtx), "Decimal time interval [{}] not supported; please use an positive integer",
                    text(numberCtx));
        }
    }

    private LogicalPlan pipe(PipeContext ctx, LogicalPlan plan) {
        String name = text(ctx.IDENTIFIER());

        if (SUPPORTED_PIPES.contains(name) == false) {
            List<String> potentialMatches = StringUtils.findSimilar(name, SUPPORTED_PIPES);

            String msg = "Unrecognized pipe [{}]";
            if (potentialMatches.isEmpty() == false) {
                String matchString = potentialMatches.toString();
                msg += ", did you mean " + (potentialMatches.size() == 1
                        ? matchString
                        : "any of " + matchString) + "?";
            }
            throw new ParsingException(source(ctx.IDENTIFIER()), msg, name);
        }

        switch (name) {
            case HEAD_PIPE:
                Expression headLimit = pipeIntArgument(source(ctx), name, ctx.booleanExpression());
                return new Head(source(ctx), headLimit, plan);

            case TAIL_PIPE:
                Expression tailLimit = pipeIntArgument(source(ctx), name, ctx.booleanExpression());
                // negate the limit
                return new Tail(source(ctx), tailLimit, plan);

            default:
                throw new ParsingException(source(ctx), "Pipe [{}] is not supported", name);
        }
    }

    private Expression onlyOnePipeArgument(Source source, String pipeName, List<BooleanExpressionContext> exps) {
        int size = CollectionUtils.isEmpty(exps) ? 0 : exps.size();
        if (size != 1) {
            throw new ParsingException(source, "Pipe [{}] expects exactly one argument but found [{}]", pipeName, size);
        }
        return expression(exps.get(0));
    }

    private Expression pipeIntArgument(Source source, String pipeName, List<BooleanExpressionContext> exps) {
        Expression expression = onlyOnePipeArgument(source, pipeName, exps);

        if (expression.dataType().isInteger() == false || expression.foldable() == false || (int) expression.fold() < 0) {
            throw new ParsingException(expression.source(), "Pipe [{}] expects a positive integer but found [{}]", pipeName, expression
                    .sourceText());
        }

        return expression;
    }
}
