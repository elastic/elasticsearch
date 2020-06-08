/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.EventFilterContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinKeysContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinTermContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.NumberContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceParamsContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceTermContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SubqueryContext;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

public abstract class LogicalPlanBuilder extends ExpressionBuilder {

    private final UnresolvedRelation RELATION = new UnresolvedRelation(Source.EMPTY, null, "", false, "");
    private final EmptyAttribute UNSPECIFIED_FIELD = new EmptyAttribute(Source.EMPTY);

    public LogicalPlanBuilder(ParserParams params) {
        super(params);
    }

    private Attribute fieldTimestamp() {
        return new UnresolvedAttribute(Source.EMPTY, params.fieldTimestamp());
    }

    private Attribute fieldTieBreaker() {
        return params.fieldTieBreaker() != null ? new UnresolvedAttribute(Source.EMPTY, params.fieldTieBreaker()) : UNSPECIFIED_FIELD;
    }

    @Override
    public LogicalPlan visitEventQuery(EqlBaseParser.EventQueryContext ctx) {
        return new Project(source(ctx), visitEventFilter(ctx.eventFilter()), emptyList());
    }

    @Override
    public LogicalPlan visitEventFilter(EventFilterContext ctx) {
        Source source = source(ctx);
        Expression condition = expression(ctx.expression());

        if (ctx.event != null) {
            Source eventSource = source(ctx.event);
            String eventName = visitIdentifier(ctx.event);
            Literal eventValue = new Literal(eventSource, eventName, DataTypes.KEYWORD);

            UnresolvedAttribute eventField = new UnresolvedAttribute(eventSource, params.fieldEventCategory());
            Expression eventMatch = new Equals(eventSource, eventField, eventValue, params.zoneId());

            condition = new And(source, eventMatch, condition);
        }

        Filter filter = new Filter(source, RELATION, condition);
        List<Order> orders = new ArrayList<>(2);

        // TODO: add implicit sorting - when pipes are added, this would better sit there (as a default pipe)
        orders.add(new Order(source, fieldTimestamp(), Order.OrderDirection.ASC, Order.NullsPosition.FIRST));
        // make sure to add the tieBreaker as well
        Attribute tieBreaker = fieldTieBreaker();
        if (Expressions.isPresent(tieBreaker)) {
            orders.add(new Order(source, tieBreaker, Order.OrderDirection.ASC, Order.NullsPosition.FIRST));
        }

        OrderBy orderBy = new OrderBy(source, filter, orders);
        return orderBy;
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

        return new Join(source, queries, until, fieldTimestamp(), fieldTieBreaker());
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

        List<Attribute> output = CollectionUtils.combine(keys, fieldTimestamp());
        Attribute fieldTieBreaker = fieldTieBreaker();
        if (Expressions.isPresent(fieldTieBreaker)) {
            output = CollectionUtils.combine(output, fieldTieBreaker);
        }
        LogicalPlan child = new Project(source(ctx), eventQuery, output);

        return new KeyedFilter(source(ctx), child, keys, fieldTimestamp(), fieldTieBreaker());
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

        return new Sequence(source, queries, until, maxSpan, fieldTimestamp(), fieldTieBreaker());
    }

    public KeyedFilter visitSequenceTerm(SequenceTermContext ctx, List<Attribute> joinKeys) {
        if (ctx.FORK() != null) {
            throw new ParsingException(source(ctx.FORK()), "sequence fork is unsupported");
        }

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
}
