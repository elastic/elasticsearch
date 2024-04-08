/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.BooleanExpressionContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.EventFilterContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinKeysContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.JoinTermContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.NumberContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.PipeContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SampleContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceParamsContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SequenceTermContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.StatementContext;
import org.elasticsearch.xpack.eql.parser.EqlBaseParser.SubqueryContext;
import org.elasticsearch.xpack.eql.plan.logical.Head;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.logical.Sample;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.logical.Tail;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.text;
import static org.elasticsearch.xpack.ql.tree.Source.synthetic;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;

public abstract class LogicalPlanBuilder extends ExpressionBuilder {

    static final String FILTER_PIPE = "filter", HEAD_PIPE = "head", TAIL_PIPE = "tail", RUNS = "runs";

    static final Set<String> SUPPORTED_PIPES = Set.of("count", FILTER_PIPE, HEAD_PIPE, "sort", TAIL_PIPE, "unique", "unique_count");

    private final UnresolvedRelation RELATION = new UnresolvedRelation(synthetic("<relation>"), null, "", false, "");
    private final EmptyAttribute UNSPECIFIED_FIELD = new EmptyAttribute(synthetic("<unspecified>"));
    private static final int MAX_SAMPLE_QUERIES = 5;

    public LogicalPlanBuilder(ParserParams params) {
        super(params);
    }

    private Attribute fieldTimestamp() {
        return new UnresolvedAttribute(synthetic("<timestamp>"), params.fieldTimestamp());
    }

    private Attribute fieldTiebreaker() {
        return params.fieldTiebreaker() != null
            ? new UnresolvedAttribute(synthetic("<tiebreaker>"), params.fieldTiebreaker())
            : UNSPECIFIED_FIELD;
    }

    private OrderDirection resultPosition() {
        return params.resultPosition();
    }

    @Override
    public Object visitStatement(StatementContext ctx) {
        LogicalPlan plan = plan(ctx.query());

        if (plan instanceof Sample) {
            if (ctx.pipe().size() > 0) {
                throw new ParsingException(source(ctx.pipe().get(0)), "Samples do not support pipes yet");
            }
            return new LimitWithOffset(plan.source(), new Literal(Source.EMPTY, params.size(), INTEGER), 0, plan);
        }
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
        Literal defaultSize = new Literal(synthetic("<default-size>"), params.size(), INTEGER);
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
            KeyedFilter joinTerm = visitJoinTerm(joinTermCtx, parentJoinKeys, fieldTimestamp(), fieldTiebreaker());
            int keySize = joinTerm.keys().size();
            if (numberOfKeys < 0) {
                numberOfKeys = keySize;
            } else {
                if (numberOfKeys != keySize) {
                    Source src = source(joinTermCtx.by != null ? joinTermCtx.by : joinTermCtx);
                    int expected = numberOfKeys - parentJoinKeys.size();
                    int found = keySize - parentJoinKeys.size();
                    throw new ParsingException(
                        src,
                        "Inconsistent number of join keys specified; expected [{}] but found [{}]",
                        expected,
                        found
                    );
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
        return new KeyedFilter(source, new LocalRelation(source, emptyList()), emptyList(), UNSPECIFIED_FIELD, UNSPECIFIED_FIELD, false);
    }

    public KeyedFilter visitJoinTerm(JoinTermContext ctx, List<Attribute> joinKeys, Attribute timestampField, Attribute tiebreakerField) {
        if (ctx.subquery().MISSING_EVENT_OPEN() != null) {
            throw new ParsingException("Missing events are supported only for sequences");
        }
        return keyedFilter(joinKeys, ctx, ctx.by, ctx.subquery(), timestampField, tiebreakerField, false);
    }

    private KeyedFilter keyedFilter(
        List<Attribute> joinKeys,
        ParseTree ctx,
        JoinKeysContext joinCtx,
        SubqueryContext subqueryCtx,
        Attribute timestampField,
        Attribute tiebreakerField,
        boolean missingEvent
    ) {
        List<Attribute> keys = CollectionUtils.combine(joinKeys, visitJoinKeys(joinCtx));
        LogicalPlan eventQuery = visitEventFilter(subqueryCtx.eventFilter());
        return new KeyedFilter(source(ctx), eventQuery, keys, timestampField, tiebreakerField, missingEvent);
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
                    throw new ParsingException(
                        src,
                        "Inconsistent number of join keys specified; expected [{}] but found [{}]",
                        expected,
                        found
                    );
                }
            }
            // check runs
            Token key = sequenceTermCtx.key;
            if (key != null) {
                String k = key.getText();
                if (RUNS.equals(k) == false) {
                    throw new ParsingException(source(key), "Unrecognized option [{}], expecting [{}]", k, RUNS);
                }
            }

            int runs = 1;
            NumberContext numberCtx = sequenceTermCtx.number();
            if (numberCtx instanceof IntegerLiteralContext) {
                Number number = (Number) visitIntegerLiteral((IntegerLiteralContext) numberCtx).fold();
                long value = number.longValue();
                if (value < 1) {
                    throw new ParsingException(source(numberCtx), "A positive runs value is required; found [{}]", value);
                }
                if (value > 100) {
                    throw new ParsingException(source(numberCtx), "A query cannot be repeated more than 100 times; found [{}]", value);
                }
                runs = (int) value;
            }

            int numberOfQueries = queries.size() + runs;
            if (numberOfQueries > 256) {
                throw new ParsingException(
                    source(sequenceTermCtx),
                    "Sequence cannot contain more than 256 queries; found [{}]",
                    numberOfQueries
                );
            }

            for (int i = 0; i < runs; i++) {
                queries.add(sequenceTerm);
            }
        }

        if (queries.size() < 2) {
            throw new ParsingException(source, "A sequence requires a minimum of 2 queries, found [{}]", queries.size());
        }

        // until is already parsed through sequenceTerm() above
        if (ctx.until != null) {
            until = queries.remove(queries.size() - 1);
        } else {
            until = defaultUntil(source);
        }

        if (maxSpan.duration() < 0 && queries.stream().anyMatch(x -> x.isMissingEventFilter())) {
            throw new ParsingException(source, "[maxspan] is required for sequences with missing events queries; found none");
        }

        if (queries.stream().allMatch(KeyedFilter::isMissingEventFilter)) {
            throw new ParsingException(source, "A sequence requires at least one positive event query; found none");
        }

        return new Sequence(source, queries, until, maxSpan, fieldTimestamp(), fieldTiebreaker(), resultPosition());
    }

    private KeyedFilter visitSequenceTerm(SequenceTermContext ctx, List<Attribute> joinKeys) {
        return keyedFilter(
            joinKeys,
            ctx,
            ctx.by,
            ctx.subquery(),
            fieldTimestamp(),
            fieldTiebreaker(),
            ctx.subquery().MISSING_EVENT_OPEN() != null
        );
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
                throw new ParsingException(
                    source(ctx.timeUnit()),
                    "No time unit specified, did you mean [s] as in [{}s]?",
                    text(ctx.timeUnit())
                );
            }

            TimeUnit timeUnit = switch (timeString) {
                case "ms" -> TimeUnit.MILLISECONDS;
                case "s" -> TimeUnit.SECONDS;
                case "m" -> TimeUnit.MINUTES;
                case "h" -> TimeUnit.HOURS;
                case "d" -> TimeUnit.DAYS;
                default -> throw new ParsingException(
                    source(ctx.timeUnit().IDENTIFIER()),
                    "Unrecognized time unit [{}] in [{}], please specify one of [ms, s, m, h, d]",
                    timeString,
                    text(ctx.timeUnit())
                );
            };

            return new TimeValue(value, timeUnit);

        } else {
            throw new ParsingException(
                source(numberCtx),
                "Decimal time interval [{}] not supported; please use an positive integer",
                text(numberCtx)
            );
        }
    }

    @Override
    public Object visitSample(SampleContext ctx) {
        Source source = source(ctx);

        List<Attribute> parentJoinKeys = visitJoinKeys(ctx.by);
        int numberOfKeys = -1;
        List<KeyedFilter> queries = new ArrayList<>(ctx.joinTerm().size());
        boolean hasMissingJoinKeys = false;
        Source missingJoinKeysSource = null;

        for (JoinTermContext joinTermCtx : ctx.joinTerm()) {
            KeyedFilter joinTerm = visitJoinTerm(joinTermCtx, parentJoinKeys, UNSPECIFIED_FIELD, UNSPECIFIED_FIELD);
            int keySize = joinTerm.keys().size();
            if (numberOfKeys < 0) {
                numberOfKeys = keySize;
            } else {
                if (numberOfKeys != keySize) {
                    Source src = source(joinTermCtx.by != null ? joinTermCtx.by : joinTermCtx);
                    int expected = numberOfKeys - parentJoinKeys.size();
                    int found = keySize - parentJoinKeys.size();
                    throw new ParsingException(
                        src,
                        "Inconsistent number of join keys specified; expected [{}] but found [{}]",
                        expected,
                        found
                    );
                }
            }

            if (keySize == 0 && hasMissingJoinKeys == false) {
                hasMissingJoinKeys = true;
                missingJoinKeysSource = source(joinTermCtx);
            }

            queries.add(joinTerm);
            int numberOfQueries = queries.size();
            if (numberOfQueries > MAX_SAMPLE_QUERIES) {
                throw new ParsingException(
                    source(joinTermCtx),
                    "A sample cannot contain more than {} queries, found [{}]",
                    MAX_SAMPLE_QUERIES,
                    numberOfQueries
                );
            }

            Set<String> uniqueKeyNames = new HashSet<>(keySize);
            Set<String> duplicateKeyNames = new LinkedHashSet<>(1);
            for (NamedExpression key : joinTerm.keys()) {
                String name = Expressions.name(key);
                if (uniqueKeyNames.contains(name)) {
                    duplicateKeyNames.add(name);
                } else {
                    uniqueKeyNames.add(name);
                }
            }
            if (duplicateKeyNames.size() > 0) {
                Source src = source(joinTermCtx.by != null ? joinTermCtx.by : joinTermCtx);
                StringJoiner duplicates = new StringJoiner(",");
                for (String duplicate : duplicateKeyNames) {
                    duplicates.add(duplicate);
                }
                throw new ParsingException(src, "Join keys must be used only once, found duplicates: [{}]", duplicates.toString());
            }
        }

        if (queries.size() < 2) {
            throw new ParsingException(source, "A sample requires a minimum of 2 queries, found [{}]", queries.size());
        }

        if (hasMissingJoinKeys) {
            throw new ParsingException(missingJoinKeysSource, "A sample must have at least one join key, found none");
        }

        return new Sample(source, queries);
    }

    private LogicalPlan pipe(PipeContext ctx, LogicalPlan plan) {
        String name = text(ctx.IDENTIFIER());

        if (SUPPORTED_PIPES.contains(name) == false) {
            List<String> potentialMatches = StringUtils.findSimilar(name, SUPPORTED_PIPES);

            String msg = "Unrecognized pipe [{}]";
            if (potentialMatches.isEmpty() == false) {
                String matchString = potentialMatches.toString();
                msg += ", did you mean " + (potentialMatches.size() == 1 ? matchString : "any of " + matchString) + "?";
            }
            throw new ParsingException(source(ctx.IDENTIFIER()), msg, name);
        }

        switch (name) {
            case HEAD_PIPE -> {
                Expression headLimit = pipeIntArgument(source(ctx), name, ctx.booleanExpression());
                return new Head(source(ctx), headLimit, plan);
            }
            case TAIL_PIPE -> {
                Expression tailLimit = pipeIntArgument(source(ctx), name, ctx.booleanExpression());
                // negate the limit
                return new Tail(source(ctx), tailLimit, plan);
            }
            default -> throw new ParsingException(source(ctx), "Pipe [{}] is not supported", name);
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
        boolean foldableInt = expression.foldable() && expression.dataType().isInteger();
        Number value = null;

        if (foldableInt) {
            try {
                value = (Number) expression.fold();
            } catch (ArithmeticException ae) {}
        }

        if (foldableInt == false || value == null || value.intValue() != value.longValue() || value.intValue() < 0) {
            throw new ParsingException(
                expression.source(),
                "Pipe [{}] expects a positive integer but found [{}]",
                pipeName,
                expression.sourceText()
            );
        }

        // 2147483650 - 4 will yield an integer (Integer.MAX_VALUE - 1) but the expression itself (SUB) will be of type LONG
        // and this type will be used when being folded later on
        return new Literal(expression.source(), value.intValue(), INTEGER);
    }
}
