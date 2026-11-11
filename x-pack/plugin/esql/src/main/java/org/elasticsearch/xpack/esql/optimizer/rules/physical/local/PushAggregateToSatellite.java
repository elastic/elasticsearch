/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.AggregateMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pushes an aggregation to a satellite of protocol version 1 (see {@code satellite-spec-v1.md}). It runs after
 * {@link PushFragmentToSatellite}, which has already put the filter, eval and project below the aggregate into the fragment of the
 * source: an {@link AggregateExec} directly over such a source becomes the same source whose fragment ends in an {@code aggregate}
 * node, so the satellite returns per-group values and no row.
 * <p>
 * The satellite's values are plain (a count, a sum, a minimum). In mode INITIAL the aggregation's output is the partial state that
 * the final aggregation, and the partial states of index shards, merge; the protocol does not know it. An {@link EvalExec} and a
 * {@link ProjectExec} over the source build it channel by channel: {@code seen} is "the value is not null", every other channel is
 * the value, or a typed zero when there is none (the aggregators ignore the channel when {@code seen} is false). In mode SINGLE the
 * output is the final value and the adapter only renames.
 * <p>
 * What is not pushed stays as it is: other modes, a source whose fragment is not a plain scan, filter, project or eval chain, a
 * function or a type outside protocol section 8.4, and {@code SUM} unless the table's record count proves that no accumulator can
 * overflow (section 8.7).
 */
public class PushAggregateToSatellite extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    private static final Set<String> BELOW_AGGREGATE = Set.of("scan", "filter", "project", "eval");
    private static final Set<DataType> COUNT_TYPES = Set.of(
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.KEYWORD,
        DataType.DATETIME,
        DataType.DATE_NANOS
    );
    private static final Set<DataType> MIN_MAX_TYPES = Set.of(
        DataType.INTEGER,
        DataType.LONG,
        DataType.BOOLEAN,
        DataType.KEYWORD,
        DataType.DATETIME,
        DataType.DATE_NANOS
    );
    private static final Set<DataType> KEY_TYPES = MIN_MAX_TYPES;
    /** {@code sum(integer)} cannot overflow a signed 64-bit accumulator below this many rows (protocol section 8.7). */
    private static final long SUM_SAFE_ROWS = 1L << 32;

    @Override
    protected PhysicalPlan rule(AggregateExec agg, LocalPhysicalOptimizerContext ctx) {
        if (agg.getMode() != AggregatorMode.INITIAL && agg.getMode() != AggregatorMode.SINGLE) {
            return agg;
        }
        if (agg.child() instanceof ExternalSourceExec ext && ext.pushedFilter() instanceof SatelliteFragment fragment) {
            return push(agg, ext, fragment);
        }
        return agg;
    }

    private static PhysicalPlan push(AggregateExec agg, ExternalSourceExec ext, SatelliteFragment fragment) {
        PushFragmentToSatellite.Caps caps = PushFragmentToSatellite.caps(ext);
        if (caps == null || caps.version() < 1 || caps.plan().contains("aggregate") == false || belowAggregate(fragment) == false) {
            return agg;
        }
        PushFragmentToSatellite.Translator tr = new PushFragmentToSatellite.Translator(caps);

        List<Map<String, Object>> keys = new ArrayList<>();
        List<Attribute> output = new ArrayList<>();
        for (Expression grouping : agg.groupings()) {
            if (grouping instanceof Attribute k) {
                if (sourceColumn(ext, k) == null) {
                    return agg;
                }
                if (KEY_TYPES.contains(k.dataType()) == false
                    || tr.declaredType(k.dataType()) == null
                    || caps.listColumns().contains(k.name())) {
                    return agg;
                }
                Map<String, Object> node = new LinkedHashMap<>();
                node.put("name", k.name());
                node.put("from", k.name());
                keys.add(node);
                output.add(k);
            } else {
                return agg;
            }
        }

        List<Map<String, Object>> functions = new ArrayList<>();
        List<AggregateFunction> pushed = new ArrayList<>();
        List<Attribute> values = new ArrayList<>();
        for (NamedExpression ne : agg.aggregates()) {
            if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                String name = "$$agg$" + functions.size();
                Map<String, Object> node = function(af, name, ext, caps, tr);
                if (node == null) {
                    return agg;
                }
                functions.add(node);
                pushed.add(af);
                Attribute value = new ReferenceAttribute(Source.EMPTY, null, name, resultType(af), Nullability.TRUE, null, true);
                values.add(value);
                output.add(value);
            } else if (ne instanceof Attribute == false) {
                return agg;
            }
        }
        if (functions.isEmpty() && keys.isEmpty()) {
            return agg;
        }

        List<Map<String, Object>> outputColumns = tr.columns(output);
        if (outputColumns == null) {
            return agg;
        }
        Map<String, Object> aggregate = PushFragmentToSatellite.Translator.node(
            "aggregate",
            "keys",
            keys,
            "aggregates",
            functions,
            "input",
            fragment.request().get("plan")
        );
        Map<String, Object> request = new LinkedHashMap<>(fragment.request());
        request.put("output", outputColumns);
        request.put("plan", aggregate);
        if (PushFragmentToSatellite.fits(request, caps.maxFragmentBytes()) == false) {
            return agg;
        }

        PhysicalPlan source = ext.withAttributes(output).withPushedFilter(new SatelliteFragment(request));
        return agg.getMode() == AggregatorMode.INITIAL ? partialState(agg, source, pushed, values) : finalValues(agg, source, values);
    }

    /** The aggregate's function as an IR node, or null when it is outside what version 1 pushes. */
    private static Map<String, Object> function(
        AggregateFunction af,
        String name,
        ExternalSourceExec ext,
        PushFragmentToSatellite.Caps caps,
        PushFragmentToSatellite.Translator tr
    ) {
        if (af.hasFilter() || af.hasWindow()) {
            return null;
        }
        String fn;
        Set<DataType> accepted;
        if (af instanceof Count) {
            fn = "count";
            accepted = COUNT_TYPES;
        } else if (af instanceof Sum) {
            if (caps.totalRecords() <= 0 || caps.totalRecords() >= SUM_SAFE_ROWS) {
                return null;
            }
            fn = "sum";
            accepted = Set.of(DataType.INTEGER);
        } else if (af instanceof Min) {
            fn = "min";
            accepted = MIN_MAX_TYPES;
        } else if (af instanceof Max) {
            fn = "max";
            accepted = MIN_MAX_TYPES;
        } else {
            return null;
        }
        if (caps.aggregates().contains(fn) == false) {
            return null;
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("name", name);
        node.put("fn", fn);
        if (af.fields().size() != 1) {
            return null;
        }
        Expression field = af.fields().get(0);
        if (af instanceof Count && field instanceof Literal literal) {
            // only COUNT(*): COUNT(1) and the like are not pushed (protocol section 8.7)
            if (literal.dataType() != DataType.KEYWORD
                || StringUtils.WILDCARD.equals(literal.value() instanceof BytesRef b ? b.utf8ToString() : null) == false) {
                return null;
            }
            return node;
        }
        if (field instanceof Attribute column) {
            if (sourceColumn(ext, column) == null
                || accepted.contains(column.dataType()) == false
                || tr.declaredType(column.dataType()) == null
                || caps.listColumns().contains(column.name())) {
                return null;
            }
            node.put("from", column.name());
            return node;
        }
        return null;
    }

    private static DataType resultType(AggregateFunction af) {
        return af instanceof Count || af instanceof Sum ? DataType.LONG : af.fields().get(0).dataType();
    }

    private static boolean belowAggregate(SatelliteFragment fragment) {
        return fragment.request().get("plan") instanceof Map<?, ?> plan && BELOW_AGGREGATE.contains(plan.get("op"));
    }

    private static Attribute sourceColumn(ExternalSourceExec ext, Attribute column) {
        for (Attribute a : ext.output()) {
            if (a.id().equals(column.id())) {
                return a;
            }
        }
        return null;
    }

    /** Mode SINGLE: the output of the aggregate is its final values, which the satellite's columns already are. */
    private static PhysicalPlan finalValues(AggregateExec agg, PhysicalPlan source, List<Attribute> values) {
        List<Alias> aliases = new ArrayList<>();
        int next = 0;
        for (NamedExpression ne : agg.aggregates()) {
            if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction) {
                aliases.add(new Alias(alias.source(), alias.name(), values.get(next++), alias.id()));
            }
        }
        return new ProjectExec(agg.source(), new EvalExec(agg.source(), source, aliases), agg.output());
    }

    /**
     * Mode INITIAL: the aggregate's output is the keys, then for each function the channels of its intermediate state. They are
     * derived from the satellite's plain value as the aggregators read them; a channel this does not know is not adapted.
     */
    private static PhysicalPlan partialState(
        AggregateExec agg,
        PhysicalPlan source,
        List<AggregateFunction> pushed,
        List<Attribute> values
    ) {
        List<Attribute> intermediate = agg.intermediateAttributes();
        List<Alias> aliases = new ArrayList<>();
        int position = agg.groupings().size();
        for (int f = 0; f < pushed.size(); f++) {
            List<IntermediateStateDesc> channels = AggregateMapper.intermediateStateDesc(pushed.get(f), agg.groupings().isEmpty() == false);
            Attribute value = values.get(f);
            for (IntermediateStateDesc channel : channels) {
                Attribute target = intermediate.get(position++);
                Expression derived = derive(channel.name(), target, value);
                if (derived == null) {
                    return agg;
                }
                aliases.add(new Alias(target.source(), target.name(), derived, target.id()));
            }
        }
        if (position != intermediate.size()) {
            return agg;
        }
        return new ProjectExec(agg.source(), new EvalExec(agg.source(), source, aliases), agg.output());
    }

    /** One channel of the partial state from the satellite's plain value, or null when the channel is not one this knows. */
    private static Expression derive(String channel, Attribute target, Attribute value) {
        if (channel.equals("seen")) {
            return new IsNotNull(target.source(), value);
        }
        if (channel.equals("failed")) {
            return Literal.FALSE;
        }
        if (Set.of("count", "sum", "min", "max").contains(channel)) {
            // the aggregators keep a datetime or a date_nanos in a long channel: the same number, in the type of the channel
            boolean asLong = target.dataType() == DataType.LONG
                && (value.dataType() == DataType.DATETIME || value.dataType() == DataType.DATE_NANOS);
            Expression plain = asLong ? new ToLong(target.source(), value) : value;
            Literal zero = zero(target.source(), target.dataType());
            return zero == null || plain.dataType() != target.dataType() ? null : new Coalesce(target.source(), plain, List.of(zero));
        }
        return null;
    }

    /** The value of a channel when the group has none: ignored by the aggregator, but a block cannot hold a null there. */
    private static Literal zero(Source source, DataType type) {
        if (type == DataType.LONG || type == DataType.DATETIME || type == DataType.DATE_NANOS) {
            return new Literal(source, 0L, type);
        }
        if (type == DataType.INTEGER) {
            return new Literal(source, 0, type);
        }
        if (type == DataType.BOOLEAN) {
            return new Literal(source, false, type);
        }
        return type == DataType.KEYWORD ? new Literal(source, new BytesRef(""), type) : null;
    }
}
