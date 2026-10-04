/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PackDimsExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.ReadDimsExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.AggregateMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A rule that moves {@code VALUES(dimension-field)} aggregations in time-series aggregations
 * to execute after the aggregation, reading the dimension fields once each group.
 * This is possible because dimension field values for {@code _tsid} are identical across all
 * documents in the same time-series.
 * For example:
 * {@code TS .. | STATS sum(rate(r1)), sum(rate(r2)) BY cluster, host, tbucket(1m)}
 * without this rule
 * {@code TS ..
 * | EXTRACT_FIELDS(r1,r2,cluster, host)
 * | STATS rate(r1), rate(r2), VALUES(cluster), VALUES(host) BY _tsid, tbucket(1m)}
 * with this rule
 * {@code TS ..
 * | EXTRACT_FIELDS(r1,r2)
 * | STATS rate(r1), rate(r2), FIRST_DOC_ID(_doc) BY _tsid, tbucket(1m)
 * | READ_DIMS(cluster, host)
 * | ...}
 */
public final class ExtractDimensionFieldsAfterAggregation extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    PhysicalPlan,
    LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan rule(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        if (plan instanceof TimeSeriesAggregateExec oldAgg && oldAgg.getMode() == AggregatorMode.INITIAL) {
            return rule(oldAgg, context);
        }
        return plan;
    }

    PhysicalPlan rule(TimeSeriesAggregateExec oldAgg, LocalPhysicalOptimizerContext context) {
        AttributeSet inputAttributes = oldAgg.inputSet();
        var sourceAttr = inputAttributes.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (sourceAttr == null) {
            return oldAgg;
        }
        Attribute tsidAttr = tsidGroupingAttribute(oldAgg);
        if (tsidAttr == null) {
            return oldAgg;
        }
        List<NamedExpression> newAggregates = new ArrayList<>();
        List<Attribute> readDims = new ArrayList<>();
        List<Attribute> packDims = new ArrayList<>();
        List<Alias> aliases = new ArrayList<>();
        Attribute packedAttr = null;
        Set<AggregateFunction> seen = new HashSet<>();
        List<Attribute> oldIntermediates = oldAgg.intermediateAttributes();
        List<Attribute> newIntermediates = new ArrayList<>(oldIntermediates.subList(0, oldAgg.groupings().size()));
        Map<NameId, TimeSeriesUnset> unsetsBelow = unsetsBelow(oldAgg);
        Map<NameId, Attribute> readByDimension = new HashMap<>();
        List<MovedUnset> movedUnsets = new ArrayList<>();
        int intermediateOffset = oldAgg.groupings().size();
        for (var agg : oldAgg.aggregates()) {
            boolean skipAgg = false;
            if (Alias.unwrap(agg) instanceof AggregateFunction af) {
                if (af instanceof PackDimsAgg packDimsAgg) {
                    skipAgg = true;
                    if (seen.add(af)) {
                        int size = intermediateStateSize(af);
                        if (size != 1) {
                            throw new IllegalStateException("expected one intermediate attribute for [" + af + "] but got [" + size + "]");
                        }
                        packedAttr = oldIntermediates.get(intermediateOffset);
                        for (Expression dim : packDimsAgg.fields()) {
                            Attribute attr = readDimAttribute((Attribute) dim);
                            readByDimension.putIfAbsent(((Attribute) dim).id(), attr);
                            readDims.add(attr);
                            packDims.add(attr);
                        }
                        intermediateOffset += size;
                    }
                } else {
                    Attribute dimensionField = valuesOfDimensionField(af, inputAttributes);
                    TimeSeriesUnset unset = dimensionField == null ? unsetOfDimensionField(af, unsetsBelow, oldAgg) : null;
                    skipAgg = (dimensionField != null || unset != null);
                    if (seen.add(af)) {
                        int size = intermediateStateSize(af);
                        if (dimensionField != null) {
                            if (size != 1) {
                                throw new IllegalStateException(
                                    "expected one intermediate attribute for [" + af + "] but got [" + size + "]"
                                );
                            }
                            Attribute oldAttr = oldIntermediates.get(intermediateOffset);
                            NameId dimension = dimensionField.id();
                            dimensionField = readDimAttribute(dimensionField);
                            readByDimension.putIfAbsent(dimension, dimensionField);
                            aliases.add(new Alias(agg.source(), agg.name(), dimensionField, oldAttr.id()));
                            readDims.add(dimensionField);
                        } else if (unset != null) {
                            if (size != 1) {
                                throw new IllegalStateException(
                                    "expected one intermediate attribute for [" + af + "] but got [" + size + "]"
                                );
                            }
                            Attribute ref = (Attribute) ((Values) af).field();
                            movedUnsets.add(new MovedUnset(agg, unset, oldIntermediates.get(intermediateOffset), ref.id()));
                        } else {
                            for (int i = 0; i < size; i++) {
                                newIntermediates.add(oldIntermediates.get(intermediateOffset + i));
                            }
                        }
                        intermediateOffset += size;
                    }
                }
            }
            if (skipAgg == false) {
                newAggregates.add(agg);
            }
        }
        // A time-series metadata operation over a dimension is identical across the series too: it applies to the dimension
        // read once per group, sharing the read with the packed dimensions, rather than to every document below.
        Set<NameId> movedRefs = new HashSet<>();
        for (MovedUnset moved : movedUnsets) {
            Attribute timeseries = (Attribute) moved.unset().timeseries();
            Attribute read = readByDimension.get(timeseries.id());
            if (read == null) {
                read = readDimAttribute(timeseries);
                readByDimension.put(timeseries.id(), read);
                readDims.add(read);
            }
            List<Expression> children = new ArrayList<>(moved.unset().children());
            children.set(0, read);
            aliases.add(
                new Alias(moved.agg().source(), moved.agg().name(), moved.unset().replaceChildren(children), moved.intermediate().id())
            );
            movedRefs.add(moved.ref());
        }
        if (aliases.isEmpty() && packedAttr == null) {
            return oldAgg;
        }
        PhysicalPlan child = movedRefs.isEmpty() ? oldAgg.child() : withoutFields(oldAgg.child(), movedRefs);
        Attribute docAttr = new ReferenceAttribute(oldAgg.source(), sourceAttr.qualifier(), sourceAttr.name(), sourceAttr.dataType());
        newIntermediates.add(docAttr);
        newAggregates.add(new Alias(oldAgg.source(), sourceAttr.name(), new FirstDocId(oldAgg.source(), sourceAttr)));
        PhysicalPlan plan = new TimeSeriesAggregateExec(
            oldAgg.source(),
            child,
            oldAgg.groupings(),
            newAggregates,
            oldAgg.getMode(),
            newIntermediates,
            oldAgg.estimatedRowSize(),
            oldAgg.timeBucket()
        );
        if (readDims.isEmpty() == false) {
            plan = new ReadDimsExec(
                oldAgg.source(),
                plan,
                docAttr,
                tsidAttr,
                readDims,
                context.configuration().pragmas().fieldExtractPreference()
            );
        }
        if (packedAttr != null) {
            plan = new PackDimsExec(oldAgg.source(), plan, packDims, packedAttr);
        }
        if (aliases.isEmpty() == false) {
            plan = new EvalExec(oldAgg.source(), plan, aliases);
        }
        return new ProjectExec(oldAgg.source(), plan, oldIntermediates);
    }

    private static Attribute tsidGroupingAttribute(TimeSeriesAggregateExec agg) {
        for (Expression grouping : agg.groupings()) {
            Attribute attr = Expressions.attribute(grouping);
            if (attr != null && attr.dataType() == DataType.TSID_DATA_TYPE && MetadataAttribute.TSID_FIELD.equals(attr.name())) {
                return attr;
            }
        }
        return null;
    }

    private static Attribute valuesOfDimensionField(AggregateFunction af, AttributeSet inputAttributes) {
        if (af instanceof DimensionValues values && values.hasFilter() == false && values.field() instanceof Attribute attr) {
            if (inputAttributes.contains(attr) == false || attr instanceof TimeSeriesMetadataAttribute) {
                return attr;
            }
        }
        return null;
    }

    /** A {@code VALUES} of a {@link TimeSeriesUnset} the rule applies after the aggregation instead. */
    private record MovedUnset(NamedExpression agg, TimeSeriesUnset unset, Attribute intermediate, NameId ref) {}

    /** The {@link TimeSeriesUnset}s of a {@code _timeseries} the evals below the aggregate compute for it, by output id. */
    private static Map<NameId, TimeSeriesUnset> unsetsBelow(TimeSeriesAggregateExec agg) {
        Map<NameId, TimeSeriesUnset> unsets = new HashMap<>();
        PhysicalPlan plan = agg.child();
        while (plan instanceof UnaryExec unary) {
            if (plan instanceof EvalExec eval) {
                for (Alias field : eval.fields()) {
                    if (field.child() instanceof TimeSeriesUnset unset && unset.timeseries() instanceof TimeSeriesMetadataAttribute) {
                        unsets.put(field.id(), unset);
                    }
                }
            }
            plan = unary.child();
        }
        return unsets;
    }

    /**
     * The {@link TimeSeriesUnset} a {@code VALUES} aggregates, when an eval below computes it and nothing else, in the aggregate
     * or below it, reads it, so it can move after the aggregation.
     */
    private static TimeSeriesUnset unsetOfDimensionField(
        AggregateFunction af,
        Map<NameId, TimeSeriesUnset> unsetsBelow,
        TimeSeriesAggregateExec agg
    ) {
        if (af instanceof Values values && values.hasFilter() == false && values.field() instanceof Attribute ref) {
            TimeSeriesUnset unset = unsetsBelow.get(ref.id());
            if (unset == null) {
                return null;
            }
            long readers = Stream.concat(agg.groupings().stream(), agg.aggregates().stream())
                .filter(expression -> references(expression, ref))
                .count();
            // per expression, as a plan's references leave out those it defines itself, such as a later field of the same eval
            boolean readBelow = agg.child()
                .anyMatch(plan -> plan.expressions().stream().anyMatch(expression -> references(expression, ref)));
            return readers == 1 && readBelow == false ? unset : null;
        }
        return null;
    }

    private static boolean references(Expression expression, Attribute attribute) {
        return expression.references().stream().anyMatch(reference -> reference.id().equals(attribute.id()));
    }

    /** The plan with the eval fields of {@code ids} removed, and an eval left with none removed altogether. */
    private static PhysicalPlan withoutFields(PhysicalPlan plan, Set<NameId> ids) {
        return plan.transformDown(EvalExec.class, eval -> {
            List<Alias> fields = eval.fields().stream().filter(field -> ids.contains(field.id()) == false).toList();
            if (fields.size() == eval.fields().size()) {
                return eval;
            }
            return fields.isEmpty() ? eval.child() : new EvalExec(eval.source(), eval.child(), fields);
        });
    }

    private static int intermediateStateSize(AggregateFunction af) {
        return AggregateMapper.intermediateStateDesc(af, true).size();
    }

    static Attribute readDimAttribute(Attribute dim) {
        if (dim instanceof TimeSeriesMetadataAttribute timeSeriesMetadataAttribute) {
            var withoutFields = timeSeriesMetadataAttribute.excludedFields();
            return new TimeSeriesMetadataAttribute(
                dim.source(),
                null,
                dim.qualifier(),
                dim.name(),
                new FunctionEsField(
                    new EsField(SourceFieldMapper.NAME, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION),
                    DataType.KEYWORD,
                    new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, withoutFields)
                ),
                dim.nullable(),
                null,
                true,
                withoutFields
            );
        }
        return dim;
    }
}
