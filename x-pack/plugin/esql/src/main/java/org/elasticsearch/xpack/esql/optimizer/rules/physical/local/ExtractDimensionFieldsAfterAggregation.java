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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.AggregateMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A rule that moves `VALUES(dimension-field)` aggregations in time-series aggregations
 * to execute after the aggregation, reading the dimension fields once each group.
 * This is possible because dimension field values for `_tsid` are identical across all
 * documents in the same time-series.
 * For example:
 * `TS .. | STATS sum(rate(r1)), sum(rate(r2)) BY cluster, host, tbucket(1m)`
 * without this rule
 * `TS ..
 * | EXTRACT_FIELDS(r1,r2,cluster, host)
 * | STATS rate(r1), rate(r2), VALUES(cluster), VALUES(host) BY _tsid, tbucket(1m)`
 * with this rule
 * `TS ..
 * | EXTRACT_FIELDS(r1,r2)
 * | STATS rate(r1), rate(r2), FIRST_DOC_ID(_doc) BY _tsid, tbucket(1m)
 * | EXTRACT_FIELDS(cluster, host)
 * | ...
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

    private PhysicalPlan rule(TimeSeriesAggregateExec oldAgg, LocalPhysicalOptimizerContext context) {
        AttributeSet inputAttributes = oldAgg.inputSet();
        var sourceAttr = inputAttributes.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (sourceAttr == null) {
            return oldAgg;
        }
        List<NamedExpression> newAggregates = new ArrayList<>();
        List<Attribute> dimensionFields = new ArrayList<>();
        List<Alias> aliases = new ArrayList<>();
        Set<AggregateFunction> seen = new HashSet<>();
        List<Attribute> oldIntermediates = oldAgg.intermediateAttributes();
        List<Attribute> newIntermediates = new ArrayList<>(oldIntermediates.subList(0, oldAgg.groupings().size()));
        int intermediateOffset = oldAgg.groupings().size();
        for (var agg : oldAgg.aggregates()) {
            Attribute dimensionField = null;
            if (Alias.unwrap(agg) instanceof AggregateFunction af) {
                dimensionField = valuesOfDimensionField(af, inputAttributes);
                if (seen.add(af)) {
                    int size = intermediateStateSize(af);
                    if (dimensionField != null) {
                        if (size != 1) {
                            throw new IllegalStateException("expected one intermediate attribute for [" + af + "] but got [" + size + "]");
                        }
                        Attribute oldAttr = oldIntermediates.get(intermediateOffset);
                        if (dimensionField.name().equals(MetadataAttribute.TIMESERIES)) {
                            var sourceField = new FieldAttribute(
                                dimensionField.source(),
                                null,
                                dimensionField.qualifier(),
                                dimensionField.name(),
                                new FunctionEsField(
                                    new EsField(
                                        SourceFieldMapper.NAME,
                                        DataType.KEYWORD,
                                        Map.of(),
                                        false,
                                        EsField.TimeSeriesFieldType.DIMENSION
                                    ),
                                    DataType.KEYWORD,
                                    new BlockLoaderFunctionConfig.JustFunction(BlockLoaderFunctionConfig.Function.TIME_SERIES_DIMENSIONS)
                                ),
                                true
                            );
                            aliases.add(new Alias(agg.source(), agg.name(), sourceField, oldAttr.id()));
                        } else {
                            aliases.add(new Alias(agg.source(), agg.name(), dimensionField, oldAttr.id()));
                            dimensionFields.add(dimensionField);
                        }
                    } else {
                        for (int i = 0; i < size; i++) {
                            newIntermediates.add(oldIntermediates.get(intermediateOffset + i));
                        }
                    }
                    intermediateOffset += size;
                }
            }
            if (dimensionField == null) {
                newAggregates.add(agg);
            }
        }
        if (aliases.isEmpty()) {
            return oldAgg;
        }
        newIntermediates.add(new ReferenceAttribute(oldAgg.source(), sourceAttr.qualifier(), sourceAttr.name(), sourceAttr.dataType()));
        newAggregates.add(new Alias(oldAgg.source(), sourceAttr.name(), new FirstDocId(oldAgg.source(), sourceAttr)));
        TimeSeriesAggregateExec newStats = new TimeSeriesAggregateExec(
            oldAgg.source(),
            oldAgg.child(),
            oldAgg.groupings(),
            newAggregates,
            oldAgg.getMode(),
            newIntermediates,
            oldAgg.estimatedRowSize(),
            oldAgg.timeBucket()
        );
        final EvalExec evalExec;
        if (dimensionFields.isEmpty()) {
            evalExec = new EvalExec(oldAgg.source(), newStats, aliases);
        } else {
            PhysicalPlan fieldExtractExec = new FieldExtractExec(
                oldAgg.source(),
                newStats,
                dimensionFields,
                context.configuration().pragmas().fieldExtractPreference()
            );
            evalExec = new EvalExec(oldAgg.source(), fieldExtractExec, aliases);
        }
        return new ProjectExec(oldAgg.source(), evalExec, oldIntermediates);
    }

    private static Attribute valuesOfDimensionField(AggregateFunction af, AttributeSet inputAttributes) {
        if (af instanceof DimensionValues values && values.hasFilter() == false && values.field() instanceof Attribute attr) {
            if (inputAttributes.contains(attr) == false || attr.name().equals(MetadataAttribute.TIMESERIES)) {
                return attr;
            }
        }
        return null;
    }

    private static int intermediateStateSize(AggregateFunction af) {
        return AggregateMapper.intermediateStateDesc(af, true).size();
    }
}
