/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialGridFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsGeoGridAggQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

/**
 * Pushes {@code STATS COUNT(*) BY cell = ST_GEOHASH/GEOTILE/GEOHEX(field, prec)} down to Lucene.
 * <p>
 * Matches the physical plan pattern produced for:
 * <pre>
 *   EVAL cell = ST_GEOHASH(location, precision)
 *   | STATS count = COUNT(*) BY cell
 * </pre>
 * which looks like:
 * <pre>
 *   AggregateExec(INITIAL)
 *     └── EvalExec([cell = ST_GEOHASH(FieldAttr(location), precision)])
 *           └── EsQueryExec
 * </pre>
 * When the conditions below are met, the {@link AggregateExec} and {@link EvalExec} are replaced
 * by a single {@link EsGeoGridAggQueryExec} that does the cell-counting inside Lucene:
 * <ul>
 *   <li>Exactly one grouping expression that resolves to a {@link SpatialGridFunction}</li>
 *   <li>The spatial field is a {@code geo_point} {@link FieldAttribute} that is not potentially unmapped</li>
 *   <li>The spatial function has no bounds argument (unbounded grids only)</li>
 *   <li>Exactly one aggregation: a {@link Count} without a filter, applied to either {@code *} or the grouping column</li>
 * </ul>
 */
public class PushGeoGridStatsToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext context) {
        // Only push down the first (partial) aggregation pass on data nodes.
        if (aggregateExec.getMode() != AggregatorMode.INITIAL) {
            return aggregateExec;
        }

        // Require exactly one grouping expression.
        if (aggregateExec.groupings().size() != 1) {
            return aggregateExec;
        }

        // Identify the child structure: EvalExec → EsQueryExec or directly EsQueryExec.
        final EsQueryExec esQueryExec;
        final EvalExec evalExec;
        if (aggregateExec.child() instanceof EvalExec ev && ev.child() instanceof EsQueryExec qe) {
            evalExec = ev;
            esQueryExec = qe;
        } else if (aggregateExec.child() instanceof EsQueryExec qe) {
            evalExec = null;
            esQueryExec = qe;
        } else {
            return aggregateExec;
        }

        // Resolve the grouping to a SpatialGridFunction.
        final Expression groupingExpr = aggregateExec.groupings().get(0);
        final SpatialGridFunction gridFn;
        final Attribute gridAttr;

        if (groupingExpr instanceof Alias alias && alias.child() instanceof SpatialGridFunction gf) {
            // Inline pattern: BY cell = ST_GEOHASH(location, prec)
            gridFn = gf;
            gridAttr = alias.toAttribute();
        } else {
            // Via-EvalExec pattern: EVAL cell = ST_GEOHASH(location, prec) | ... BY cell
            Attribute ref = Expressions.attribute(groupingExpr);
            if (ref == null || evalExec == null) {
                return aggregateExec;
            }
            SpatialGridFunction found = null;
            Attribute foundAttr = null;
            for (Alias alias : evalExec.fields()) {
                if (alias.id().equals(ref.id()) && alias.child() instanceof SpatialGridFunction gf) {
                    found = gf;
                    foundAttr = alias.toAttribute();
                    break;
                }
            }
            if (found == null) {
                return aggregateExec;
            }
            gridFn = found;
            gridAttr = foundAttr;
        }

        // The spatial field must be a geo_point FieldAttribute that is not potentially unmapped.
        // geo_shape requires a different approach (cell intersection, not point-in-cell) and is excluded.
        if (!(gridFn.spatialField() instanceof FieldAttribute geoField)
            || geoField.dataType() != DataType.GEO_POINT
            || geoField.isPotentiallyUnmapped()) {
            return aggregateExec;
        }

        // Only push down when the field has doc values indexed. The Lucene operator reads
        // directly from SortedNumericDocValues; without doc values there is nothing to read.
        if (context.searchStats().hasDocValues(geoField.fieldName()) == false) {
            return aggregateExec;
        }

        // The precision parameter must be foldable (a constant integer).
        if (gridFn.parameter().foldable() == false) {
            return aggregateExec;
        }
        final int precision = ((Number) gridFn.parameter().fold(context.foldCtx())).intValue();

        // Bounded grids are not supported in the pushdown; the bounding-box filter would need to be
        // applied inside the operator, which is left as a future enhancement.
        if (gridFn.bounds() != null) {
            return aggregateExec;
        }

        // Validate aggregates: allow exactly one COUNT (without a filter) over * or the grouping column,
        // plus the grouping alias that appears in the aggregates list.
        boolean foundCount = false;
        for (NamedExpression ne : aggregateExec.aggregates()) {
            // The grouping column may appear in the aggregates list as a plain Attribute OR as an
            // Alias that wraps a reference attribute. Identify it by checking the attribute ID.
            Attribute neAttr = Expressions.attribute(ne);
            if (neAttr != null && neAttr.id().equals(gridAttr.id())) {
                continue; // skip the grouping column
            }
            if (ne instanceof Alias alias && alias.child() instanceof Count count && count.hasFilter() == false) {
                // Accept COUNT(*) (foldable field) or COUNT(cell) where cell is the grouping attribute.
                Expression countField = count.field();
                if (countField.foldable()) {
                    foundCount = true;
                    continue;
                }
                Attribute countFieldAttr = Expressions.attribute(countField);
                if (countFieldAttr != null && countFieldAttr.id().equals(gridAttr.id())) {
                    foundCount = true;
                    continue;
                }
            }
            // Any other aggregate means we cannot push down.
            return aggregateExec;
        }
        if (foundCount == false) {
            return aggregateExec;
        }

        return new EsGeoGridAggQueryExec(
            aggregateExec.source(),
            esQueryExec.indexPattern(),
            esQueryExec.query(),
            geoField.fieldName().string(),
            precision,
            gridFn.dataType(),
            aggregateExec.intermediateAttributes()
        );
    }
}
