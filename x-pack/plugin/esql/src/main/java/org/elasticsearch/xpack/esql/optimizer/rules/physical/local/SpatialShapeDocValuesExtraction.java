/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtent;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules.ParameterizedOptimizerRule;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This rule is responsible for marking spatial shape fields whose centroid or extent can be extracted from doc-values.
 * This optimization applies to {@code ST_CENTROID_AGG} and {@code ST_EXTENT_AGG} aggregations on shape types.
 * <p>
 * Normally spatial fields are extracted from source values because this maintains original precision, but is very slow.
 * The centroid data (location and weight) and extent bounds are stored in the header of the shape doc-values,
 * allowing for fast extraction without reading the full geometry.
 * <p>
 * We only apply this optimization under specific conditions:
 * <ul>
 *     <li>The spatial data is of type GEO_SHAPE or CARTESIAN_SHAPE.</li>
 *     <li>The spatial data is consumed directly by {@code ST_CENTROID_AGG} or {@code ST_EXTENT_AGG}.</li>
 *     <li>The spatial data is not consumed by any other operation (except the other spatial aggregation).</li>
 *     <li>The field has doc-values enabled.</li>
 * </ul>
 * <p>
 * When both {@code ST_CENTROID_AGG} and {@code ST_EXTENT_AGG} operate on the same field, this rule ensures
 * both aggregations use the combined extraction preference ({@code EXTRACT_SPATIAL_BOUNDS_AND_CENTROID}).
 */
public class SpatialShapeDocValuesExtraction extends ParameterizedOptimizerRule<AggregateExec, LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(AggregateExec aggregate, LocalPhysicalOptimizerContext ctx) {
        Set<Attribute> centroidAttributes = new HashSet<>();
        Set<Attribute> boundsAttributes = new HashSet<>();
        findSpatialShapeAttributes(aggregate, ctx, centroidAttributes, boundsAttributes);

        if (centroidAttributes.isEmpty() && boundsAttributes.isEmpty()) {
            return aggregate;
        }

        return aggregate.transformDown(PhysicalPlan.class, exec -> switch (exec) {
            case AggregateExec agg -> transformAggregateExec(agg, centroidAttributes, boundsAttributes);
            case FieldExtractExec fieldExtractExec -> transformFieldExtractExec(fieldExtractExec, centroidAttributes, boundsAttributes);
            default -> exec;
        });
    }

    private static void findSpatialShapeAttributes(
        AggregateExec aggregate,
        LocalPhysicalOptimizerContext ctx,
        Set<Attribute> centroidAttributes,
        Set<Attribute> boundsAttributes
    ) {
        aggregate.transformDown(UnaryExec.class, exec -> {
            switch (exec) {
                case AggregateExec agg -> {
                    List<AggregateFunction> aggregateFunctions = agg.aggregates()
                        .stream()
                        .flatMap(e -> extractAggregateFunction(e).stream())
                        .toList();

                    List<SpatialCentroid> spatialCentroids = aggregateFunctions.stream()
                        .filter(SpatialCentroid.class::isInstance)
                        .map(SpatialCentroid.class::cast)
                        .toList();

                    List<SpatialExtent> spatialExtents = aggregateFunctions.stream()
                        .filter(SpatialExtent.class::isInstance)
                        .map(SpatialExtent.class::cast)
                        .toList();

                    // Fields appearing in non-spatial aggregations should not be optimized
                    Set<EsField> fieldsInOtherAggregations = aggregateFunctions.stream()
                        .filter(a -> a instanceof SpatialCentroid == false && a instanceof SpatialExtent == false)
                        .flatMap(af -> af.references().stream())
                        .filter(FieldAttribute.class::isInstance)
                        .map(f -> ((FieldAttribute) f).field())
                        .collect(Collectors.toSet());

                    // Find centroid-eligible attributes
                    spatialCentroids.stream()
                        .map(SpatialCentroid::field)
                        .filter(FieldAttribute.class::isInstance)
                        .map(FieldAttribute.class::cast)
                        .filter(f -> isEligibleShapeField(f, fieldsInOtherAggregations, ctx))
                        .forEach(centroidAttributes::add);

                    // Find bounds-eligible attributes
                    spatialExtents.stream()
                        .map(SpatialExtent::field)
                        .filter(FieldAttribute.class::isInstance)
                        .map(FieldAttribute.class::cast)
                        .filter(f -> isEligibleShapeField(f, fieldsInOtherAggregations, ctx))
                        .forEach(boundsAttributes::add);
                }
                case EvalExec evalExec -> {
                    centroidAttributes.removeAll(evalExec.references());
                    boundsAttributes.removeAll(evalExec.references());
                }
                case FilterExec filterExec -> {
                    var refsToRemove = referencesExcludingIsNotNull(filterExec.condition());
                    centroidAttributes.removeAll(refsToRemove);
                    boundsAttributes.removeAll(refsToRemove);
                }
                default -> { // Do nothing
                }
            }
            return exec;
        });
    }

    private static boolean isEligibleShapeField(
        FieldAttribute field,
        Set<EsField> fieldsInOtherAggregations,
        LocalPhysicalOptimizerContext ctx
    ) {
        return isShape(field.field().getDataType())
            && fieldsInOtherAggregations.contains(field.field()) == false
            && ctx.searchStats().hasDocValues(field.fieldName());
    }

    private static PhysicalPlan transformFieldExtractExec(
        FieldExtractExec fieldExtractExec,
        Set<Attribute> centroidAttributes,
        Set<Attribute> boundsAttributes
    ) {
        var centroidAttrs = new HashSet<>(centroidAttributes);
        centroidAttrs.retainAll(fieldExtractExec.attributesToExtract());

        var boundsAttrs = new HashSet<>(boundsAttributes);
        boundsAttrs.retainAll(fieldExtractExec.attributesToExtract());

        if (centroidAttrs.isEmpty() && boundsAttrs.isEmpty()) {
            return fieldExtractExec;
        }

        return fieldExtractExec.withCentroidAttributes(centroidAttrs).withBoundsAttributes(boundsAttrs);
    }

    private static PhysicalPlan transformAggregateExec(
        AggregateExec agg,
        Set<Attribute> centroidAttributes,
        Set<Attribute> boundsAttributes
    ) {
        if (centroidAttributes.isEmpty() == false || boundsAttributes.isEmpty() == false) {
            return agg.transformExpressionsDown(
                SpatialAggregateFunction.class,
                a -> transformFieldExtractPreference(a, centroidAttributes, boundsAttributes)
            );
        }
        return agg;
    }

    private static SpatialAggregateFunction transformFieldExtractPreference(
        SpatialAggregateFunction agg,
        Set<Attribute> centroidAttributes,
        Set<Attribute> boundsAttributes
    ) {
        boolean withCentroid = centroidAttributes.contains(agg.field());
        boolean withBounds = boundsAttributes.contains(agg.field());
        MappedFieldType.FieldExtractPreference p = agg.fieldExtractPreference();
        return withCentroid
            ? (withBounds
                ? agg.withFieldExtractPreference(p.getWithSpatialCentroid().getWithSpatialBounds())
                : agg.withFieldExtractPreference(p.getWithSpatialCentroid()))
            : (withBounds ? agg.withFieldExtractPreference(p.getWithSpatialBounds()) : agg);
    }

    /**
     * Collect references from an expression tree, but skip {@link IsNotNull} sub-expressions.
     * {@link IsNotNull} checks on spatial fields don't require full-precision data and can be
     * answered from doc values, so they shouldn't prevent doc-values extraction.
     */
    private static Set<Attribute> referencesExcludingIsNotNull(Expression expr) {
        var refs = new HashSet<Attribute>();
        collectRefsExcludingIsNotNull(expr, refs);
        return refs;
    }

    private static void collectRefsExcludingIsNotNull(Expression expr, Set<Attribute> refs) {
        if (expr instanceof IsNotNull) {
            return;
        }
        if (expr instanceof Attribute attr) {
            refs.add(attr);
            return;
        }
        for (Expression child : expr.children()) {
            collectRefsExcludingIsNotNull(child, refs);
        }
    }

    private static boolean isShape(DataType dataType) {
        return dataType == DataType.GEO_SHAPE || dataType == DataType.CARTESIAN_SHAPE;
    }

    private static Optional<AggregateFunction> extractAggregateFunction(NamedExpression expr) {
        return expr instanceof Alias as && as.child() instanceof AggregateFunction af ? Optional.of(af) : Optional.empty();
    }
}
