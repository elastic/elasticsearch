/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtent;
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
                    centroidAttributes.removeAll(filterExec.condition().references());
                    boundsAttributes.removeAll(filterExec.condition().references());
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
        PhysicalPlan result = agg;

        // Transform centroid aggregations
        if (centroidAttributes.isEmpty() == false) {
            result = result.transformExpressionsDown(
                SpatialCentroid.class,
                spatialCentroid -> centroidAttributes.contains(spatialCentroid.field())
                    ? spatialCentroid.withFieldExtractPreference(spatialCentroid.fieldExtractPreference().getWithSpatialCentroid())
                    : spatialCentroid
            );
        }

        // Transform extent aggregations
        if (boundsAttributes.isEmpty() == false) {
            result = result.transformExpressionsDown(
                SpatialExtent.class,
                spatialExtent -> boundsAttributes.contains(spatialExtent.field())
                    ? spatialExtent.withFieldExtractPreference(spatialExtent.fieldExtractPreference().getWithSpatialBounds())
                    : spatialExtent
            );
        }

        // When centroid and extent share a field, ensure both use the combined preference
        // This handles cases where one was already set by the above transforms
        Set<Attribute> sharedAttributes = new HashSet<>(centroidAttributes);
        sharedAttributes.retainAll(boundsAttributes);

        if (sharedAttributes.isEmpty() == false) {
            // Upgrade centroid aggregations on shared fields to also include bounds
            result = result.transformExpressionsDown(
                SpatialCentroid.class,
                spatialCentroid -> sharedAttributes.contains(spatialCentroid.field())
                    ? spatialCentroid.withFieldExtractPreference(spatialCentroid.fieldExtractPreference().getWithSpatialBounds())
                    : spatialCentroid
            );

            // Upgrade extent aggregations on shared fields to also include centroid
            result = result.transformExpressionsDown(
                SpatialExtent.class,
                spatialExtent -> sharedAttributes.contains(spatialExtent.field())
                    ? spatialExtent.withFieldExtractPreference(spatialExtent.fieldExtractPreference().getWithSpatialCentroid())
                    : spatialExtent
            );
        }

        return result;
    }

    private static boolean isShape(DataType dataType) {
        return dataType == DataType.GEO_SHAPE || dataType == DataType.CARTESIAN_SHAPE;
    }

    private static Optional<AggregateFunction> extractAggregateFunction(NamedExpression expr) {
        return expr instanceof Alias as && as.child() instanceof AggregateFunction af ? Optional.of(af) : Optional.empty();
    }
}
