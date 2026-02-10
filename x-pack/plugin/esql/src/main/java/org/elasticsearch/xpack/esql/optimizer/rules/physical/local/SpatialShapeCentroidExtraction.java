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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
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
 * This rule is responsible for marking spatial shape fields whose centroid can be extracted from the binary representation.
 * This is a very specific optimization that is only used in the context of <code>ST_CENTROID_AGG</code> aggregations.
 * Normally spatial fields are extracted from source values because this maintains original precision, but is very slow.
 * The centroid data (location and weight) is stored in the header of the shape doc-values, allowing for fast extraction.
 * For this reason we only consider extracting the centroid under very specific conditions:
 * <ul>
 *     <li>The spatial data is of type GEO_SHAPE or CARTESIAN_SHAPE.</li>
 *     <li>The spatial data is consumed directly by an <code>ST_CENTROID_AGG</code>.</li>
 *     <li>The spatial data is not consumed by any other operation. While this is stricter than necessary,
 *     it is a good enough approximation for now.</li>
 * </ul>
 */
public class SpatialShapeCentroidExtraction extends ParameterizedOptimizerRule<AggregateExec, LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(AggregateExec aggregate, LocalPhysicalOptimizerContext ctx) {
        Set<Attribute> foundAttributes = findSpatialShapeCentroidAttributes(aggregate, ctx);
        if (foundAttributes.isEmpty()) {
            return aggregate;
        }
        return aggregate.transformDown(PhysicalPlan.class, exec -> switch (exec) {
            case AggregateExec agg -> transformAggregateExec(agg, foundAttributes);
            case FieldExtractExec fieldExtractExec -> transformFieldExtractExec(fieldExtractExec, foundAttributes);
            default -> exec;
        });
    }

    private static Set<Attribute> findSpatialShapeCentroidAttributes(AggregateExec aggregate, LocalPhysicalOptimizerContext ctx) {
        var foundAttributes = new HashSet<Attribute>();
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
                    List<AggregateFunction> nonSpatialCentroids = aggregateFunctions.stream()
                        .filter(a -> a instanceof SpatialCentroid == false)
                        .toList();
                    // Fields appearing in non-centroid aggregations should not be optimized
                    Set<EsField> fieldsAppearingInNonSpatialCentroids = nonSpatialCentroids.stream()
                        .flatMap(af -> af.references().stream())
                        .filter(FieldAttribute.class::isInstance)
                        .map(f -> ((FieldAttribute) f).field())
                        .collect(Collectors.toSet());
                    spatialCentroids.stream()
                        .map(SpatialCentroid::field)
                        .filter(FieldAttribute.class::isInstance)
                        .map(FieldAttribute.class::cast)
                        .filter(
                            f -> isShape(f.field().getDataType())
                                && fieldsAppearingInNonSpatialCentroids.contains(f.field()) == false
                                && ctx.searchStats().hasDocValues(f.fieldName())
                        )
                        .forEach(foundAttributes::add);
                }
                case EvalExec evalExec -> foundAttributes.removeAll(evalExec.references());
                case FilterExec filterExec -> foundAttributes.removeAll(filterExec.condition().references());
                default -> { // Do nothing
                }
            }
            return exec;
        });
        return foundAttributes;
    }

    private static PhysicalPlan transformFieldExtractExec(FieldExtractExec fieldExtractExec, Set<Attribute> foundAttributes) {
        var centroidAttributes = new HashSet<>(foundAttributes);
        centroidAttributes.retainAll(fieldExtractExec.attributesToExtract());
        return fieldExtractExec.withCentroidAttributes(centroidAttributes);
    }

    private static PhysicalPlan transformAggregateExec(AggregateExec agg, Set<Attribute> foundAttributes) {
        return agg.transformExpressionsDown(
            SpatialCentroid.class,
            spatialCentroid -> foundAttributes.contains(spatialCentroid.field())
                ? spatialCentroid.withFieldExtractPreference(MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_CENTROID)
                : spatialCentroid
        );
    }

    private static boolean isShape(DataType dataType) {
        return dataType == DataType.GEO_SHAPE || dataType == DataType.CARTESIAN_SHAPE;
    }

    private static Optional<AggregateFunction> extractAggregateFunction(NamedExpression expr) {
        return expr instanceof Alias as && as.child() instanceof AggregateFunction af ? Optional.of(af) : Optional.empty();
    }
}
