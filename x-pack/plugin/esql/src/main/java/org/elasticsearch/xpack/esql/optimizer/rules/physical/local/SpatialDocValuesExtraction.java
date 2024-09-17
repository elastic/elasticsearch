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
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SpatialDocValuesExtraction extends PhysicalOptimizerRules.OptimizerRule<AggregateExec> {
    @Override
    protected PhysicalPlan rule(AggregateExec aggregate) {
        var foundAttributes = new HashSet<FieldAttribute>();

        PhysicalPlan plan = aggregate.transformDown(UnaryExec.class, exec -> {
            if (exec instanceof AggregateExec agg) {
                var orderedAggregates = new ArrayList<NamedExpression>();
                var changedAggregates = false;
                for (NamedExpression aggExpr : agg.aggregates()) {
                    if (aggExpr instanceof Alias as && as.child() instanceof SpatialAggregateFunction af) {
                        if (af.field() instanceof FieldAttribute fieldAttribute
                            && allowedForDocValues(fieldAttribute, agg, foundAttributes)) {
                            // We need to both mark the field to load differently, and change the spatial function to know to use it
                            foundAttributes.add(fieldAttribute);
                            changedAggregates = true;
                            orderedAggregates.add(as.replaceChild(af.withDocValues()));
                        } else {
                            orderedAggregates.add(aggExpr);
                        }
                    } else {
                        orderedAggregates.add(aggExpr);
                    }
                }
                if (changedAggregates) {
                    exec = new AggregateExec(
                        agg.source(),
                        agg.child(),
                        agg.groupings(),
                        orderedAggregates,
                        agg.getMode(),
                        agg.intermediateAttributes(),
                        agg.estimatedRowSize()
                    );
                }
            }
            if (exec instanceof EvalExec evalExec) {
                List<Alias> fields = evalExec.fields();
                List<Alias> changed = fields.stream()
                    .map(
                        f -> (Alias) f.transformDown(
                            SpatialRelatesFunction.class,
                            spatialRelatesFunction -> (spatialRelatesFunction.hasFieldAttribute(foundAttributes))
                                ? spatialRelatesFunction.withDocValues(foundAttributes)
                                : spatialRelatesFunction
                        )
                    )
                    .toList();
                if (changed.equals(fields) == false) {
                    exec = new EvalExec(exec.source(), exec.child(), changed);
                }
            }
            if (exec instanceof FilterExec filterExec) {
                // Note that ST_CENTROID does not support shapes, but SpatialRelatesFunction does, so when we extend the centroid
                // to support shapes, we need to consider loading shape doc-values for both centroid and relates (ST_INTERSECTS)
                var condition = filterExec.condition()
                    .transformDown(
                        SpatialRelatesFunction.class,
                        spatialRelatesFunction -> (spatialRelatesFunction.hasFieldAttribute(foundAttributes))
                            ? spatialRelatesFunction.withDocValues(foundAttributes)
                            : spatialRelatesFunction
                    );
                if (filterExec.condition().equals(condition) == false) {
                    exec = new FilterExec(filterExec.source(), filterExec.child(), condition);
                }
            }
            if (exec instanceof FieldExtractExec fieldExtractExec) {
                // Tell the field extractor that it should extract the field from doc-values instead of source values
                var attributesToExtract = fieldExtractExec.attributesToExtract();
                Set<Attribute> docValuesAttributes = new HashSet<>();
                for (Attribute found : foundAttributes) {
                    if (attributesToExtract.contains(found)) {
                        docValuesAttributes.add(found);
                    }
                }
                if (docValuesAttributes.isEmpty() == false) {
                    exec = fieldExtractExec.withDocValuesAttributes(docValuesAttributes);
                }
            }
            return exec;
        });
        return plan;
    }

    /**
     * This function disallows the use of more than one field for doc-values extraction in the same spatial relation function.
     * This is because comparing two doc-values fields is not supported in the current implementation.
     */
    private boolean allowedForDocValues(FieldAttribute fieldAttribute, AggregateExec agg, Set<FieldAttribute> foundAttributes) {
        if (fieldAttribute.field().isAggregatable() == false) {
            return false;
        }
        var candidateDocValuesAttributes = new HashSet<>(foundAttributes);
        candidateDocValuesAttributes.add(fieldAttribute);
        var spatialRelatesAttributes = new HashSet<FieldAttribute>();
        agg.forEachExpressionDown(SpatialRelatesFunction.class, relatesFunction -> {
            candidateDocValuesAttributes.forEach(candidate -> {
                if (relatesFunction.hasFieldAttribute(Set.of(candidate))) {
                    spatialRelatesAttributes.add(candidate);
                }
            });
        });
        // Disallow more than one spatial field to be extracted using doc-values (for now)
        return spatialRelatesAttributes.size() < 2;
    }
}
