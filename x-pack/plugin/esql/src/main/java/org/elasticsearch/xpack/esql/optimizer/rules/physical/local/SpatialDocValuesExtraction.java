/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.BinarySpatialFunction;
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

/**
 * This rule is responsible for marking spatial fields to be extracted from doc-values instead of source values.
 * This is a very specific optimization that is only used in the context of spatial aggregations.
 * Normally spatial fields are extracted from source values because this maintains original precision, but is very slow.
 * Simply loading from doc-values loses precision for points, and loses the geometry topological information for shapes.
 * For this reason we only consider loading from doc values under very specific conditions:
 * <ul>
 *     <li>The spatial data is consumed by a spatial aggregation (eg. <code>ST_CENTROIDS_AGG</code>, negating the need for precision.</li>
 *     <li>This aggregation is planned to run on the data node, so the doc-values Blocks are never transmit to the coordinator node.</li>
 *     <li>The data node index in question has doc-values stored for the field in question.</li>
 * </ul>
 * While we do not support transmitting spatial doc-values to the coordinator node, it is still important on the data node to ensure
 * that all spatial functions that will receive these doc-values are aware of this fact. For this reason, if the above conditions are met,
 * we need to make four edits to the local physical plan to consistently support spatial doc-values:
 * <ul>
 *     <li>The spatial aggregation function itself is marked using <code>withDocValues()</code> to enable its
 *     <code>toEvaluator()</code> method to produce the correct doc-values aware <code>Evaluator</code> functions.</li>
 *     <li>Any spatial functions called within <code>EVAL</code> commands before the doc-values are consumed by the aggregation
 *     also need to be marked using <code>withDocValues()</code> so their evaluators are correct.</li>
 *     <li>Any spatial functions used within filters, <code>WHERE</code> commands, are similarly marked for the same reason.</li>
 *     <li>The <code>FieldExtractExec</code> that will extract the field is marked with <code>withDocValuesAttributes(...)</code>
 *     so that it calls the <code>FieldType.blockReader()</code> method with the correct <code>FieldExtractPreference</code></li>
 * </ul>
 * The question has been raised why the spatial functions need to know if they are using doc-values or not. At first glance one might
 * perceive ES|QL functions as being logical planning only constructs, reflecting only the intent of the user. This, however, is not true.
 * The ES|QL functions all contain the runtime implementation of the functions behaviour, in the form of one or more static methods,
 * as well as a <code>toEvaluator()</code> instance method that is used to generates Block traversal code to call these runtime
 * implementations, based on some internal state of the instance of the function. In most cases this internal state contains information
 * determined during the logical planning phase, such as the field name and type, and whether it is a literal and can be folded.
 * In the case of spatial functions, the internal state also contains information about whether the function is using doc-values or not.
 * This knowledge is determined in the class being described here, and is only determined during local physical planning on each data
 * node. This is because the decision to use doc-values is based on the local data node's index configuration, and the local physical plan
 * is the only place where this information is available. This also means that the knowledge of the usage of doc-values does not need
 * to be serialized between nodes, and is only used locally.
 */
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
                    .map(f -> (Alias) f.transformDown(BinarySpatialFunction.class, s -> withDocValues(s, foundAttributes)))
                    .toList();
                if (changed.equals(fields) == false) {
                    exec = new EvalExec(exec.source(), exec.child(), changed);
                }
            }
            if (exec instanceof FilterExec filterExec) {
                // Note that ST_CENTROID does not support shapes, but SpatialRelatesFunction does, so when we extend the centroid
                // to support shapes, we need to consider loading shape doc-values for both centroid and relates (ST_INTERSECTS)
                var condition = filterExec.condition().transformDown(BinarySpatialFunction.class, s -> withDocValues(s, foundAttributes));
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

    private BinarySpatialFunction withDocValues(BinarySpatialFunction spatial, Set<FieldAttribute> foundAttributes) {
        // Only update the docValues flags if the field is found in the attributes
        boolean foundLeft = foundField(spatial.left(), foundAttributes);
        boolean foundRight = foundField(spatial.right(), foundAttributes);
        return foundLeft || foundRight ? spatial.withDocValues(foundLeft, foundRight) : spatial;
    }

    private boolean hasFieldAttribute(BinarySpatialFunction spatial, Set<FieldAttribute> foundAttributes) {
        return foundField(spatial.left(), foundAttributes) || foundField(spatial.right(), foundAttributes);
    }

    private boolean foundField(Expression expression, Set<FieldAttribute> foundAttributes) {
        return expression instanceof FieldAttribute field && foundAttributes.contains(field);
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
                if (hasFieldAttribute(relatesFunction, Set.of(candidate))) {
                    spatialRelatesAttributes.add(candidate);
                }
            });
        });
        // Disallow more than one spatial field to be extracted using doc-values (for now)
        return spatialRelatesAttributes.size() < 2;
    }
}
