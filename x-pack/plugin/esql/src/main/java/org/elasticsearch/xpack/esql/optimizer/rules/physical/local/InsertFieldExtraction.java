/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 *
 * Materialize the concrete fields that need to be extracted from the storage until the last possible moment.
 * Expects the local plan to already have a projection containing the fields needed upstream.
 * <p>
 * 1. add the materialization right before usage inside the local plan
 * 2. materialize any missing fields needed further up the chain
 *
 * @see ProjectAwayColumns
 */
public class InsertFieldExtraction extends Rule<PhysicalPlan, PhysicalPlan> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        // apply the plan locally, adding a field extractor right before data is loaded
        // by going bottom-up
        plan = plan.transformUp(UnaryExec.class, p -> {
            var missing = missingAttributes(p);

            /*
             * If there is a single grouping then we'll try to use ords. Either way
             * it loads the field lazily. If we have more than one field we need to
             * make sure the fields are loaded for the standard hash aggregator.
             */
            if (p instanceof AggregateExec agg && agg.groupings().size() == 1) {
                var leaves = new LinkedList<>();
                // TODO: this seems out of place
                agg.aggregates().stream().filter(a -> agg.groupings().contains(a) == false).forEach(a -> leaves.addAll(a.collectLeaves()));
                var remove = agg.groupings().stream().filter(g -> leaves.contains(g) == false).toList();
                missing.removeAll(Expressions.references(remove));
            }

            // add extractor
            if (missing.isEmpty() == false) {
                // collect source attributes and add the extractor
                var extractor = new FieldExtractExec(p.source(), p.child(), List.copyOf(missing));
                p = p.replaceChild(extractor);
            }

            return p;
        });

        return plan;
    }

    private static Set<Attribute> missingAttributes(PhysicalPlan p) {
        var missing = new LinkedHashSet<Attribute>();
        var input = p.inputSet();

        // collect field attributes used inside expressions
        p.forEachExpression(TypedAttribute.class, f -> {
            if (f instanceof FieldAttribute || f instanceof MetadataAttribute) {
                if (input.contains(f) == false) {
                    missing.add(f);
                }
            }
        });
        return missing;
    }
}
