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
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.physical.AbstractAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LeafExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Materialize the concrete fields that need to be extracted from the storage until the last possible moment.
 * Expects the local plan to already have a projection containing the fields needed upstream.
 * <p>
 * 1. add the materialization right before usage inside the local plan
 * 2. materialize any missing fields needed further up the chain
 *
 * @see ProjectAwayColumns
 */
public class InsertFieldExtraction extends PhysicalOptimizerRules.ParameterizedOptimizerRule<PhysicalPlan, LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan rule(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        // apply the plan locally, adding a field extractor right before data is loaded
        // by going bottom-up
        plan = plan.transformUp(p -> {
            // skip source nodes
            if (p instanceof LeafExec) {
                return p;
            }

            var missing = missingAttributes(p);

            /*
             * If there is a single grouping then we'll try to use ords. Either way
             * it loads the field lazily. If we have more than one field we need to
             * make sure the fields are loaded for the standard hash aggregator.
             */
            if (p instanceof AbstractAggregateExec agg) {
                var ordinalAttributes = agg.ordinalAttributes();
                missing.removeAll(Expressions.references(ordinalAttributes));
            }

            // add extractor
            if (missing.isEmpty() == false) {
                // identify child (for binary nodes) that exports _doc and place the field extractor there
                List<PhysicalPlan> newChildren = new ArrayList<>(p.children().size());
                boolean found = false;
                for (PhysicalPlan child : p.children()) {
                    if (found == false) {
                        if (child.outputSet().stream().anyMatch(EsQueryExec::isSourceAttribute)) {
                            found = true;
                            // collect source attributes and add the extractor
                            child = new FieldExtractExec(
                                p.source(),
                                child,
                                List.copyOf(missing),
                                context.configuration().pragmas().fieldExtractPreference()
                            );
                        }
                    }
                    newChildren.add(child);
                }
                // somehow no doc id
                if (found == false) {
                    throw new IllegalArgumentException("No child with doc id found");
                }
                return p.replaceChildren(newChildren);
            }

            return p;
        });

        return plan;
    }

    private static Set<Attribute> missingAttributes(PhysicalPlan p) {
        var missing = new LinkedHashSet<Attribute>();
        var input = p.inputSet();

        // Collect field attributes referenced by this plan but not yet present in the child's output.
        // This is also correct for LookupJoinExec, where we only need field extraction on the left fields used to match, since the right
        // side is always materialized.
        p.references().forEach(f -> {
            if (f instanceof FieldAttribute || f instanceof MetadataAttribute) {
                if (input.contains(f) == false) {
                    missing.add(f);
                }
            }
        });

        return missing;
    }
}
