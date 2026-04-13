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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.MissingEsField;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
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
        var preference = context.configuration() != null
            ? context.configuration().pragmas().fieldExtractPreference()
            : MappedFieldType.FieldExtractPreference.NONE;
        return InsertFieldExtraction.rule(plan, preference);
    }

    static PhysicalPlan rule(PhysicalPlan plan, MappedFieldType.FieldExtractPreference fieldExtractPreference) {
        // apply the plan locally, adding a field extractor right before data is loaded
        // by going bottom-up
        plan = plan.transformUp(p -> {
            // skip source nodes
            if (p instanceof LeafExec) {
                return p;
            }

            var missing = missingAttributes(p);

            if (missing.isEmpty() == false) {
                // Nullified fields (MissingEsField) don't exist in the index; materialize them as constant null columns
                // via EvalExec rather than attempting a FieldExtractExec that would require index access.
                List<Attribute> nullifiedFields = new ArrayList<>();
                List<Attribute> realFields = new ArrayList<>();
                for (Attribute attr : missing) {
                    if (attr instanceof FieldAttribute fa && fa.field() instanceof MissingEsField) {
                        nullifiedFields.add(attr);
                    } else {
                        realFields.add(attr);
                    }
                }

                // identify child (for binary nodes) that exports _doc and place the field extractor there
                List<PhysicalPlan> newChildren = new ArrayList<>(p.children().size());
                boolean handled = false;
                for (PhysicalPlan child : p.children()) {
                    if (handled == false) {
                        if (realFields.isEmpty() == false) {
                            if (child.outputSet().stream().anyMatch(EsQueryExec::isDocAttribute)) {
                                handled = true;
                                if (nullifiedFields.isEmpty() == false) {
                                    child = insertNullEval(p.source(), child, nullifiedFields);
                                }
                                child = new FieldExtractExec(p.source(), child, realFields, fieldExtractPreference);
                            }
                        } else {
                            // Only nullified fields -- no index access needed, pick first child
                            handled = true;
                            child = insertNullEval(p.source(), child, nullifiedFields);
                        }
                    }
                    newChildren.add(child);
                }
                if (handled == false) {
                    throw new IllegalArgumentException("No child with doc id found");
                }
                return p.replaceChildren(newChildren);
            }

            return p;
        });

        return plan;
    }

    private static PhysicalPlan insertNullEval(Source source, PhysicalPlan child, List<Attribute> nullifiedFields) {
        List<Alias> aliases = nullifiedFields.stream()
            .map(a -> new Alias(a.source(), a.name(), Literal.of(a, null), a.id()))
            .toList();
        return new EvalExec(source, child, aliases);
    }

    private static Set<Attribute> missingAttributes(PhysicalPlan p) {
        var missing = new LinkedHashSet<Attribute>();
        var input = p.inputSet();

        // Collect field attributes referenced by this plan but not yet present in the child's output.
        // This is also correct for LookupJoinExec, where we only need field extraction on the left fields used to match, since the right
        // side is always materialized.
        p.references().forEach(f -> {
            if ((f instanceof FieldAttribute || f instanceof MetadataAttribute) && EsQueryExec.isDocAttribute(f) == false) {
                if (input.contains(f) == false) {
                    missing.add(f);
                }
            }
        });

        return missing;
    }
}
