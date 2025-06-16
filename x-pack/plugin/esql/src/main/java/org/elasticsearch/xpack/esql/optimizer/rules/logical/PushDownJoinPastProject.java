/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * If a {@link Project} is found in the left child of a left {@link Join}, perform it after. Due to requiring the projected attributes
 * later, field extractions can also happen later, making joins cheapter to execute on data nodes.
 * E.g. {@code ... | RENAME field AS otherfield | LOOKUP JOIN lu_idx ON key}
 * becomes {@code ... | LOOKUP JOIN lu_idx ON key | RENAME field AS otherfield }.
 * When a {@code LOOKUP JOIN}'s lookup fields shadow the previous fields, we may need to leave an {@link Eval} in place to assign a
 * temporary name. Assume that {@code field} is a lookup field, then {@code ... | RENAME field AS otherfield | LOOKUP JOIN lu_idx ON key}
 * becomes something like {@code ... | EVAL $$field = field | LOOKUP JOIN lu_idx ON key | RENAME $$field AS otherfield}.
 * Leaving {@code EVAL $$field = field} in place of the original projection, rather than a Project, avoids infinite loops.
 */
public final class PushDownJoinPastProject extends OptimizerRules.OptimizerRule<Join> {
    @Override
    protected LogicalPlan rule(Join join) {
        if (join.left() instanceof Project project && join.config().type() == JoinTypes.LEFT) {
            AttributeMap.Builder<Expression> aliasBuilder = AttributeMap.builder();
            project.forEachExpression(Alias.class, a -> aliasBuilder.put(a.toAttribute(), a.child()));
            var aliasesFromProject = aliasBuilder.build();

            // Propagate any renames into the Join, as we will remove the upstream Project.
            // E.g. `RENAME field AS key | LOOKUP JOIN idx ON key` -> `LOOKUP JOIN idx ON field | ...`
            Join updatedJoin = PushDownUtils.resolveRenamesFromMap(join, aliasesFromProject);

            // Construct the expressions for the new downstream Project using the Join's output.
            // We need to carry over RENAMEs/aliases from the original upstream Project.
            List<Attribute> originalOutput = join.output();
            List<NamedExpression> newProjections = new ArrayList<>(originalOutput.size());
            for (Attribute attr : originalOutput) {
                Attribute resolved = (Attribute) aliasesFromProject.resolve(attr, attr);
                if (attr.semanticEquals(resolved)) {
                    newProjections.add(attr);
                } else {
                    Alias renamed = new Alias(attr.source(), attr.name(), resolved, attr.id(), attr.synthetic());
                    newProjections.add(renamed);
                }
            }

            // This doesn't deal with name conflicts yet. Any name shadowed by a lookup field from the `LOOKUP JOIN` could still have been
            // used in the original Project; any such conflict needs to be resolved by copying the attribute under a temporary name via an
            // Eval - and using the attribute from said Eval in the new downstream Project.
            Set<String> lookupFieldNames = new HashSet<>(Expressions.names(join.rightOutputFields()));
            PushDownUtils.AttributeReplacement replacement = PushDownUtils.renameAttributesInExpressions(lookupFieldNames, newProjections);

            List<Expression> conflictFreeProjections = replacement.rewrittenExpressions();
            List<Alias> evalAliases = new ArrayList<>(replacement.replacedAttributes().values());

            if (evalAliases.isEmpty()) {
                // No name conflicts, so no eval needed.
                return new Project(project.source(), updatedJoin.replaceLeft(project.child()), newProjections);
            }

            // The conflict free projections replaced any shadowed attributes by temporary attributes that we'll create in an Eval.
            // That's good if the replaced attribute was in an Alias; if the projection was a mere attribute to begin with, we need to
            // alias it back to the name/id that's expected in the original output.
            List<NamedExpression> finalProjections = new ArrayList<>(conflictFreeProjections.size());
            for (int i = 0; i < newProjections.size(); i++) {
                Expression conflictFreeProj = conflictFreeProjections.get(i);
                if (conflictFreeProj instanceof Alias as) {
                    // Already aliased - keep it, it's fine if the child was rewritten.
                    finalProjections.add(as);
                } else if (conflictFreeProj instanceof Attribute conflictFreeAttr) {
                    Attribute expectedOutputAttr = (Attribute) newProjections.get(i);
                    if (expectedOutputAttr.semanticEquals(conflictFreeAttr)) {
                        // no conflict, wasn't rewritten
                        finalProjections.add(expectedOutputAttr);
                    } else {
                        Alias renameBack = new Alias(
                            expectedOutputAttr.source(),
                            expectedOutputAttr.name(),
                            conflictFreeAttr,
                            expectedOutputAttr.id(),
                            expectedOutputAttr.synthetic()
                        );
                        finalProjections.add(renameBack);
                    }
                } else {
                    throw new IllegalStateException("Resolving a list of projections must yield a list of projections again");
                }
            }

            Eval eval = new Eval(project.source(), project.child(), evalAliases);
            Join finalJoin = new Join(join.source(), eval, updatedJoin.right(), updatedJoin.config());

            return new Project(project.source(), finalJoin, finalProjections);
        }

        return join;
    }
}
