/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

/**
 * Moves an {@link MvExpand} below the {@link Project} directly underneath it, so that the {@link Project} can keep moving up
 * the plan. Used by {@link HoistProjects}.
 * <p>
 * Without any renaming, the {@link Project} just has to output the expanded attribute instead of the target:
 * {@snippet lang="text":
 * MvExpand[a{r}#1,a{r}#2]                 Project[[a{r}#2, b{r}#3]]
 * \_Project[[a{r}#1, b{r}#3]]       =>    \_MvExpand[a{r}#1,a{r}#2]
 *   \_...                                   \_...
 * }
 * If either the {@link Project} or the {@link MvExpand} renames the target, applying the {@link MvExpand} below the
 * {@link Project} could make its output clash with other attributes of the same name. So the new expanded attribute always
 * gets a temporary name, and the {@link Project} renames it back, keeping the original expanded attribute's name id:
 * {@snippet lang="text":
 * MvExpand[b{r}#1,b{r}#2]                 Project[[$$a$mv_expand$4{r$}#4 AS b#2]]
 * \_Project[[a{r}#3 AS b#1]]        =>    \_MvExpand[a{r}#3,$$a$mv_expand$4{r$}#4]
 *   \_...                                   \_...
 * }
 * Finally, if the {@link Project} still needs the original values of the target's input attribute for another output, the
 * {@link MvExpand} cannot replace that attribute. An {@link Eval} then copies the input attribute to a temporary attribute, and
 * the {@link MvExpand} expands the copy instead:
 * {@snippet lang="text":
 * MvExpand[a{r}#1,a{r}#2]                 Project[[$$a$mv_expand$5{r$}#5 AS a#2, a{r}#1 AS b#3]]
 * \_Project[[a{r}#1, a{r}#1 AS b#3]] =>   \_MvExpand[$$a$mv_expand$4{r$}#4,$$a$mv_expand$5{r$}#5]
 *   \_...                                   \_Eval[[a{r}#1 AS $$a$mv_expand$4#4]]
 *                                             \_...
 * }
 */
public final class PushDownMvExpandPastProject extends OptimizerRules.OptimizerRule<MvExpand> {
    @Override
    protected LogicalPlan rule(MvExpand mvExpand) {
        return mvExpand.child() instanceof Project project ? pushDownPastProject(mvExpand, project) : mvExpand;
    }

    private static LogicalPlan pushDownPastProject(MvExpand mvExpand, Project project) {
        Attribute target = mvExpand.target().toAttribute();
        Attribute expanded = mvExpand.expanded();
        List<? extends NamedExpression> projections = project.projections();

        // Find the projection that outputs the target and the Project's input attribute it is defined by.
        int targetIndex = -1;
        for (int i = 0; i < projections.size(); i++) {
            if (projections.get(i).toAttribute().semanticEquals(target)) {
                targetIndex = i;
                break;
            }
        }
        if (targetIndex < 0) {
            return mvExpand;
        }
        NamedExpression targetProjection = projections.get(targetIndex);
        Attribute input;
        if (targetProjection instanceof Alias alias) {
            if (alias.child() instanceof Attribute aliased) {
                input = aliased;
            } else {
                return mvExpand;
            }
        } else if (targetProjection instanceof Attribute attribute) {
            input = attribute;
        } else {
            return mvExpand;
        }

        boolean renamed = targetProjection instanceof Alias || expanded.name().equals(target.name()) == false;
        boolean inputUsedElsewhere = false;
        for (int i = 0; i < projections.size(); i++) {
            if (i != targetIndex && projections.get(i).references().contains(input)) {
                inputUsedElsewhere = true;
                break;
            }
        }

        List<NamedExpression> newProjections = new ArrayList<>(projections);
        if (renamed == false && inputUsedElsewhere == false) {
            newProjections.set(targetIndex, expanded);
            return new Project(project.source(), new MvExpand(mvExpand.source(), project.child(), input, expanded), newProjections);
        }

        LogicalPlan newChild = project.child();
        Attribute newTarget = input;
        if (inputUsedElsewhere) {
            Alias copy = new Alias(input.source(), temporaryName(input.name()), input, null, true);
            newChild = new Eval(mvExpand.source(), newChild, List.of(copy));
            newTarget = copy.toAttribute();
        }
        // MV_EXPAND does not drop rows with null values, so the expanded attribute is exactly as nullable as the original one.
        Attribute newExpanded = new ReferenceAttribute(
            expanded.source(),
            null,
            temporaryName(input.name()),
            expanded.dataType(),
            expanded.nullable(),
            null,
            true,
            AnalyzedTextExpression.valuesAnalyzerOf(expanded)
        );
        newProjections.set(targetIndex, new Alias(targetProjection.source(), expanded.name(), newExpanded, expanded.id()));
        return new Project(project.source(), new MvExpand(mvExpand.source(), newChild, newTarget, newExpanded), newProjections);
    }

    /**
     * Temporary names are specific to this rule and carry a fresh {@link NameId}, so that repeated applications - also on
     * the same attribute - never produce the same name twice.
     */
    private static String temporaryName(String name) {
        return Attribute.rawTemporaryName(name, "mv_expand", new NameId().toString());
    }
}
