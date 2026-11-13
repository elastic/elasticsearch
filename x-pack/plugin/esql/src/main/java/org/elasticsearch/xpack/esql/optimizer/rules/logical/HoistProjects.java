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
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Moves every {@link Project} up past the streaming nodes above it in a single bottom-up pass. The top-down rules each move a
 * {@code Project} one node per batch iteration, so a long chain of {@code LOOKUP JOIN} blocks would otherwise need one
 * iteration per block and can exceed the batch's rule execution limit.
 * <p>
 * Where a node has its own top-down rule, its case must stay identical to the {@code Project} step of that rule; only the
 * bottom-up direction differs. {@link Join} and {@link MvExpand} have no such rule, so their pushdowns live here.
 */
public final class HoistProjects extends OptimizerRules.OptimizerRule<LogicalPlan> implements OptimizerRules.LocalAware<LogicalPlan> {

    private final PushDownEval pushDownEval = new PushDownEval();
    private final CombineProjections combineProjections;

    public HoistProjects() {
        this(false);
    }

    private HoistProjects(boolean local) {
        super(OptimizerRules.TransformDirection.UP);
        this.combineProjections = new CombineProjections(local);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return switch (plan) {
            case Join join when join.left() instanceof Project project -> pushDownJoinPastProject(join, project);
            case MvExpand mvExpand when mvExpand.child() instanceof Project project -> pushDownMvExpandPastProject(mvExpand, project);
            case Project project when project.child() instanceof Project -> combineProjections.rule(project);
            case Eval eval when eval.child() instanceof Project -> pushDownEval.rule(eval);
            case Limit limit when limit.child() instanceof Project project -> project.replaceChild(limit.replaceChild(project.child()));
            case Filter filter when filter.child() instanceof Project -> PushDownUtils.pushDownPastProject(filter);
            case OrderBy orderBy when orderBy.child() instanceof Project -> PushDownUtils.pushDownPastProject(orderBy);
            case RegexExtract regex when regex.child() instanceof Project -> PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(regex);
            case Enrich enrich when enrich.child() instanceof Project -> PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(enrich);
            case CompoundOutputEval<?> coe when coe.child() instanceof Project -> PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(
                coe
            );
            case InferencePlan<?> inference when inference.child() instanceof Project -> PushDownUtils
                .pushGeneratingPlanPastProjectAndOrderBy(inference);
            default -> plan;
        };
    }

    @Override
    public Rule<LogicalPlan, LogicalPlan> local() {
        return new HoistProjects(true);
    }

    /**
     * If a {@link Project} is found in the left child of a left {@link Join}, perform it after. Due to requiring the projected attributes
     * later, field extractions can also happen later, making joins cheaper to execute on data nodes.
     * E.g. {@code ... | RENAME field AS otherfield | LOOKUP JOIN lu_idx ON key}
     * becomes {@code ... | LOOKUP JOIN lu_idx ON key | RENAME field AS otherfield }.
     * When a {@code LOOKUP JOIN}'s lookup fields shadow the previous fields, we need to leave an {@link Eval} in place of the
     * {@link Project} to assign a temporary name. Assume that {@code field} is a lookup-added field, then
     * {@code ... | RENAME field AS otherfield | LOOKUP JOIN lu_idx ON key}
     * becomes something like
     * {@code ... | EVAL $$field = field | LOOKUP JOIN lu_idx ON key | RENAME $$field AS otherfield}.
     * Leaving {@code EVAL $$field = field} in place of the original projection, rather than a Project, avoids infinite loops.
     */
    private static LogicalPlan pushDownJoinPastProject(Join join, Project project) {
        if (join instanceof InlineJoin) {
            // Do not apply to INLINE STATS; this rule could be expanded to include INLINE STATS, but the StubRelation refers to the left
            // child - so pulling out a Project from the left child would require us to also update the StubRelation (and the Aggregate
            // on top of it)
            // TODO: figure out how to push down in case of INLINE STATS
            return join;
        }
        if (join.config().type() != JoinTypes.LEFT) {
            return join;
        }

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
        List<NamedExpression> finalProjections = new ArrayList<>(newProjections.size());
        AttributeMap.Builder<Alias> aliasesForReplacedAttributesBuilder = AttributeMap.builder();
        AttributeSet leftOutput = project.child().outputSet();

        for (NamedExpression newProj : newProjections) {
            Attribute coreAttr = (Attribute) Alias.unwrap(newProj);
            // Only fields from the left need to be protected from conflicts - because fields from the right shadow them.
            if (leftOutput.contains(coreAttr) && lookupFieldNames.contains(coreAttr.name())) {
                // Conflict - the core attribute will be shadowed by the `LOOKUP JOIN` and we need to alias it in an upstream Eval.
                Alias renaming = aliasesForReplacedAttributesBuilder.computeIfAbsent(coreAttr, a -> {
                    String tempName = TemporaryNameGenerator.locallyUniqueTemporaryName(a.name());
                    return new Alias(a.source(), tempName, a, null, true);
                });

                Attribute renamedAttribute = renaming.toAttribute();
                Alias renamedBack;
                if (newProj instanceof Alias as) {
                    renamedBack = new Alias(as.source(), as.name(), renamedAttribute, as.id(), as.synthetic());
                } else {
                    // no alias - that means proj == coreAttr
                    renamedBack = new Alias(coreAttr.source(), coreAttr.name(), renamedAttribute, coreAttr.id(), coreAttr.synthetic());
                }
                finalProjections.add(renamedBack);
            } else {
                finalProjections.add(newProj);
            }
        }

        if (aliasesForReplacedAttributesBuilder.isEmpty()) {
            // No name conflicts, so no eval needed.
            return new Project(project.source(), updatedJoin.replaceLeft(project.child()), newProjections);
        }

        List<Alias> renamesForEval = new ArrayList<>(aliasesForReplacedAttributesBuilder.build().values());
        Eval eval = new Eval(project.source(), project.child(), renamesForEval);
        Join finalJoin = new Join(join.source(), eval, updatedJoin.right(), updatedJoin.config(), updatedJoin.executesOn());

        return new Project(project.source(), finalJoin, finalProjections);
    }

    /**
     * Moves an {@link MvExpand} below the {@link Project} directly underneath it. The {@link MvExpand} then expands the
     * {@link Project}'s input attribute that defines the target, and the {@link Project} outputs the expanded attribute in place
     * of the target:
     * {@snippet lang="text":
     * MvExpand[b{r}#1,b{r}#2]                 Project[[b{r}#2]]
     * \_Project[[a{r}#3 AS b#1]]        =>    \_MvExpand[a{r}#3,b{r}#2]
     *   \_...                                   \_...
     * }
     * Two independent fixes may be needed on top of that.
     * <p>
     * If another output of the {@link Project} still needs the unexpanded values of the input attribute, an {@link Eval} copies the
     * input attribute to a temporary attribute first, and the other outputs use the copy instead:
     * {@snippet lang="text":
     * MvExpand[a{r}#1,a{r}#2]                  Project[[a{r}#2, $$a$mv_expand$4{r$}#4 AS b#3]]
     * \_Project[[a{r}#1, a{r}#1 AS b#3]]  =>   \_MvExpand[a{r}#1,a{r}#2]
     *   \_...                                    \_Eval[[a{r}#1 AS $$a$mv_expand$4#4]]
     *                                              \_...
     * }
     * If the expanded attribute's name is already used by another attribute below the {@link Project} - e.g. one that the
     * {@link Project} drops or renames - the {@link MvExpand} would output two attributes of the same name. The expanded
     * attribute then gets a temporary name, and the {@link Project} renames it back, keeping the original expanded attribute's
     * name id:
     * {@snippet lang="text":
     * MvExpand[b{r}#1,b{r}#2]                 Project[[$$b$mv_expand$5{r$}#5 AS b#2]]
     * \_Project[[a{r}#3 AS b#1]]        =>    \_MvExpand[a{r}#3,$$b$mv_expand$5{r$}#5]
     *   \_...[a{r}#3, b{r}#4]                   \_...[a{r}#3, b{r}#4]
     * }
     */
    private static LogicalPlan pushDownMvExpandPastProject(MvExpand mvExpand, Project project) {
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
            // Should never happen: the target is resolved against the MvExpand's child, i.e. the Project's output.
            return mvExpand;
        }
        NamedExpression targetProjection = projections.get(targetIndex);
        Attribute input;
        if (targetProjection instanceof Alias alias) {
            if (alias.child() instanceof Attribute aliased) {
                input = aliased;
            } else {
                // Should never happen: a Project only aliases attributes, see Project#validateProjections.
                return mvExpand;
            }
        } else if (targetProjection instanceof Attribute attribute) {
            input = attribute;
        } else {
            // Should never happen: a resolved Project only contains attributes and aliases, see Project#validateProjections.
            return mvExpand;
        }

        List<NamedExpression> newProjections = new ArrayList<>(projections);
        LogicalPlan newChild = project.child();

        // Other outputs that still need the unexpanded input get it from a copy.
        Alias copy = null;
        for (int i = 0; i < newProjections.size(); i++) {
            NamedExpression projection = newProjections.get(i);
            if (i == targetIndex || projection.references().contains(input) == false) {
                continue;
            }
            if (copy == null) {
                copy = new Alias(input.source(), temporaryName(input.name()), input, null, true);
            }
            Attribute copied = copy.toAttribute();
            newProjections.set(
                i,
                projection instanceof Alias alias
                    ? alias.replaceChild(alias.child().transformUp(Attribute.class, a -> a.semanticEquals(input) ? copied : a))
                    : new Alias(projection.source(), projection.name(), copied, projection.id(), projection.synthetic())
            );
        }
        if (copy != null) {
            newChild = new Eval(mvExpand.source(), newChild, List.of(copy));
        }

        // The expanded attribute must not share its name with another attribute below the Project.
        boolean nameClash = false;
        for (Attribute attribute : newChild.output()) {
            if (attribute.name().equals(expanded.name()) && attribute.semanticEquals(input) == false) {
                nameClash = true;
                break;
            }
        }
        Attribute newExpanded = expanded;
        NamedExpression expandedProjection = expanded;
        if (nameClash) {
            // MV_EXPAND does not drop rows with null values, so the expanded attribute is exactly as nullable as the original one.
            newExpanded = new ReferenceAttribute(
                expanded.source(),
                null,
                temporaryName(expanded.name()),
                expanded.dataType(),
                expanded.nullable(),
                null,
                true,
                AnalyzedTextExpression.valuesAnalyzerOf(expanded)
            );
            expandedProjection = new Alias(targetProjection.source(), expanded.name(), newExpanded, expanded.id());
        }
        newProjections.set(targetIndex, expandedProjection);

        return new Project(project.source(), new MvExpand(mvExpand.source(), newChild, input, newExpanded), newProjections);
    }

    /**
     * Temporary names are specific to the {@link MvExpand} pushdown and carry a fresh {@link NameId}, so that repeated
     * pushdowns - also of the same attribute - never produce the same name twice.
     */
    private static String temporaryName(String name) {
        return Attribute.rawTemporaryName(name, "mv_expand", new NameId().toString());
    }
}
