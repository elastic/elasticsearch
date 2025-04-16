/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;

class PushDownUtils {
    /**
     * Pushes LogicalPlans which generate new attributes (Eval, Grok/Dissect, Enrich), past OrderBys and Projections.
     * Although it seems arbitrary whether the OrderBy or the generating plan is executed first, this transformation ensures that OrderBys
     * only separated by e.g. an Eval can be combined by {@link PushDownAndCombineOrderBy}.
     * <p>
     * E.g. {@code ... | sort a | eval x = b + 1 | sort x} becomes {@code ... | eval x = b + 1 | sort a | sort x}
     * <p>
     * Ordering the generating plans before the OrderBys has the advantage that it's always possible to order the plans like this.
     * E.g., in the example above it would not be possible to put the eval after the two orderBys.
     * <p>
     * In case one of the generating plan's attributes would shadow the OrderBy's attributes, we alias the generated attribute first.
     * <p>
     * E.g. {@code ... | sort a | eval a = b + 1 | ...} becomes {@code ... | eval $$a = a | eval a = b + 1 | sort $$a | drop $$a ...}
     * <p>
     * In case the generating plan's attributes would shadow the Project's attributes, we rename the generated attributes in place.
     * <p>
     * E.g. {@code ... | rename a as z | eval a = b + 1 | ...} becomes {@code ... eval $$a = b + 1 | rename a as z, $$a as a ...}
     */
    public static <Plan extends UnaryPlan & GeneratingPlan<Plan>> LogicalPlan pushGeneratingPlanPastProjectAndOrderBy(Plan generatingPlan) {
        LogicalPlan child = generatingPlan.child();
        if (child instanceof OrderBy orderBy) {
            Set<String> evalFieldNames = new LinkedHashSet<>(Expressions.names(generatingPlan.generatedAttributes()));

            // Look for attributes in the OrderBy's expressions and create aliases with temporary names for them.
            AttributeReplacement nonShadowedOrders = renameAttributesInExpressions(evalFieldNames, orderBy.order());

            AttributeMap<Alias> aliasesForShadowedOrderByAttrs = nonShadowedOrders.replacedAttributes;
            @SuppressWarnings("unchecked")
            List<Order> newOrder = (List<Order>) (List<?>) nonShadowedOrders.rewrittenExpressions;

            if (aliasesForShadowedOrderByAttrs.isEmpty() == false) {
                List<Alias> newAliases = new ArrayList<>(aliasesForShadowedOrderByAttrs.values());

                LogicalPlan plan = new Eval(orderBy.source(), orderBy.child(), newAliases);
                plan = generatingPlan.replaceChild(plan);
                plan = new OrderBy(orderBy.source(), plan, newOrder);
                plan = new Project(generatingPlan.source(), plan, generatingPlan.output());

                return plan;
            }

            return orderBy.replaceChild(generatingPlan.replaceChild(orderBy.child()));
        } else if (child instanceof Project project) {
            // We need to account for attribute shadowing: a rename might rely on a name generated in an Eval/Grok/Dissect/Enrich.
            // E.g. in:
            //
            // Eval[[2 * x{f}#1 AS y]]
            // \_Project[[x{f}#1, y{f}#2, y{f}#2 AS z]]
            //
            // Just moving the Eval down breaks z because we shadow y{f}#2.
            // Instead, we use a different alias in the Eval, eventually renaming back to y:
            //
            // Project[[x{f}#1, y{f}#2 as z, $$y{r}#3 as y]]
            // \_Eval[[2 * x{f}#1 as $$y]]

            List<Attribute> generatedAttributes = generatingPlan.generatedAttributes();

            @SuppressWarnings("unchecked")
            Plan generatingPlanWithResolvedExpressions = (Plan) resolveRenamesFromProject(generatingPlan, project);

            Set<String> namesReferencedInRenames = new HashSet<>();
            for (NamedExpression ne : project.projections()) {
                if (ne instanceof Alias as) {
                    namesReferencedInRenames.addAll(as.child().references().names());
                }
            }
            Map<String, String> renameGeneratedAttributeTo = newNamesForConflictingAttributes(
                generatingPlan.generatedAttributes(),
                namesReferencedInRenames
            );
            List<String> newNames = generatedAttributes.stream()
                .map(attr -> renameGeneratedAttributeTo.getOrDefault(attr.name(), attr.name()))
                .toList();
            Plan generatingPlanWithRenamedAttributes = generatingPlanWithResolvedExpressions.withGeneratedNames(newNames);

            // Put the project at the top, but include the generated attributes.
            // Any generated attributes that had to be renamed need to be re-renamed to their original names.
            List<NamedExpression> generatedAttributesRenamedToOriginal = new ArrayList<>(generatedAttributes.size());
            List<Attribute> renamedGeneratedAttributes = generatingPlanWithRenamedAttributes.generatedAttributes();
            for (int i = 0; i < generatedAttributes.size(); i++) {
                Attribute originalAttribute = generatedAttributes.get(i);
                Attribute renamedAttribute = renamedGeneratedAttributes.get(i);
                if (originalAttribute.name().equals(renamedAttribute.name())) {
                    generatedAttributesRenamedToOriginal.add(renamedAttribute);
                } else {
                    generatedAttributesRenamedToOriginal.add(
                        new Alias(
                            originalAttribute.source(),
                            originalAttribute.name(),
                            renamedAttribute,
                            originalAttribute.id(),
                            originalAttribute.synthetic()
                        )
                    );
                }
            }

            Project projectWithGeneratingChild = project.replaceChild(generatingPlanWithRenamedAttributes.replaceChild(project.child()));
            return projectWithGeneratingChild.withProjections(
                mergeOutputExpressions(generatedAttributesRenamedToOriginal, projectWithGeneratingChild.projections())
            );
        }

        return generatingPlan;
    }

    /**
     * Replace attributes in the given expressions by assigning them temporary names.
     * Returns the rewritten expressions and a map with an alias for each replaced attribute; the rewritten expressions reference
     * these aliases.
     */
    private static AttributeReplacement renameAttributesInExpressions(
        Set<String> attributeNamesToRename,
        List<? extends Expression> expressions
    ) {
        AttributeMap.Builder<Alias> aliasesForReplacedAttributesBuilder = AttributeMap.builder();
        List<Expression> rewrittenExpressions = new ArrayList<>();

        for (Expression expr : expressions) {
            rewrittenExpressions.add(expr.transformUp(Attribute.class, attr -> {
                if (attributeNamesToRename.contains(attr.name())) {
                    Alias renamedAttribute = aliasesForReplacedAttributesBuilder.computeIfAbsent(attr, a -> {
                        String tempName = TemporaryNameUtils.locallyUniqueTemporaryName(a.name(), "temp_name");
                        return new Alias(a.source(), tempName, a, null, true);
                    });
                    return renamedAttribute.toAttribute();
                }

                return attr;
            }));
        }

        return new AttributeReplacement(rewrittenExpressions, aliasesForReplacedAttributesBuilder.build());
    }

    private static Map<String, String> newNamesForConflictingAttributes(
        List<Attribute> potentiallyConflictingAttributes,
        Set<String> reservedNames
    ) {
        if (reservedNames.isEmpty()) {
            return Map.of();
        }

        Map<String, String> renameAttributeTo = new HashMap<>();
        for (Attribute attr : potentiallyConflictingAttributes) {
            String name = attr.name();
            if (reservedNames.contains(name)) {
                renameAttributeTo.putIfAbsent(name, TemporaryNameUtils.locallyUniqueTemporaryName(name, "temp_name"));
            }
        }

        return renameAttributeTo;
    }

    public static Project pushDownPastProject(UnaryPlan parent) {
        if (parent.child() instanceof Project project) {
            UnaryPlan expressionsWithResolvedAliases = resolveRenamesFromProject(parent, project);

            return project.replaceChild(expressionsWithResolvedAliases.replaceChild(project.child()));
        } else {
            throw new EsqlIllegalArgumentException("Expected child to be instance of Project");
        }
    }

    private static UnaryPlan resolveRenamesFromProject(UnaryPlan plan, Project project) {
        AttributeMap.Builder<Expression> aliasBuilder = AttributeMap.builder();
        project.forEachExpression(Alias.class, a -> aliasBuilder.put(a.toAttribute(), a.child()));
        var aliases = aliasBuilder.build();

        return (UnaryPlan) plan.transformExpressionsOnly(ReferenceAttribute.class, r -> aliases.resolve(r, r));
    }

    private record AttributeReplacement(List<Expression> rewrittenExpressions, AttributeMap<Alias> replacedAttributes) {}
}
