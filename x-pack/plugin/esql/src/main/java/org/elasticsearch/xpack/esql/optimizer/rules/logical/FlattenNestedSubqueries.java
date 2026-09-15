/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Removes a plain {@link UnionAll} after other optimizer rules have reduced it to one branch.
 *
 * <p>A one-branch union has no union semantics but still maps to a pipeline-breaking {@code MergeExec} on the coordinator, so it can be
 * eliminated. Multi-branch unions, {@link ViewUnionAll}s, and bare FORK plans are left unchanged.
 *
 * <h2>Output-identity projection</h2>
 * <p>
 * The {@link UnionAll} and its surviving child branch may use different {@link Attribute} instances for the same logical column even
 * though they carry the same name and data type. This happens because the analyzer assigns fresh attribute IDs to the union's output
 * that are independent of the IDs inside each branch. When the IDs differ the plan above the {@link UnionAll} holds references that
 * no longer resolve against the child. This rule detects the mismatch and inserts a {@link Project} that re-maps the child's
 * attributes to the IDs the rest of the plan expects.
 *
 * <h2>Example</h2>
 * <p>
 * Consider the query:
 * <pre>
 * FROM employees, (FROM languages)
 * | WHERE emp_no &gt; 10000
 * </pre>
 * The {@code languages} index has no {@code emp_no} field, so predicate-pushdown and empty-branch pruning eliminate that branch,
 * leaving a single-branch {@link UnionAll} whose surviving child is the employees branch:
 * <pre>
 * UnionAll[emp_no{r}#1, ...]          ← output uses union-assigned reference IDs
 * └─ Limit[1000]
 *    └─ Filter[emp_no &gt; 10000]
 *       └─ EsRelation[employees][emp_no{f}#2, ...]
 * </pre>
 * The union output {@code [emp_no{r}#1, ...]} differs from the child output {@code [emp_no{f}#2, ...]} in attribute ID, so a
 * correlating {@link Project} is inserted:
 * <pre>
 * Project[emp_no{f}#2 AS emp_no#1, ...]
 * └─ Limit[1000]
 *    └─ Filter[emp_no &gt; 10000]
 *       └─ EsRelation[employees][emp_no{f}#2, ...]
 * </pre>
 * The {@link UnionAll} is gone; the plan is now a linear pipeline with no merge point.
 */
public final class FlattenNestedSubqueries extends OptimizerRules.OptimizerRule<UnionAll> {

    public FlattenNestedSubqueries() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(UnionAll unionAll) {
        // TODO more flatten rules can be added here later
        return flattenUnionAllWithOneChild(unionAll);
    }

    private static LogicalPlan flattenUnionAllWithOneChild(UnionAll unionAll) {
        if (unionAll instanceof ViewUnionAll || unionAll.children().size() != 1) {
            return unionAll;
        }
        LogicalPlan child = unionAll.children().getFirst();
        List<Attribute> unionOutput = unionAll.output();
        List<Attribute> childOutput = child.output();
        if (unionOutput.equals(childOutput)) {
            return child;
        }
        if (unionOutput.size() != childOutput.size()) {
            return unionAll;
        }

        Map<String, Attribute> childByName = new HashMap<>(childOutput.size());
        for (Attribute childAttribute : childOutput) {
            if (childByName.put(childAttribute.name(), childAttribute) != null) {
                return unionAll;
            }
        }

        Set<String> unionSeen = new HashSet<>(unionOutput.size());
        List<NamedExpression> projections = new ArrayList<>(unionOutput.size());
        for (Attribute unionAttribute : unionOutput) {
            if (unionSeen.add(unionAttribute.name()) == false) {
                return unionAll;
            }
            Attribute childAttribute = childByName.get(unionAttribute.name());
            if (childAttribute == null || childAttribute.dataType() != unionAttribute.dataType()) {
                return unionAll;
            }
            if (unionAttribute.equals(childAttribute)) {
                projections.add(childAttribute);
            } else {
                // Alias.toAttribute() always creates a ReferenceAttribute. Keep the union when correlation would
                // erase a specialized output subtype such as ExternalMetadataAttribute/VirtualAttribute, since
                // downstream optimizer and execution rules use those marker types for correctness.
                if (unionAttribute instanceof ReferenceAttribute == false) {
                    return unionAll;
                }
                projections.add(
                    new Alias(
                        unionAttribute.source(),
                        unionAttribute.name(),
                        childAttribute,
                        unionAttribute.id(),
                        unionAttribute.synthetic()
                    )
                );
            }
        }
        return new Project(unionAll.source(), child, projections);
    }
}
