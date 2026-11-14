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
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedReduceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Lets NESTED_EXPAND, NESTED_FLATTEN and NESTED reductions on a data node read each parent's child documents from the index. It keeps
 * {@code _doc} through the projections below them; without it they read {@code _source}, which is also what the
 * {@code nested_loader: source} pragma asks for. When every child field has doc values on this node, the index is
 * certain to serve them, so {@code _source} is not loaded either.
 */
public final class LoadNestedFromIndex extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    PhysicalPlan,
    LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        if ((plan instanceof NestedExpandExec || plan instanceof NestedReduceExec) == false
            || context.configuration().pragmas().nestedLoader() != QueryPragmas.NestedLoader.INDEX) {
            return plan;
        }
        UnaryExec unary = (UnaryExec) plan;
        PhysicalPlan child = keepDoc(unary.child());
        if (doc(child) == null) {
            return plan;
        }
        Attribute source = sourceWithDocValues(unary, context.searchStats());
        if (source == null) {
            return child == unary.child() ? plan : unary.replaceChild(child);
        }
        PhysicalPlan withoutSource = unary.replaceChild(dropSource(child, source));
        if (withoutSource instanceof NestedExpandExec expand) {
            return new NestedExpandExec(
                expand.source(),
                expand.child(),
                expand.flatten(),
                expand.metadata().subList(1, expand.metadata().size()),
                expand.path(),
                expand.indices(),
                expand.expanded(),
                expand.paths(),
                expand.mappedIndices(),
                expand.sourceFailures()
            );
        }
        NestedReduceExec reduce = (NestedReduceExec) withoutSource;
        List<Alias> fields = reduce.fields()
            .stream()
            .map(alias -> (Alias) alias.transformUp(NestedReduction.class, NestedReduction::withoutSource))
            .toList();
        return new NestedReduceExec(reduce.source(), reduce.child(), fields);
    }

    /**
     * The {@code _source} column {@code plan} reads, if every child field it reads has doc values on this node. All
     * reductions in one EVAL share the column, so it stays when any of them would read source.
     */
    private static Attribute sourceWithDocValues(UnaryExec plan, SearchStats stats) {
        if (plan instanceof NestedExpandExec expand) {
            boolean docValues = expand.paths()
                .stream()
                .allMatch(p -> stats.nestedFieldHasDocValues(new FieldAttribute.FieldName(expand.path() + "." + p)));
            return docValues ? expand.metadata().getFirst() : null;
        }
        List<NestedReduction> reductions = new ArrayList<>();
        ((NestedReduceExec) plan).fields().forEach(alias -> alias.forEachDown(NestedReduction.class, reductions::add));
        for (NestedReduction reduction : reductions) {
            if (reduction.fieldNames().stream().anyMatch(name -> stats.nestedFieldHasDocValues(new FieldAttribute.FieldName(name)) == false)
                || reduction.children().getFirst() instanceof Attribute == false) {
                return null;
            }
        }
        return reductions.isEmpty() ? null : (Attribute) reductions.getFirst().children().getFirst();
    }

    private static PhysicalPlan keepDoc(PhysicalPlan plan) {
        if (doc(plan) != null || plan instanceof UnaryExec == false) {
            return plan;
        }
        UnaryExec unary = (UnaryExec) plan;
        PhysicalPlan child = keepDoc(unary.child());
        Attribute doc = doc(child);
        if (doc == null) {
            return plan;
        }
        if (unary instanceof ProjectExec project) {
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            projections.add(doc);
            return new ProjectExec(project.source(), child, projections);
        }
        PhysicalPlan replaced = unary.replaceChild(child);
        return doc(replaced) == null ? plan : replaced;
    }

    /**
     * Removes {@code source}, and the column it renames, from the projections below, so nothing asks to load it there.
     * Anything above that still reads that column has it loaded there instead, as {@code _doc} is kept.
     */
    private static PhysicalPlan dropSource(PhysicalPlan plan, Attribute source) {
        if (plan instanceof UnaryExec == false) {
            return plan;
        }
        UnaryExec unary = (UnaryExec) plan;
        PhysicalPlan child = dropSource(unary.child(), source);
        if (unary instanceof ProjectExec project) {
            Set<NameId> dropped = new HashSet<>();
            dropped.add(source.id());
            for (NamedExpression p : project.projections()) {
                if (p instanceof Alias alias && alias.id().equals(source.id()) && alias.child() instanceof Attribute renamed) {
                    dropped.add(renamed.id());
                }
            }
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            projections.removeIf(p -> dropped.contains(p.toAttribute().id()));
            return new ProjectExec(project.source(), child, projections);
        }
        return child == unary.child() ? plan : unary.replaceChild(child);
    }

    private static Attribute doc(PhysicalPlan plan) {
        return plan.output().stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
    }
}
