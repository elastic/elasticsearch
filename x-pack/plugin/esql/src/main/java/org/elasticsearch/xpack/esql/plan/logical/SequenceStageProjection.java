/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.ArrayList;
import java.util.List;

/**
 * Derives the exact raw source projection for a sequence stage without physical planning or I/O. The same branch
 * binding and pruning are used by verification and execution, including union ID remapping, hidden source identity
 * and membership predicates whose right sides are independent queries rather than event streams.
 */
public final class SequenceStageProjection {
    private SequenceStageProjection() {}

    /** Returns only event sources; membership subquery sources retain their ordinary ES|QL execution. */
    public static List<EsRelation> eventSources(SequenceStage stage) {
        List<EsRelation> sources = new ArrayList<>();
        collectSources(stage.child(), sources);
        return List.copyOf(sources);
    }

    /** Retains stage output order while including every correlation key needed independently of visible payloads. */
    public static List<Attribute> visibleOutput(SequenceStage stage, List<Attribute> payloads) {
        if (stage.outputSet().containsAll(payloads) == false) {
            throw new IllegalArgumentException("sequence payloads must belong to their stage output");
        }
        AttributeSet wanted = AttributeSet.of(payloads).combine(AttributeSet.of(stage.keys()));
        return stage.output().stream().filter(wanted::contains).toList();
    }

    /**
     * Binds one event branch to a local input and prunes its unused raw fields. The marker survives every projection
     * even when no payloads are retained, so event identity and cardinality remain available to matching.
     */
    public static Prepared prepare(
        SequenceStage stage,
        List<Attribute> payloads,
        EsRelation selected,
        Attribute marker,
        LocalSupplier supplier
    ) {
        BoundPlan bound = bindPlan(stage.child(), selected, marker, supplier);
        if (bound.active == false) {
            throw new IllegalArgumentException("selected sequence source does not belong to its event stream");
        }
        List<NamedExpression> projections = new ArrayList<>(visibleOutput(stage, payloads));
        projections.add(marker);
        LogicalPlan logical = new PruneColumns().apply(new Project(Source.EMPTY, bound.plan, projections));
        var references = AttributeSet.builder();
        collectReferences(logical, references);
        AttributeSet used = references.build();
        // Ordinary fragments trim fields through InsertFieldExtraction; these local sources need the projection
        // before their input page is allocated, including stages used only by COUNT(*).
        logical = logical.transformUp(LocalRelation.class, relation -> {
            return relation.supplier() == supplier && relation.outputSet().contains(marker)
                ? new LocalRelation(relation.source(), relation.output().stream().filter(used::contains).toList(), supplier)
                : relation;
        });
        return new Prepared(logical, selected.withAttributes(selected.output().stream().filter(used::contains).toList()));
    }

    private static BoundPlan bindPlan(LogicalPlan plan, EsRelation selected, Attribute marker, LocalSupplier supplier) {
        if (plan instanceof EsRelation relation) {
            List<Attribute> attributes = new ArrayList<>(relation.output());
            attributes.add(marker);
            boolean active = relation == selected;
            return new BoundPlan(new LocalRelation(relation.source(), attributes, active ? supplier : EmptyLocalSupplier.EMPTY), active);
        }
        if (plan instanceof AbstractSubqueryJoin join) {
            BoundPlan left = bindPlan(join.left(), selected, marker, supplier);
            return new BoundPlan(join.replaceChildren(List.of(left.plan, join.right())), left.active);
        }
        if (plan instanceof LocalRelation relation && relation.hasEmptySupplier() == false) {
            // Prepared membership tables keep their original schemas and suppliers across page bindings.
            return new BoundPlan(relation, false);
        }
        List<LogicalPlan> children = new ArrayList<>();
        LogicalPlan activeChild = null;
        boolean active = false;
        for (LogicalPlan child : plan.children()) {
            BoundPlan bound = bindPlan(child, selected, marker, supplier);
            children.add(bound.plan);
            if (bound.active) {
                if (activeChild != null) {
                    throw new IllegalStateException("a sequence input page cannot belong to multiple source branches");
                }
                activeChild = bound.plan;
                active = true;
            }
        }
        if (plan instanceof UnionAll union) {
            List<Attribute> attributes = new ArrayList<>(union.output());
            attributes.add(marker);
            if (active == false) {
                return new BoundPlan(new LocalRelation(union.source(), attributes, EmptyLocalSupplier.EMPTY), false);
            }
            List<NamedExpression> projections = new ArrayList<>();
            for (int column = 0; column < union.output().size(); column++) {
                Attribute desired = union.output().get(column);
                Attribute actual = activeChild.output().get(column);
                projections.add(new Alias(desired.source(), desired.name(), actual, desired.id(), desired.synthetic()));
            }
            projections.add(marker);
            return new BoundPlan(new Project(union.source(), activeChild, projections), true);
        }
        if (plan instanceof LocalRelation relation && relation.hasEmptySupplier()) {
            List<Attribute> attributes = new ArrayList<>(relation.output());
            attributes.add(marker);
            return new BoundPlan(new LocalRelation(relation.source(), attributes, EmptyLocalSupplier.EMPTY), false);
        }
        LogicalPlan result = plan.replaceChildren(children);
        if (result instanceof Project project) {
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            projections.add(marker);
            result = project.withProjections(projections);
        }
        return new BoundPlan(result, active);
    }

    private static void collectSources(LogicalPlan plan, List<EsRelation> sources) {
        if (plan instanceof EsRelation relation) {
            sources.add(relation);
        } else if (plan instanceof AbstractSubqueryJoin join) {
            collectSources(join.left(), sources);
        } else {
            plan.children().forEach(child -> collectSources(child, sources));
        }
    }

    private static void collectReferences(LogicalPlan plan, AttributeSet.Builder references) {
        references.addAll(plan.references());
        if (plan instanceof AbstractSubqueryJoin join) {
            collectReferences(join.left(), references);
        } else {
            plan.children().forEach(child -> collectReferences(child, references));
        }
    }

    private record BoundPlan(LogicalPlan plan, boolean active) {}

    /** Carries the compact raw schema beside the logical row operators that consume it. */
    public record Prepared(LogicalPlan plan, EsRelation source) {}
}
