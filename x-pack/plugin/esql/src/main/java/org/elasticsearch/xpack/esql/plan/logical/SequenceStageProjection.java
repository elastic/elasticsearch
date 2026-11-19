/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Derives the exact raw source projection for a sequence stage without physical planning or I/O. The same branch
 * binding and pruning are used by verification and execution, including union ID remapping, hidden source identity
 * and membership predicates whose right sides are independent queries rather than event streams.
 */
public final class SequenceStageProjection {
    private SequenceStageProjection() {}

    /** Only exact source searches can be removed from the coordinator, which has no index to recheck them. */
    public static boolean isExactSourceSearchPredicate(Filter filter, Expression predicate) {
        EsRelation source = exactSearchSource(filter.child());
        return source != null
            && predicate instanceof FullTextFunction function
            && function.isRuntimeSearch() == false
            && function instanceof Knn == false
            && source.outputSet().containsAll(predicate.references())
            && predicate.references().stream().noneMatch(MetadataAttribute::isScoreAttribute)
            && TranslationAware.translatable(predicate, LucenePushdownPredicates.DEFAULT)
                .finish() == TranslationAware.FinishedTranslatable.YES;
    }

    private static EsRelation exactSearchSource(LogicalPlan plan) {
        if (plan instanceof EsRelation relation) {
            return relation;
        }
        if (plan instanceof Project project
            && project.child() instanceof Eval eval
            && eval.child() instanceof EsRelation relation
            && project.source().equals(relation.source())
            && eval.source().equals(relation.source())
            && eval.fields().isEmpty() == false
            && eval.fields().stream().allMatch(alias -> switch (alias.name()) {
                case MetadataAttribute.RELATION_CLASS -> alias.child() instanceof Literal literal
                    && new BytesRef(relation.relationClass().value()).equals(literal.value());
                case MetadataAttribute.RELATION_NAME -> alias.child() instanceof MetadataAttribute name
                    && name.name().equals(MetadataAttribute.INDEX);
                default -> false;
            })) {
            return relation;
        }
        return null;
    }

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
     * Includes mandatory values without widening a stage's public output. An own condition input is resolved at
     * its WHERE boundary, so a trailing projection may remove its public name while selection still needs its value.
     */
    public static List<Attribute> selectionOutput(SequenceStage stage, List<Attribute> payloads, List<Attribute> mandatory) {
        if (mandatory.isEmpty()) {
            return visibleOutput(stage, payloads);
        }
        List<Attribute> publicPayloads = new ArrayList<>(payloads);
        for (Attribute attribute : mandatory) {
            Attribute current = find(stage.output(), attribute);
            if (current != null) {
                publicPayloads.add(current);
            }
        }
        List<Attribute> result = new ArrayList<>(visibleOutput(stage, publicPayloads));
        Set<NameId> included = new HashSet<>();
        result.forEach(attribute -> included.add(attribute.id()));
        Set<NameId> demanded = new HashSet<>();
        mandatory.forEach(attribute -> demanded.add(attribute.id()));
        List<Attribute> own = new ArrayList<>();
        Set<NameId> ownIds = new HashSet<>();
        stage.child().forEachDown(SequenceCondition.class, condition -> {
            for (SequenceCondition.Input input : condition.activeInputs()) {
                if (input.stage() == condition.ownStage() && demanded.contains(input.source().id()) && ownIds.add(input.source().id())) {
                    own.add(canonical(condition.child().output(), input.source()));
                }
            }
        });
        for (Attribute attribute : mandatory) {
            Attribute current = find(stage.output(), attribute);
            if (current == null) {
                current = canonical(own, attribute);
            }
            if (included.add(current.id())) {
                result.add(current);
            }
        }
        return List.copyOf(result);
    }

    private static Attribute canonical(List<Attribute> output, Attribute expected) {
        Attribute current = find(output, expected);
        if (current == null) {
            throw new IllegalStateException("SEQUENCE condition input lost column [" + expected.name() + "#" + expected.id() + "]");
        }
        return current;
    }

    private static Attribute find(List<Attribute> output, Attribute expected) {
        Attribute result = null;
        for (Attribute current : output) {
            if (current.id().equals(expected.id())) {
                if (result != null) {
                    throw new IllegalStateException("SEQUENCE condition input has duplicate column identity [" + expected.id() + "]");
                }
                if (current.dataType() != expected.dataType()) {
                    throw new IllegalStateException("SEQUENCE condition input changed type of column [" + expected.name() + "]");
                }
                result = current;
            }
        }
        return result;
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
        return prepare(stage, payloads, List.of(), selected, marker, supplier);
    }

    /** Keeps condition-only own values private while removing deferred conditions from ordinary row execution. */
    public static Prepared prepare(
        SequenceStage stage,
        List<Attribute> payloads,
        List<Attribute> mandatory,
        EsRelation selected,
        Attribute marker,
        LocalSupplier supplier
    ) {
        Set<NameId> demanded = new HashSet<>();
        mandatory.forEach(attribute -> demanded.add(attribute.id()));
        BoundPlan bound = bindPlan(stage.child(), selected, marker, supplier, demanded);
        if (bound.active == false) {
            throw new IllegalArgumentException("selected sequence source does not belong to its event stream");
        }
        List<NamedExpression> projections = new ArrayList<>(selectionOutput(stage, payloads, mandatory));
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

    private static BoundPlan bindPlan(
        LogicalPlan plan,
        EsRelation selected,
        Attribute marker,
        LocalSupplier supplier,
        Set<NameId> demanded
    ) {
        if (plan instanceof SequenceCondition condition) {
            BoundPlan child = bindPlan(condition.child(), selected, marker, supplier, demanded);
            List<Attribute> values = new ArrayList<>(child.conditionValues);
            Set<NameId> included = new HashSet<>();
            values.forEach(attribute -> included.add(attribute.id()));
            for (SequenceCondition.Input input : condition.activeInputs()) {
                if (input.stage() == condition.ownStage() && demanded.contains(input.source().id()) && included.add(input.source().id())) {
                    values.add(canonical(child.plan.output(), input.source()));
                }
            }
            return new BoundPlan(child.plan, child.active, List.copyOf(values));
        }
        if (plan instanceof EsRelation relation) {
            List<Attribute> attributes = new ArrayList<>(relation.output());
            attributes.add(marker);
            boolean active = relation == selected;
            return new BoundPlan(
                new LocalRelation(relation.source(), attributes, active ? supplier : EmptyLocalSupplier.EMPTY),
                active,
                List.of()
            );
        }
        if (plan instanceof AbstractSubqueryJoin join) {
            BoundPlan left = bindPlan(join.left(), selected, marker, supplier, demanded);
            return new BoundPlan(join.replaceChildren(List.of(left.plan, join.right())), left.active, left.conditionValues);
        }
        if (plan instanceof LocalRelation relation && relation.hasEmptySupplier() == false) {
            // Prepared membership tables keep their original schemas and suppliers across page bindings.
            return new BoundPlan(relation, false, List.of());
        }
        List<LogicalPlan> children = new ArrayList<>();
        LogicalPlan activeChild = null;
        boolean active = false;
        List<Attribute> conditionValues = List.of();
        for (LogicalPlan child : plan.children()) {
            BoundPlan bound = bindPlan(child, selected, marker, supplier, demanded);
            children.add(bound.plan);
            if (bound.active) {
                if (activeChild != null) {
                    throw new IllegalStateException("a sequence input page cannot belong to multiple source branches");
                }
                activeChild = bound.plan;
                active = true;
                conditionValues = bound.conditionValues;
            }
        }
        if (plan instanceof UnionAll union) {
            List<Attribute> attributes = new ArrayList<>(union.output());
            attributes.add(marker);
            if (active == false) {
                return new BoundPlan(new LocalRelation(union.source(), attributes, EmptyLocalSupplier.EMPTY), false, List.of());
            }
            List<NamedExpression> projections = new ArrayList<>();
            for (int column = 0; column < union.output().size(); column++) {
                Attribute desired = union.output().get(column);
                Attribute actual = activeChild.output().get(column);
                projections.add(new Alias(desired.source(), desired.name(), actual, desired.id(), desired.synthetic()));
            }
            projections.add(marker);
            return new BoundPlan(new Project(union.source(), activeChild, projections), true, conditionValues);
        }
        if (plan instanceof LocalRelation relation && relation.hasEmptySupplier()) {
            List<Attribute> attributes = new ArrayList<>(relation.output());
            attributes.add(marker);
            return new BoundPlan(new LocalRelation(relation.source(), attributes, EmptyLocalSupplier.EMPTY), false, List.of());
        }
        LogicalPlan result = plan.replaceChildren(children);
        if (active && plan instanceof Filter filter && exactSearchSource(filter.child()) == selected) {
            List<Expression> conjuncts = Predicates.splitAnd(filter.condition());
            List<Expression> remaining = conjuncts.stream()
                .filter(predicate -> isExactSourceSearchPredicate(filter, predicate) == false)
                .toList();
            if (remaining.size() != conjuncts.size()) {
                result = remaining.isEmpty()
                    ? children.getFirst()
                    : new Filter(filter.source(), children.getFirst(), Predicates.combineAnd(remaining));
            }
        }
        if (result instanceof Project project) {
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            Set<NameId> included = new HashSet<>();
            projections.forEach(expression -> included.add(expression.id()));
            for (Attribute value : conditionValues) {
                if (included.add(value.id())) {
                    projections.add(value);
                }
            }
            if (included.add(marker.id())) {
                projections.add(marker);
            }
            result = project.withProjections(projections);
        }
        return new BoundPlan(result, active, conditionValues);
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

    private record BoundPlan(LogicalPlan plan, boolean active, List<Attribute> conditionValues) {}

    /** Carries the compact raw schema beside the logical row operators that consume it. */
    public record Prepared(LogicalPlan plan, EsRelation source) {}
}
