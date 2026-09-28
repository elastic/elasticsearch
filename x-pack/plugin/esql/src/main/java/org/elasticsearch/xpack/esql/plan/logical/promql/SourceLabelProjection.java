/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Physical fallback for projecting an unchanged source label record. Only the shard's metadata loader knows the
 * target of a mapping alias; a JSON edit on the coordinator cannot infer it from the requested name. Keep the original
 * record wherever it is still needed (for example, to rank whole series), and carry the source projection alongside it.
 * This is not additional logical label state: the caller replaces its one current record with the returned attribute.
 * A computed record must never take this path, because rereading source would resurrect labels changed by the query.
 */
final class SourceLabelProjection {
    private SourceLabelProjection() {}

    record Projection(LogicalPlan plan, Attribute attribute) {}

    /** Returns null when the record is not an unchanged source grouping key. */
    @Nullable
    static Projection excluding(LogicalPlan plan, Attribute record, Set<String> excluded) {
        if (plan.output().stream().noneMatch(a -> a.id().equals(record.id()))) return null;
        if (plan instanceof EsRelation relation) {
            for (Attribute attribute : relation.output()) {
                if (attribute.id().equals(record.id()) && attribute instanceof TimeSeriesMetadataAttribute stored) {
                    var fields = new LinkedHashSet<>(stored.excludedFields());
                    if (fields.addAll(excluded) == false) return new Projection(plan, record);
                    var projected = new TimeSeriesMetadataAttribute(record.source(), fields);
                    return new Projection(relation.withAdditionalAttributes(List.of(projected)), projected);
                }
            }
            return null;
        }
        if (plan instanceof UnpackDims unpack && unpack.dims().stream().anyMatch(a -> a.id().equals(record.id()))) {
            // A regroup packs its keys to preserve multivalued dimensions. Extend the matching pack/unpack pair;
            // adding an unrelated column above the aggregate would lose it at that pipeline boundary.
            if (unpack.child() instanceof Aggregate aggregate && aggregate.child() instanceof PackDims pack) {
                boolean samePacking = aggregate.groupings()
                    .stream()
                    .anyMatch(
                        g -> g instanceof Alias alias && alias.id().equals(unpack.packed().id()) && alias.child().equals(pack.packed())
                    );
                if (samePacking == false || pack.dims().stream().noneMatch(a -> a.id().equals(record.id()))) {
                    return null;
                }
                Projection projected = excluding(pack.child(), record, excluded);
                if (projected == null) return null;
                if (projected.plan() == pack.child()) return new Projection(plan, record);
                var packedDims = new ArrayList<>(pack.dims());
                packedDims.add(projected.attribute());
                var unpackedDims = new ArrayList<>(unpack.dims());
                unpackedDims.add(projected.attribute());
                var packed = new PackDims(pack.source(), projected.plan(), packedDims, pack.packed());
                return new Projection(
                    new UnpackDims(unpack.source(), aggregate.replaceChild(packed), unpack.packed(), unpackedDims),
                    projected.attribute()
                );
            }
            return null;
        }
        if (plan instanceof Aggregate aggregate) {
            for (Expression grouping : aggregate.groupings()) {
                if (grouping instanceof NamedExpression named && named.id().equals(record.id())) {
                    if (Alias.unwrap(grouping) instanceof Attribute input) {
                        Projection projected = excluding(aggregate.child(), input, excluded);
                        if (projected == null) return null;
                        if (projected.plan() == aggregate.child()) return new Projection(plan, record);
                        Alias key = new Alias(record.source(), projected.attribute().name(), projected.attribute());
                        var groupings = new ArrayList<>(aggregate.groupings());
                        groupings.add(key);
                        var outputs = new ArrayList<NamedExpression>(aggregate.aggregates());
                        outputs.add(key.toAttribute());
                        return new Projection(aggregate.with(projected.plan(), groupings, outputs), key.toAttribute());
                    }
                }
            }
            return null;
        }
        if (plan instanceof Eval eval) {
            for (Alias field : eval.fields()) {
                if (field.id().equals(record.id())) {
                    if (field.child() instanceof Attribute input) {
                        Projection projected = excluding(eval.child(), input, excluded);
                        return passThrough(eval, eval.child(), record, projected);
                    }
                    return null;
                }
            }
            Projection projected = excluding(eval.child(), record, excluded);
            return passThrough(eval, eval.child(), record, projected);
        }
        if (plan instanceof Project project) {
            Projection projected = excluding(project.child(), record, excluded);
            if (projected == null) return null;
            if (projected.plan() == project.child()) return new Projection(plan, record);
            var outputs = new ArrayList<NamedExpression>(project.projections());
            outputs.add(projected.attribute());
            return new Projection(new Project(project.source(), projected.plan(), outputs), projected.attribute());
        }
        if (plan instanceof Filter filter) {
            Projection projected = excluding(filter.child(), record, excluded);
            return passThrough(filter, filter.child(), record, projected);
        }
        if (plan instanceof TopNBy top) {
            Projection projected = excluding(top.child(), record, excluded);
            return passThrough(top, top.child(), record, projected);
        }
        return null;
    }

    private static Projection passThrough(LogicalPlan plan, LogicalPlan child, Attribute record, @Nullable Projection projected) {
        if (projected == null) return null;
        return projected.plan() == child
            ? new Projection(plan, record)
            : new Projection(plan.replaceChildren(List.of(projected.plan())), projected.attribute());
    }
}
