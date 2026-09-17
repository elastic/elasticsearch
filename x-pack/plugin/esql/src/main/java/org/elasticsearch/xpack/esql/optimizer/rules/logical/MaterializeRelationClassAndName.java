/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.ClassifiedAs;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

/**
 * Answer {@code _class} and {@code _name} on the relation the row came from, rather than on the node
 * that reads the data.
 * <p>
 * Both are properties of the relation and are known once it resolves, so they are materialised here
 * instead of at a shard or a file. {@code _class} is the same for every row of a relation and folds
 * to a literal the relation itself supplies through {@link ClassifiedAs}. {@code _name} is a literal
 * for a dataset, which has exactly one name, but not for an index: an {@link EsRelation} covers an
 * index pattern, so a row's name is the concrete index it came from, which is what {@code _index}
 * already reports — {@code _name} aliases that attribute.
 * <p>
 * The resulting shape, with the materialised names dropped from the relation so nothing downstream
 * is asked to read a column that no data source has:
 * <pre>
 * Project[_class, _name, a, b]
 * \_Eval[_class = "index", _name = _index]
 *   \_EsRelation[a, b, _index]
 * </pre>
 * <p>
 * The {@code Eval} has to be its own node: {@code Project.validateProjections} requires an
 * {@code Alias} in a projection list to have an {@code Attribute} child, so a literal cannot live in
 * the {@code Project}.
 * <p>
 * That has a measured cost under a {@code UnionAll}. {@code PushDownUtils.isLeafUnionAll} accepts only a
 * relation or a {@code Project} directly over one, and it uses the absence of an {@code Eval} to tell the
 * heterogeneous-FROM shape apart from the subquery shape {@code Project > Eval? > Subquery}. So asking for
 * either column stops {@code PushAggregateThroughUnionAll} firing: the aggregate stays above the union
 * instead of decomposing into a partial per branch. {@code RelationClassGoldenTests} pins both plans side
 * by side. Widening that predicate would have to keep the distinction it was written to make, so it is not
 * done here. A query that asks for neither column is returned untouched and keeps every pushdown.
 */
public final class MaterializeRelationClassAndName extends OptimizerRules.OptimizerRule<LeafPlan> {

    @Override
    protected LogicalPlan rule(LeafPlan plan) {
        if (plan instanceof ClassifiedAs == false) {
            return plan;
        }
        ClassifiedAs classified = (ClassifiedAs) plan;
        List<Attribute> output = plan.output();
        if (output.stream().noneMatch(MaterializeRelationClassAndName::isRelationColumn)) {
            return plan;
        }

        // _name on an index needs _index to point at, so add it when the query did not ask for it.
        // It is left out of the projection below, so adding it does not widen what the user sees.
        LeafPlan relation = plan;
        Attribute indexAttribute = firstNamed(output, MetadataAttribute.INDEX);
        if (indexAttribute == null
            && firstNamed(output, MetadataAttribute.RELATION_NAME) != null
            && plan instanceof EsRelation esRelation) {
            indexAttribute = (Attribute) MetadataAttribute.create(plan.source(), MetadataAttribute.INDEX);
            relation = esRelation.withAdditionalAttribute(indexAttribute);
        }

        List<Alias> values = new ArrayList<>(2);
        List<NamedExpression> projections = new ArrayList<>(output.size());
        for (Attribute attr : output) {
            Expression value = valueOf(attr, classified, indexAttribute);
            if (value == null) {
                projections.add(attr);
            } else {
                // Reuse the attribute's own id so nothing downstream of the relation needs rewriting.
                Alias alias = new Alias(attr.source(), attr.name(), value, attr.id());
                values.add(alias);
                projections.add(alias.toAttribute());
            }
        }

        List<Attribute> remaining = relation.output().stream().filter(a -> isRelationColumn(a) == false).toList();
        LeafPlan stripped = switch (relation) {
            case EsRelation esRelation -> esRelation.withAttributes(remaining);
            case ExternalRelation externalRelation -> externalRelation.withAttributes(remaining);
            default -> throw new IllegalStateException("unhandled " + ClassifiedAs.class.getSimpleName() + ": " + relation.nodeName());
        };

        return new Project(plan.source(), new Eval(plan.source(), stripped, values), projections);
    }

    /**
     * The value the relation answers for this column, or null when the column is not one of ours.
     */
    private static Expression valueOf(Attribute attr, ClassifiedAs classified, Attribute indexAttribute) {
        return switch (attr.name()) {
            case MetadataAttribute.RELATION_CLASS -> Literal.keyword(attr.source(), classified.relationClass().value());
            case MetadataAttribute.RELATION_NAME -> relationName(attr, classified, indexAttribute);
            default -> null;
        };
    }

    private static Expression relationName(Attribute attr, ClassifiedAs classified, Attribute indexAttribute) {
        // A dataset has one name, which is unknown when a bare glob named no dataset.
        if (classified instanceof ExternalRelation externalRelation) {
            String datasetName = externalRelation.datasetName();
            return datasetName == null ? new Literal(attr.source(), null, DataType.KEYWORD) : Literal.keyword(attr.source(), datasetName);
        }
        return indexAttribute;
    }

    private static boolean isRelationColumn(Attribute attr) {
        return MetadataAttribute.RELATION_CLASS.equals(attr.name()) || MetadataAttribute.RELATION_NAME.equals(attr.name());
    }

    private static Attribute firstNamed(List<Attribute> attributes, String name) {
        return attributes.stream().filter(a -> name.equals(a.name())).findFirst().orElse(null);
    }
}
