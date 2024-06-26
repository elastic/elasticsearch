/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Looks up values from the associated {@code tables}.
 * The class is supposed to be substituted by a {@link Join}.
 */
public class Lookup extends UnaryPlan {
    private final Expression tableName;
    /**
     * References to the input fields to match against the {@link #localRelation}.
     */
    private final List<Attribute> matchFields;
    // initialized during the analysis phase for output and validation
    // afterward, it is converted into a Join (BinaryPlan) hence why here it is not a child
    private final LocalRelation localRelation;
    private List<Attribute> lazyOutput;

    public Lookup(
        Source source,
        LogicalPlan child,
        Expression tableName,
        List<Attribute> matchFields,
        @Nullable LocalRelation localRelation
    ) {
        super(source, child);
        this.tableName = tableName;
        this.matchFields = matchFields;
        this.localRelation = localRelation;
    }

    public Lookup(PlanStreamInput in) throws IOException {
        super(Source.readFrom(in), in.readLogicalPlanNode());
        this.tableName = in.readExpression();
        this.matchFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.localRelation = in.readBoolean() ? new LocalRelation(in) : null;
    }

    public void writeTo(PlanStreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeLogicalPlanNode(child());
        out.writeExpression(tableName);
        out.writeNamedWriteableCollection(matchFields);
        if (localRelation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            localRelation.writeTo(out);
        }
    }

    public Expression tableName() {
        return tableName;
    }

    public List<Attribute> matchFields() {
        return matchFields;
    }

    public LocalRelation localRelation() {
        return localRelation;
    }

    public JoinConfig joinConfig() {
        List<Expression> conditions = new ArrayList<>(matchFields.size());
        List<Attribute> rhsOutput = Join.makeReference(localRelation.output());
        for (NamedExpression lhs : matchFields) {
            for (Attribute rhs : rhsOutput) {
                if (lhs.name().equals(rhs.name())) {
                    conditions.add(new Equals(source(), lhs, rhs));
                    break;
                }
            }
        }
        return new JoinConfig(JoinType.LEFT, matchFields, conditions);
    }

    @Override
    public boolean expressionsResolved() {
        return tableName.resolved() && Resolvables.resolved(matchFields) && localRelation != null;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Lookup(source(), newChild, tableName, matchFields, localRelation);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Lookup::new, child(), tableName, matchFields, localRelation);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            if (localRelation == null) {
                throw new IllegalStateException("Cannot determine output of LOOKUP with unresolved table");
            }
            lazyOutput = Join.computeOutput(child().output(), localRelation.output(), joinConfig());
        }
        return lazyOutput;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        Lookup lookup = (Lookup) o;
        return Objects.equals(tableName, lookup.tableName)
            && Objects.equals(matchFields, lookup.matchFields)
            && Objects.equals(localRelation, lookup.localRelation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tableName, matchFields, localRelation);
    }
}
