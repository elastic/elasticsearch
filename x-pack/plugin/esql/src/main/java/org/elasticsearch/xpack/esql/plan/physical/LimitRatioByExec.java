/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for {@code limit_ratio(r, v)} per group.
 * <p>
 * A pure row filter: it drops rows but neither adds nor removes columns, so it does not
 * participate in row-size estimation.
 */
public class LimitRatioByExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "LimitRatioByExec",
        LimitRatioByExec::readFrom
    );

    private final Expression ratio;
    private final List<Expression> groupings;

    public LimitRatioByExec(Source source, PhysicalPlan child, Expression ratio, List<Expression> groupings) {
        super(source, child);
        this.ratio = ratio;
        this.groupings = groupings;
    }

    private static LimitRatioByExec readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        PhysicalPlan child = in.readNamedWriteable(PhysicalPlan.class);
        Expression ratio = in.readNamedWriteable(Expression.class);
        List<Expression> groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        return new LimitRatioByExec(source, child, ratio, groupings);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(ratio());
        out.writeNamedWriteableCollection(groupings());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends LimitRatioByExec> info() {
        return NodeInfo.create(this, LimitRatioByExec::new, child(), ratio, groupings);
    }

    @Override
    public LimitRatioByExec replaceChild(PhysicalPlan newChild) {
        return new LimitRatioByExec(source(), newChild, ratio, groupings);
    }

    public Expression ratio() {
        return ratio;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ratio, groupings, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LimitRatioByExec other = (LimitRatioByExec) obj;
        return Objects.equals(ratio, other.ratio) && Objects.equals(groupings, other.groupings) && Objects.equals(child(), other.child());
    }
}
