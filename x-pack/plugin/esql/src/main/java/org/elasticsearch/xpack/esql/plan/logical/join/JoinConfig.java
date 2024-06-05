/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Configuration for a {@code JOIN} style operation.
 * @param matchFields fields that are merged from the left and right relations
 * @param conditions when these conditions are true the rows are joined
 */
public record JoinConfig(JoinType type, List<NamedExpression> matchFields, List<Expression> conditions) implements Writeable {
    public JoinConfig(StreamInput in) throws IOException {
        this(
            JoinType.readFrom(in),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            in.readCollectionAsList(i -> ((PlanStreamInput) i).readExpression())
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        type.writeTo(out);
        out.writeNamedWriteableCollection(matchFields);
        out.writeCollection(conditions, (o, v) -> ((PlanStreamOutput) o).writeExpression(v));
    }

    public boolean expressionsResolved() {
        return Resolvables.resolved(matchFields) && Resolvables.resolved(conditions);
    }
}
