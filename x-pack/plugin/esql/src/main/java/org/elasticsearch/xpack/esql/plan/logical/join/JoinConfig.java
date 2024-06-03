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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;

public record JoinConfig(JoinType type, List<NamedExpression> matchFields) implements Writeable {
    public JoinConfig(StreamInput in) throws IOException {
        this(JoinType.readFrom(in), in.readCollectionAsList(i -> ((PlanStreamInput) i).readNamedExpression()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        type.writeTo(out);
        out.writeCollection(unionFields, (o, v) -> ((PlanStreamOutput) o).writeNamedExpression(v));
    }
}
