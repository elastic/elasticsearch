/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;

public abstract class InsensitiveBinaryComparison extends BinaryScalarFunction {

    protected InsensitiveBinaryComparison(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    protected InsensitiveBinaryComparison(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), ((PlanStreamInput) in).readExpression(), ((PlanStreamInput) in).readExpression());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        ((PlanStreamOutput) out).writeExpression(left());
        ((PlanStreamOutput) out).writeExpression(right());
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

}
