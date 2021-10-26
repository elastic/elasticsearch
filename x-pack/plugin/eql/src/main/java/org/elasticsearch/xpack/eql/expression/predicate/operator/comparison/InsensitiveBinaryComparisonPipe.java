/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveBinaryComparisonProcessor.InsensitiveBinaryComparisonOperation;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class InsensitiveBinaryComparisonPipe extends BinaryPipe {

    private final InsensitiveBinaryComparisonOperation operation;

    public InsensitiveBinaryComparisonPipe(Source source, Expression expression, Pipe left, Pipe right,
                                           InsensitiveBinaryComparisonOperation operation) {
        super(source, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<InsensitiveBinaryComparisonPipe> info() {
        return NodeInfo.create(this, InsensitiveBinaryComparisonPipe::new, expression(), left(), right(), operation);
    }

    @Override
    protected InsensitiveBinaryComparisonPipe replaceChildren(Pipe left, Pipe right) {
        return new InsensitiveBinaryComparisonPipe(source(), expression(), left, right, operation);
    }

    @Override
    public InsensitiveBinaryComparisonProcessor asProcessor() {
        return new InsensitiveBinaryComparisonProcessor(left().asProcessor(), right().asProcessor(), operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InsensitiveBinaryComparisonPipe other = (InsensitiveBinaryComparisonPipe) obj;
            return Objects.equals(operation, other.operation);
        }
        return false;
    }
}
