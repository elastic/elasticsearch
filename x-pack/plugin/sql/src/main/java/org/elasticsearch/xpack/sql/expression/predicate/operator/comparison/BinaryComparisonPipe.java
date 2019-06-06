/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Objects;

public class BinaryComparisonPipe extends BinaryPipe {

    private final BinaryComparisonOperation operation;

    public BinaryComparisonPipe(Source source, Expression expression, Pipe left, Pipe right, BinaryComparisonOperation operation) {
        super(source, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<BinaryComparisonPipe> info() {
        return NodeInfo.create(this, BinaryComparisonPipe::new, expression(), left(), right(), operation);
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new BinaryComparisonPipe(source(), expression(), left, right, operation);
    }

    @Override
    public BinaryComparisonProcessor asProcessor() {
        return new BinaryComparisonProcessor(left().asProcessor(), right().asProcessor(), operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            BinaryComparisonPipe other = (BinaryComparisonPipe) obj;
            return Objects.equals(operation, other.operation);
        }
        return false;
    }
}
