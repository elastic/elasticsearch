/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.predicate.operator;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.types.PromqlDataTypes;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

public abstract class VectorBinaryOperator extends Expression {

    private final Expression left, right;
    private final VectorMatch match;
    private final boolean dropMetricName;

    private DataType dataType;

    private BinaryOp binaryOp;

    /**
     * Underlying binary operation (e.g. +, -, *, /, etc.) being performed
     * on the actual values of the vectors.
     */
    public interface BinaryOp {
        String name();

        ScalarFunctionFactory asFunction();
    }

    public interface ScalarFunctionFactory {
        Function create(Source source, Expression left, Expression right);
    }

    protected VectorBinaryOperator(
        Source source,
        Expression left,
        Expression right,
        VectorMatch match,
        boolean dropMetricName,
        BinaryOp binaryOp
    ) {
        super(source, asList(left, right));
        this.left = left;
        this.right = right;
        this.match = match;
        this.dropMetricName = dropMetricName;
        this.binaryOp = binaryOp;
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    public VectorMatch match() {
        return match;
    }

    public boolean dropMetricName() {
        return dropMetricName;
    }

    public BinaryOp binaryOp() {
        return binaryOp;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            dataType = PromqlDataTypes.operationType(left.dataType(), right.dataType());
        }
        return dataType;
    }

    @Override
    public VectorBinaryOperator replaceChildren(List<Expression> newChildren) {
        return replaceChildren(left, right);
    }

    protected abstract VectorBinaryOperator replaceChildren(Expression left, Expression right);

    @Override
    public boolean foldable() {
        return left.foldable() && right.foldable();
    }

    @Override
    public Object fold(FoldContext ctx) {
        return binaryOp.asFunction().create(source(), left(), right()).fold(ctx);
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            VectorBinaryOperator that = (VectorBinaryOperator) o;
            return dropMetricName == that.dropMetricName && Objects.equals(match, that.match) && Objects.equals(binaryOp, that.binaryOp);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, match, dropMetricName, binaryOp);
    }

    public String getWriteableName() {
        throw new EsqlIllegalArgumentException("should not be serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new EsqlIllegalArgumentException("should not be serialized");
    }
}
