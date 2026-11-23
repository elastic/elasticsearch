/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.UnaryExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Objects;

/**
 * A cast that can't convert its input, which is always {@code null}. The analyzer uses it instead of a {@code null} literal in
 * {@code STATS} when the input isn't a constant, as in {@code STATS x = COUNT(*)::ip} or {@code STATS x = f::ip BY f}: STATS
 * verifies its expressions by what they reference, so a constant isn't valid there, while an expression over the aggregation or
 * a grouping key is. The optimizer moves it out of the aggregation before replacing it with a {@code null} literal.
 * <p>
 * It can't be serialized, so one that survives the optimizer is reported by the plan verification, instead of failing later when the
 * plan is sent to the data nodes.
 */
public class NullCast extends UnaryExpression implements PostOptimizationVerificationAware {
    private final DataType dataType;

    public NullCast(Source source, Expression child, DataType dataType) {
        super(source, child);
        this.dataType = dataType;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("replaced on the coordinator");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("replaced on the coordinator");
    }

    @Override
    protected NullCast replaceChild(Expression newChild) {
        return new NullCast(source(), newChild, dataType);
    }

    @Override
    protected NodeInfo<NullCast> info() {
        return NodeInfo.create(this, NullCast::new, child(), dataType);
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        failures.add(Failure.fail(this, "[{}] was not replaced with null by the optimizer", sourceText()));
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataType);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && dataType == ((NullCast) obj).dataType;
    }
}
