/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Reads a logical label in an ordinary scalar expression. Metadata-backed reads lower to {@code TS_GET} and the
 * label's native conversion; existing columns remain visible to time-series aggregation without a wrapper.
 */
public final class LabelEval extends EsqlConfigurationFunction implements OnlySurrogateExpression {
    private final DataType resultType;

    private LabelEval(Source source, Expression input, Expression label, DataType resultType, Configuration configuration) {
        super(source, List.of(input, label), configuration);
        if (resultType != DataType.KEYWORD && EsqlDataTypeConverter.converterFunctionFactory(resultType) == null) {
            throw new IllegalArgumentException("Unsupported label type [" + resultType + "]");
        }
        this.resultType = resultType;
    }

    /** Avoids hiding native attributes from aggregation analysis; missing named labels are supplied as typed nulls. */
    public static Expression create(Source source, Expression input, String label, DataType resultType, Configuration configuration) {
        if (input.dataType() != DataType.TIMESERIES_DATA_TYPE) {
            if (input.dataType() != resultType) {
                throw new IllegalArgumentException("Conflicting types for label [" + label + "]");
            }
            return input;
        }
        if (label == null || label.isEmpty()) {
            throw new IllegalArgumentException("Label names must be non-empty");
        }
        return new LabelEval(source, input, Literal.keyword(source, label), resultType, configuration);
    }

    @Override
    public DataType dataType() {
        return resultType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return new TimeSeriesGet(source(), children().getFirst(), children().get(1)).typeResolved();
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public boolean foldable() {
        return children().stream().allMatch(Expression::foldable);
    }

    @Override
    public Object fold(FoldContext context) {
        return surrogate().fold(context);
    }

    @Override
    public Expression surrogate() {
        Expression value = new TimeSeriesGet(source(), children().getFirst(), children().get(1));
        return resultType == DataType.KEYWORD
            ? value
            : EsqlDataTypeConverter.converterFunctionFactory(resultType).apply(source(), value, configuration());
    }

    @Override
    public Expression replaceChildren(List<Expression> children) {
        return new LabelEval(source(), children.getFirst(), children.get(1), resultType, configuration());
    }

    @Override
    protected NodeInfo<LabelEval> info() {
        return NodeInfo.create(this, LabelEval::new, children().getFirst(), children().get(1), resultType, configuration());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultType);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other) && resultType == ((LabelEval) other).resultType;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("LabelEval must be replaced by its surrogate before evaluation");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("LabelEval must be replaced by its surrogate before serialization");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("LabelEval must be replaced by its surrogate before serialization");
    }
}
