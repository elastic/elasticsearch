/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.knn;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KnnQueryPredicate extends Expression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "KnnQueryPredicate",
        KnnQueryPredicate::new
    );

    private final float[] vectorData;

    private final Map<String, Object> options;

    private final Expression field;

    KnnQueryPredicate(StreamInput in) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readFloatArray(),
            in.readGenericMap()
        );
    }

    public KnnQueryPredicate(Source source, Expression field, float[] vectorData, Map<String, Object> options) {
        super(source, Collections.singletonList(field));
        this.vectorData = vectorData;
        this.options = options;
        this.field = field;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public DataType dataType() {
        return DataType.DENSE_VECTOR;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new KnnQueryPredicate(source(), newChildren.get(0), getVectorData(), getOptions());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, KnnQueryPredicate::new, field, getVectorData(), getOptions());
    }

    public float[] getVectorData() {
        return vectorData;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public Expression getField() {
        return field;
    }
}
