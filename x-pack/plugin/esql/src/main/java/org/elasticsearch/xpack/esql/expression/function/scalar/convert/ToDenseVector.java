/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToDouble;

public class ToDenseVector extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToDenseVector",
        ToDenseVector::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DENSE_VECTOR, (source, fieldEval) -> fieldEval),
        Map.entry(KEYWORD, ToDenseVectorFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToDenseVectorFromStringEvaluator.Factory::new),
        Map.entry(LONG, ToDenseVectorFromLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToDenseVectorFromIntEvaluator.Factory::new),
        Map.entry(DOUBLE, ToDenseVectorFromDoubleEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "dense_vector",
        description = "Converts a multi-valued input of numbers or strings to a dense_vector."
    )
    public ToDenseVector(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text", "double", "long", "integer" },
            description = "Input multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToDenseVector(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return DENSE_VECTOR;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDenseVector(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDenseVector::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { org.elasticsearch.xpack.esql.core.InvalidArgumentException.class })
    static float fromString(BytesRef in) {
        return (float) stringToDouble(in.utf8ToString());
    }

    @ConvertEvaluator(extraName = "FromLong")
    static float fromLong(long l) {
        return l;
    }

    @ConvertEvaluator(extraName = "FromInt")
    static float fromInt(int i) {
        return i;
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static float fromDouble(double d) {
        return (float) d;
    }
}
