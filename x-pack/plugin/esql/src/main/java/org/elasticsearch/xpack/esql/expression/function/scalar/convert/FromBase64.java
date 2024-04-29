/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Base64;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public class FromBase64 extends UnaryScalarFunction {

    @FunctionInfo(
        returnType = "keyword",
        description = "Decode a base64 string.",
        examples = @Example(file = "string", tag = "from_base64")
    )
    public FromBase64(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "A base64 string.") Expression string
    ) {
        super(source, string);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isString(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FromBase64(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FromBase64::new, field());
    }

    @Evaluator()
    static BytesRef process(BytesRef field, @Fixed(includeInToString = false, build = true) BytesRefBuilder oScratch) {
        byte[] bytes = new byte[field.length];
        System.arraycopy(field.bytes, field.offset, bytes, 0, field.length);
        oScratch.grow(field.length);
        oScratch.clear();
        int decodedSize = Base64.getDecoder().decode(bytes, oScratch.bytes());
        return new BytesRef(oScratch.bytes(), 0, decodedSize);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case BYTES_REF -> new FromBase64Evaluator.Factory(source(), toEvaluator.apply(field), context -> new BytesRefBuilder());
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }
}
