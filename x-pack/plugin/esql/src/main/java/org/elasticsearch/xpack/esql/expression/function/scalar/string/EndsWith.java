/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class EndsWith extends EsqlScalarFunction {

    private final Expression str;
    private final Expression suffix;

    @FunctionInfo(
        returnType = "boolean",
        description = "Returns a boolean that indicates whether a keyword string ends with another string"
    )
    public EndsWith(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }) Expression str,
        @Param(name = "suffix", type = { "keyword", "text" }) Expression suffix
    ) {
        super(source, Arrays.asList(str, suffix));
        this.str = str;
        this.suffix = suffix;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(str, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        return isString(suffix, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return str.foldable() && suffix.foldable();
    }

    @Evaluator
    static boolean process(BytesRef str, BytesRef suffix) {
        if (str.length < suffix.length) {
            return false;
        }
        return Arrays.equals(
            str.bytes,
            str.offset + str.length - suffix.length,
            str.offset + str.length,
            suffix.bytes,
            suffix.offset,
            suffix.offset + suffix.length
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new EndsWith(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, EndsWith::new, str, suffix);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return new EndsWithEvaluator.Factory(source(), toEvaluator.apply(str), toEvaluator.apply(suffix));
    }
}
