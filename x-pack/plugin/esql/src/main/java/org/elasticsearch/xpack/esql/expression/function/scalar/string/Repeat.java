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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Repeat function, given a string 'a' and in integer 'b', it returns 'a' concatenated with itself 'b' times.
 */
public class Repeat extends EsqlScalarFunction implements OptionalArgument {

    private final Expression str;
    private final Expression number;

    @FunctionInfo(
        returnType = "keyword",
        description = "Returns an integer that indicates the position of a keyword substring within another string",
        examples = @Example(file = "string", tag = "locate")
    )
    public Repeat(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "An input string") Expression str,
        @Param(name = "number", type = { "integer" }, description = "Number times to repeat") Expression number
    ) {
        super(source, Arrays.asList(str, number));
        this.str = str;
        this.number = number;
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
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

        return isType(number, dt -> dt == DataTypes.INTEGER, sourceText(), SECOND, "integer");
    }

    @Override
    public boolean foldable() {
        return str.foldable() && number.foldable();
    }

    @Evaluator
    static BytesRef process(BytesRef str, int number) {
        if (str == null) {
            return null;
        }
        if (number < 0) {
            throw new IllegalArgumentException("Times parameter cannot be negative, found [" + number + "]");
        }

        String utf8ToString = str.utf8ToString();
        String repeated = utf8ToString.repeat(number);
        return new BytesRef(repeated);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Repeat(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Repeat::new, str, number);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        ExpressionEvaluator.Factory strExpr = toEvaluator.apply(str);
        ExpressionEvaluator.Factory numberExpr = toEvaluator.apply(number);
        return new RepeatEvaluator.Factory(source(), strExpr, numberExpr);
    }
}
