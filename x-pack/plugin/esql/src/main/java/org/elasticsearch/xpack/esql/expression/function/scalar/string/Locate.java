/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
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

/**
 * Locate function, given a string 'a' and a substring 'b', it returns the index of the first occurrence of the substring 'b' in 'a'.
 */
public class Locate extends EsqlScalarFunction {

    private final Expression str;
    private final Expression substr;

    @FunctionInfo(
        returnType = "integer",
        description = "Returns an integer that indicates the position of a keyword substring within another string"
    )
    public Locate(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "An input string") Expression str,
        @Param(name = "substr", type = { "keyword", "text" }, description = "a substring to locate in the input string") Expression substr
    ) {
        super(source, Arrays.asList(str, substr));
        this.str = str;
        this.substr = substr;
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
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
        return isString(substr, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return str.foldable() && substr.foldable();
    }

    @Evaluator
    static int process(BytesRef str, BytesRef substr) {
        if (str == null || substr == null || str.length < substr.length) {
            return 0;
        }
        int strCodePoints = UnicodeUtil.codePointCount(str);
        int substrCodePoints = UnicodeUtil.codePointCount(substr);
        int strIndex = 0;
        int substrIndex = 0;
        int locateIndex = -1;
        UnicodeUtil.UTF8CodePoint strReuse = null;
        UnicodeUtil.UTF8CodePoint substrReuse = null;
        while (strIndex < strCodePoints) {
            strReuse = UnicodeUtil.codePointAt(str.bytes, strIndex, strReuse);
            substrReuse = UnicodeUtil.codePointAt(substr.bytes, substrIndex, substrReuse);
            strIndex += 1;
            if (strReuse.codePoint == substrReuse.codePoint) {
                substrIndex += 1;
            }
            if (substrIndex == substrCodePoints) {
                locateIndex = strIndex - substrCodePoints;
                break;
            }
        }
        return 1 + locateIndex;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Locate(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Locate::new, str, substr);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return new LocateEvaluator.Factory(source(), toEvaluator.apply(str), toEvaluator.apply(substr));
    }
}
