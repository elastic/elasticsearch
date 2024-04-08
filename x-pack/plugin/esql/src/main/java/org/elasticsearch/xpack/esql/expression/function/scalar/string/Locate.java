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
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

/**
 * Locate function, given a string 'a' and a substring 'b', it returns the index of the first occurrence of the substring 'b' in 'a'.
 */
public class Locate extends EsqlScalarFunction implements OptionalArgument {

    private final Expression str;
    private final Expression substr;
    private final Expression start;

    @FunctionInfo(
        returnType = "integer",
        description = "Returns an integer that indicates the position of a keyword substring within another string"
    )
    public Locate(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "An input string") Expression str,
        @Param(
            name = "substring",
            type = { "keyword", "text" },
            description = "A substring to locate in the input string"
        ) Expression substr,
        @Param(optional = true, name = "start", type = { "integer" }, description = "The start index") Expression start
    ) {
        super(source, start == null ? Arrays.asList(str, substr) : Arrays.asList(str, substr, start));
        this.str = str;
        this.substr = substr;
        this.start = start;
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
        resolution = isString(substr, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return start == null ? TypeResolution.TYPE_RESOLVED : isInteger(start, sourceText(), THIRD);
    }

    @Override
    public boolean foldable() {
        return str.foldable() && substr.foldable() && (start == null || start.foldable());
    }

    @Evaluator
    static int process(BytesRef str, BytesRef substr, int start) {
        if (str == null || substr == null || str.length < substr.length) {
            return 0;
        }
        int codePointCount = UnicodeUtil.codePointCount(str);
        int indexStart = indexStart(codePointCount, start);
        String utf8ToString = str.utf8ToString();
        return 1 + utf8ToString.indexOf(substr.utf8ToString(), utf8ToString.offsetByCodePoints(0, indexStart));
    }

    @Evaluator(extraName = "NoStart")
    static int process(BytesRef str, BytesRef substr) {
        return process(str, substr, 0);
    }

    private static int indexStart(int codePointCount, int start) {
        // esql is 1-based when it comes to string manipulation. We treat start = 0 and 1 the same
        // a negative value is relative to the end of the string
        int indexStart;
        if (start > 0) {
            indexStart = start - 1;
        } else if (start < 0) {
            indexStart = codePointCount + start; // start is negative, so this is a subtraction
        } else {
            indexStart = start; // start == 0
        }
        return Math.min(Math.max(0, indexStart), codePointCount); // sanitise string start index
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Locate(source(), newChildren.get(0), newChildren.get(1), start == null ? null : newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Locate::new, str, substr, start);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        ExpressionEvaluator.Factory strExpr = toEvaluator.apply(str);
        ExpressionEvaluator.Factory substrExpr = toEvaluator.apply(substr);
        if (start == null) {
            return new LocateNoStartEvaluator.Factory(source(), strExpr, substrExpr);
        }
        return new LocateEvaluator.Factory(source(), strExpr, substrExpr, toEvaluator.apply(start));
    }
}
