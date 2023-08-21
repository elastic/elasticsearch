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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class Substring extends ScalarFunction implements OptionalArgument, Mappable {

    private final Expression str, start, length;

    public Substring(Source source, Expression str, Expression start, Expression length) {
        super(source, length == null ? Arrays.asList(str, start) : Arrays.asList(str, start, length));
        this.str = str;
        this.start = start;
        this.length = length;
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

        resolution = isInteger(start, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return length == null ? TypeResolution.TYPE_RESOLVED : isInteger(length, sourceText(), THIRD);
    }

    @Override
    public boolean foldable() {
        return str.foldable() && start.foldable() && (length == null || length.foldable());
    }

    @Override
    public Object fold() {
        return Mappable.super.fold();
    }

    @Evaluator(extraName = "NoLength")
    static BytesRef process(BytesRef str, int start) {
        if (str.length == 0) {
            return null;
        }
        int codePointCount = UnicodeUtil.codePointCount(str);
        int indexStart = indexStart(codePointCount, start);
        return new BytesRef(str.utf8ToString().substring(indexStart));
    }

    @Evaluator
    static BytesRef process(BytesRef str, int start, int length) {
        if (str.length == 0) {
            return null;
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length parameter cannot be negative, found [" + length + "]");
        }
        int codePointCount = UnicodeUtil.codePointCount(str);
        int indexStart = indexStart(codePointCount, start);
        int indexEnd = Math.min(codePointCount, indexStart + length);
        String s = str.utf8ToString();
        return new BytesRef(s.substring(s.offsetByCodePoints(0, indexStart), s.offsetByCodePoints(0, indexEnd)));
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
        return new Substring(source(), newChildren.get(0), newChildren.get(1), length == null ? null : newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Substring::new, str, start, length);
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> strSupplier = toEvaluator.apply(str);
        Supplier<EvalOperator.ExpressionEvaluator> startSupplier = toEvaluator.apply(start);
        if (length == null) {
            return () -> new SubstringNoLengthEvaluator(strSupplier.get(), startSupplier.get());
        }
        Supplier<EvalOperator.ExpressionEvaluator> lengthSupplier = toEvaluator.apply(length);
        return () -> new SubstringEvaluator(strSupplier.get(), startSupplier.get(), lengthSupplier.get());
    }
}
