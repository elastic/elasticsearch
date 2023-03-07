/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.compute.data.Page;
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
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

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

        TypeResolution resolution = isStringAndExact(str, sourceText(), FIRST);
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
        BytesRef source = (BytesRef) str.fold();
        Integer startPos = (Integer) start.fold();
        Integer runFor = length == null ? null : (Integer) length.fold();

        return process(source, startPos, runFor);
    }

    public static BytesRef process(BytesRef str, Integer start, Integer length) {
        if (str == null || str.length == 0 || start == null) {
            return null;
        }

        if (length != null && length < 0) {
            throw new IllegalArgumentException("Length parameter cannot be negative, found [" + length + "]");
        }

        // esql is 1-based when it comes to string manipulation. We treat start = 0 and 1 the same
        // a negative value is relative to the end of the string
        int codePointCount = UnicodeUtil.codePointCount(str);
        int indexStart;
        if (start > 0) {
            indexStart = start - 1;
        } else if (start < 0) {
            indexStart = codePointCount + start; // start is negative, so this is a subtraction
        } else {
            indexStart = start; // start == 0
        }
        indexStart = Math.min(Math.max(0, indexStart), codePointCount); // sanitise string start index

        int indexEnd = Math.min(codePointCount, length == null ? indexStart + codePointCount : indexStart + length);

        final String s = str.utf8ToString();
        return new BytesRef(s.substring(s.offsetByCodePoints(0, indexStart), s.offsetByCodePoints(0, indexEnd)));
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
        Supplier<EvalOperator.ExpressionEvaluator> lengthSupplier = length == null ? () -> null : toEvaluator.apply(length);
        return () -> new SubstringEvaluator(strSupplier.get(), startSupplier.get(), lengthSupplier.get());
    }

    record SubstringEvaluator(
        EvalOperator.ExpressionEvaluator str,
        EvalOperator.ExpressionEvaluator start,
        EvalOperator.ExpressionEvaluator length
    ) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            return Substring.process(
                (BytesRef) str.computeRow(page, pos),
                (Integer) start.computeRow(page, pos),
                length == null ? null : (Integer) length.computeRow(page, pos)
            );
        }
    }
}
