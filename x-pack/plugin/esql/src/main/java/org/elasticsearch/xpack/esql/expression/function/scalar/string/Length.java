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
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public class Length extends UnaryScalarFunction implements Mappable {

    public Length(Source source, Expression field) {
        super(source, field);
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

        return isType(field(), dt -> dt == DataTypes.KEYWORD, sourceText(), FIRST, DataTypes.KEYWORD.typeName());
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        return process((BytesRef) field().fold());
    }

    public static Integer process(BytesRef fieldVal) {
        if (fieldVal == null) {
            return null;
        }
        return UnicodeUtil.codePointCount(fieldVal);
    }

    @Override
    protected UnaryScalarFunction replaceChild(Expression newChild) {
        return new Length(source(), newChild);
    }

    @Override
    protected Processor makeProcessor() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Length::new, field());
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> field = toEvaluator.apply(field());
        return () -> new LengthEvaluator(field.get());
    }

    record LengthEvaluator(EvalOperator.ExpressionEvaluator exp) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            return Length.process(((BytesRef) exp.computeRow(page, pos)));
        }
    }
}
