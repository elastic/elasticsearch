/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class Equals extends org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals implements EvaluatorMapper {
    public Equals(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    public Equals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, zoneId);
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return EsqlTypeResolutions.isExact(e, sourceText(), DEFAULT);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals> info() {
        return NodeInfo.create(this, Equals::new, left(), right(), zoneId());
    }

    @Override
    protected Equals replaceChildren(Expression newLeft, Expression newRight) {
        return new Equals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public Equals swapLeftAndRight() {
        return new Equals(source(), right(), left(), zoneId());
    }

    @Override
    public BinaryComparison negate() {
        return new NotEquals(source(), left(), right(), zoneId());
    }

    @Evaluator(extraName = "Ints")
    static boolean processInts(int lhs, int rhs) {
        return lhs == rhs;
    }

    @Evaluator(extraName = "Longs")
    static boolean processLongs(long lhs, long rhs) {
        return lhs == rhs;
    }

    @Evaluator(extraName = "Doubles")
    static boolean processDoubles(double lhs, double rhs) {
        return lhs == rhs;
    }

    @Evaluator(extraName = "Keywords")
    static boolean processKeywords(BytesRef lhs, BytesRef rhs) {
        return lhs.equals(rhs);
    }

    @Evaluator(extraName = "Bools")
    static boolean processBools(boolean lhs, boolean rhs) {
        return lhs == rhs;
    }

    @Evaluator(extraName = "Geometries")
    static boolean processGeometries(BytesRef lhs, BytesRef rhs) {
        return lhs.equals(rhs);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator) {
        // Our type is always boolean, so figure out the evaluator type from the inputs
        DataType commonType = EsqlDataTypeRegistry.INSTANCE.commonType(left().dataType(), right().dataType());
        var lhs = Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left()));
        var rhs = Cast.cast(source(), right().dataType(), commonType, toEvaluator.apply(right()));
        if (DataTypes.isDateTime(commonType)) {
            return new EqualsLongsEvaluator.Factory(source(), lhs, rhs);
        }
        if (EsqlDataTypes.isSpatial(commonType)) {
            return new EqualsGeometriesEvaluator.Factory(source(), lhs, rhs);
        }
        if (commonType.equals(DataTypes.INTEGER)) {
            return new EqualsIntsEvaluator.Factory(source(), lhs, rhs);
        }
        if (commonType.equals(DataTypes.LONG) || commonType.equals(DataTypes.UNSIGNED_LONG)) {
            return new EqualsLongsEvaluator.Factory(source(), lhs, rhs);
        }
        if (commonType.equals(DataTypes.DOUBLE)) {
            return new EqualsDoublesEvaluator.Factory(source(), lhs, rhs);
        }
        if (commonType.equals(DataTypes.BOOLEAN)) {
            return new EqualsBoolsEvaluator.Factory(source(), lhs, rhs);
        }
        if (DataTypes.isString(commonType)) {
            return new EqualsKeywordsEvaluator.Factory(source(), lhs, rhs);
        }
        throw new EsqlIllegalArgumentException("Unsupported type " + left().dataType());
    }
    @Override
    public Boolean fold() {
        return (Boolean) EvaluatorMapper.super.fold();
    }
}
