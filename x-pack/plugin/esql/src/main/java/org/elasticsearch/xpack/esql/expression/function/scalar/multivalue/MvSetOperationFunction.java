/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Base class for multi-value set operations.
 */
public abstract class MvSetOperationFunction extends BinaryScalarFunction implements EvaluatorMapper {

    protected DataType dataType;

    protected MvSetOperationFunction(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (left().dataType() != DataType.NULL && right().dataType() != DataType.NULL) {
            this.dataType = left().dataType().noText();
            return isType(
                right(),
                t -> t.noText() == left().dataType().noText(),
                sourceText(),
                SECOND,
                left().dataType().noText().typeName()
            );
        }

        Expression evaluatedField = left().dataType() == DataType.NULL ? right() : left();
        this.dataType = evaluatedField.dataType().noText();

        return isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(evaluatedField, sourceText(), FIRST);
    }

    /**
     * Shared set operation processing logic.
     */
    protected static <T> void processSetOperation(
        Block.Builder builder,
        int position,
        Block field1,
        Block field2,
        BiFunction<Integer, Block, T> getValue,
        Consumer<T> addValue,
        BiConsumer<Set<T>, Set<T>> combineOp
    ) {
        int firstCount = field1.getValueCount(position);
        int secondCount = field2.getValueCount(position);

        // Extract values
        Set<T> firstSet = new LinkedHashSet<>();
        int firstIndex = field1.getFirstValueIndex(position);
        for (int i = 0; i < firstCount; i++) {
            firstSet.add(getValue.apply(firstIndex + i, field1));
        }

        Set<T> secondSet = new LinkedHashSet<>();
        int secondIndex = field2.getFirstValueIndex(position);
        for (int i = 0; i < secondCount; i++) {
            secondSet.add(getValue.apply(secondIndex + i, field2));
        }

        combineOp.accept(firstSet, secondSet);

        if (firstSet.isEmpty()) {
            builder.appendNull();
            return;
        }

        // Build result
        builder.beginPositionEntry();
        for (T value : firstSet) {
            addValue.accept(value);
        }
        builder.endPositionEntry();
    }
}
