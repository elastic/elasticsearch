// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedianAbsoluteDeviation}.
 * This class is generated. Edit {@code MvEvaluatorImplementer} instead.
 */
public final class MvMedianAbsoluteDeviationDoubleEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMedianAbsoluteDeviationDoubleEvaluator(EvalOperator.ExpressionEvaluator field,
      DriverContext driverContext) {
    super(driverContext, field);
  }

  @Override
  public String name() {
    return "MvMedianAbsoluteDeviation";
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block evalNullable(Block fieldVal) {
    if (fieldVal.mvSortedAscending()) {
      return evalAscendingNullable(fieldVal);
    }
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        if (valueCount == 1) {
          double value = v.getDouble(first);
          double result = MvMedianAbsoluteDeviation.single(value);
          builder.appendDouble(result);
          continue;
        }
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          double value = v.getDouble(i);
          MvMedianAbsoluteDeviation.process(work, value);
        }
        double result = MvMedianAbsoluteDeviation.finish(work);
        builder.appendDouble(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block evalNotNullable(Block fieldVal) {
    if (fieldVal.mvSortedAscending()) {
      return evalAscendingNotNullable(fieldVal);
    }
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleVector.FixedBuilder builder = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        if (valueCount == 1) {
          double value = v.getDouble(first);
          double result = MvMedianAbsoluteDeviation.single(value);
          builder.appendDouble(result);
          continue;
        }
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          double value = v.getDouble(i);
          MvMedianAbsoluteDeviation.process(work, value);
        }
        double result = MvMedianAbsoluteDeviation.finish(work);
        builder.appendDouble(result);
      }
      return builder.build().asBlock();
    }
  }

  /**
   * Evaluate blocks containing only single valued fields.
   */
  @Override
  public Block evalSingleValuedNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        assert valueCount == 1;
        int first = v.getFirstValueIndex(p);
        double value = v.getDouble(first);
        double result = MvMedianAbsoluteDeviation.single(value);
        builder.appendDouble(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing only single valued fields.
   */
  @Override
  public Block evalSingleValuedNotNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleVector.FixedBuilder builder = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        assert valueCount == 1;
        int first = v.getFirstValueIndex(p);
        double value = v.getDouble(first);
        double result = MvMedianAbsoluteDeviation.single(value);
        builder.appendDouble(result);
      }
      return builder.build().asBlock();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        double result = MvMedianAbsoluteDeviation.ascending(work, v, first, valueCount);
        builder.appendDouble(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNotNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleVector.FixedBuilder builder = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Doubles work = new MvMedianAbsoluteDeviation.Doubles();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        double result = MvMedianAbsoluteDeviation.ascending(work, v, first, valueCount);
        builder.appendDouble(result);
      }
      return builder.build().asBlock();
    }
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory field;

    public Factory(EvalOperator.ExpressionEvaluator.Factory field) {
      this.field = field;
    }

    @Override
    public MvMedianAbsoluteDeviationDoubleEvaluator get(DriverContext context) {
      return new MvMedianAbsoluteDeviationDoubleEvaluator(field.get(context), context);
    }

    @Override
    public String toString() {
      return "MvMedianAbsoluteDeviation[field=" + field + "]";
    }
  }
}
