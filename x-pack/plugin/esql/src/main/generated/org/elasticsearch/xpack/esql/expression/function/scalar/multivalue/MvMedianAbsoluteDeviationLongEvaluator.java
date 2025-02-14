// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedianAbsoluteDeviation}.
 * This class is generated. Edit {@code MvEvaluatorImplementer} instead.
 */
public final class MvMedianAbsoluteDeviationLongEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMedianAbsoluteDeviationLongEvaluator(EvalOperator.ExpressionEvaluator field,
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
    LongBlock v = (LongBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        if (valueCount == 1) {
          long value = v.getLong(first);
          long result = MvMedianAbsoluteDeviation.single(value);
          builder.appendLong(result);
          continue;
        }
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          long value = v.getLong(i);
          MvMedianAbsoluteDeviation.process(work, value);
        }
        long result = MvMedianAbsoluteDeviation.finish(work);
        builder.appendLong(result);
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
    LongBlock v = (LongBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (LongVector.FixedBuilder builder = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        if (valueCount == 1) {
          long value = v.getLong(first);
          long result = MvMedianAbsoluteDeviation.single(value);
          builder.appendLong(result);
          continue;
        }
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          long value = v.getLong(i);
          MvMedianAbsoluteDeviation.process(work, value);
        }
        long result = MvMedianAbsoluteDeviation.finish(work);
        builder.appendLong(result);
      }
      return builder.build().asBlock();
    }
  }

  /**
   * Evaluate blocks containing only single valued fields.
   */
  @Override
  public Block evalSingleValuedNullable(Block fieldVal) {
    LongBlock v = (LongBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        assert valueCount == 1;
        int first = v.getFirstValueIndex(p);
        long value = v.getLong(first);
        long result = MvMedianAbsoluteDeviation.single(value);
        builder.appendLong(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing only single valued fields.
   */
  @Override
  public Block evalSingleValuedNotNullable(Block fieldVal) {
    LongBlock v = (LongBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (LongVector.FixedBuilder builder = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        assert valueCount == 1;
        int first = v.getFirstValueIndex(p);
        long value = v.getLong(first);
        long result = MvMedianAbsoluteDeviation.single(value);
        builder.appendLong(result);
      }
      return builder.build().asBlock();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNullable(Block fieldVal) {
    LongBlock v = (LongBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        long result = MvMedianAbsoluteDeviation.ascending(work, v, first, valueCount);
        builder.appendLong(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNotNullable(Block fieldVal) {
    LongBlock v = (LongBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (LongVector.FixedBuilder builder = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        long result = MvMedianAbsoluteDeviation.ascending(work, v, first, valueCount);
        builder.appendLong(result);
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
    public MvMedianAbsoluteDeviationLongEvaluator get(DriverContext context) {
      return new MvMedianAbsoluteDeviationLongEvaluator(field.get(context), context);
    }

    @Override
    public String toString() {
      return "MvMedianAbsoluteDeviation[field=" + field + "]";
    }
  }
}
