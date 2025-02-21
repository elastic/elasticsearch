// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedianAbsoluteDeviation}.
 * This class is generated. Edit {@code MvEvaluatorImplementer} instead.
 */
public final class MvMedianAbsoluteDeviationIntEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMedianAbsoluteDeviationIntEvaluator(EvalOperator.ExpressionEvaluator field,
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
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        if (valueCount == 1) {
          int value = v.getInt(first);
          int result = MvMedianAbsoluteDeviation.single(value);
          builder.appendInt(result);
          continue;
        }
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          int value = v.getInt(i);
          MvMedianAbsoluteDeviation.process(work, value);
        }
        int result = MvMedianAbsoluteDeviation.finishInts(work);
        builder.appendInt(result);
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
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        if (valueCount == 1) {
          int value = v.getInt(first);
          int result = MvMedianAbsoluteDeviation.single(value);
          builder.appendInt(result);
          continue;
        }
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          int value = v.getInt(i);
          MvMedianAbsoluteDeviation.process(work, value);
        }
        int result = MvMedianAbsoluteDeviation.finishInts(work);
        builder.appendInt(result);
      }
      return builder.build().asBlock();
    }
  }

  /**
   * Evaluate blocks containing only single valued fields.
   */
  @Override
  public Block evalSingleValuedNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        assert valueCount == 1;
        int first = v.getFirstValueIndex(p);
        int value = v.getInt(first);
        int result = MvMedianAbsoluteDeviation.single(value);
        builder.appendInt(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing only single valued fields.
   */
  @Override
  public Block evalSingleValuedNotNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        assert valueCount == 1;
        int first = v.getFirstValueIndex(p);
        int value = v.getInt(first);
        int result = MvMedianAbsoluteDeviation.single(value);
        builder.appendInt(result);
      }
      return builder.build().asBlock();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        int result = MvMedianAbsoluteDeviation.ascending(work, v, first, valueCount);
        builder.appendInt(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNotNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      MvMedianAbsoluteDeviation.Longs work = new MvMedianAbsoluteDeviation.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        int result = MvMedianAbsoluteDeviation.ascending(work, v, first, valueCount);
        builder.appendInt(result);
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
    public MvMedianAbsoluteDeviationIntEvaluator get(DriverContext context) {
      return new MvMedianAbsoluteDeviationIntEvaluator(field.get(context), context);
    }

    @Override
    public String toString() {
      return "MvMedianAbsoluteDeviation[field=" + field + "]";
    }
  }
}
