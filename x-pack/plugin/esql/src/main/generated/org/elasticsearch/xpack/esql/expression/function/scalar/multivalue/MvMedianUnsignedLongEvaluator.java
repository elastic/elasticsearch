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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedian}.
 * This class is generated. Edit {@code MvEvaluatorImplementer} instead.
 */
public final class MvMedianUnsignedLongEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMedianUnsignedLongEvaluator(EvalOperator.ExpressionEvaluator field,
      DriverContext driverContext) {
    super(driverContext, field);
  }

  @Override
  public String name() {
    return "MvMedian";
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
      MvMedian.Longs work = new MvMedian.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          long value = v.getLong(i);
          MvMedian.processUnsignedLong(work, value);
        }
        long result = MvMedian.finishUnsignedLong(work);
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
      MvMedian.Longs work = new MvMedian.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          long value = v.getLong(i);
          MvMedian.processUnsignedLong(work, value);
        }
        long result = MvMedian.finishUnsignedLong(work);
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
      MvMedian.Longs work = new MvMedian.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        long result = MvMedian.ascendingUnsignedLong(v, first, valueCount);
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
      MvMedian.Longs work = new MvMedian.Longs();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        long result = MvMedian.ascendingUnsignedLong(v, first, valueCount);
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
    public MvMedianUnsignedLongEvaluator get(DriverContext context) {
      return new MvMedianUnsignedLongEvaluator(field.get(context), context);
    }

    @Override
    public String toString() {
      return "MvMedian[field=" + field + "]";
    }
  }
}
