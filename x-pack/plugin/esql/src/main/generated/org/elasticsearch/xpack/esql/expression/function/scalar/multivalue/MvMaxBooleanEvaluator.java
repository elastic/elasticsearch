// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMax}.
 * This class is generated. Edit {@code MvEvaluatorImplementer} instead.
 */
public final class MvMaxBooleanEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMaxBooleanEvaluator(EvalOperator.ExpressionEvaluator field,
      DriverContext driverContext) {
    super(driverContext, field);
  }

  @Override
  public String name() {
    return "MvMax";
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block evalNullable(Block fieldVal) {
    if (fieldVal.mvSortedAscending()) {
      return evalAscendingNullable(fieldVal);
    }
    BooleanBlock v = (BooleanBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        int end = first + valueCount;
        boolean value = v.getBoolean(first);
        for (int i = first + 1; i < end; i++) {
          boolean next = v.getBoolean(i);
          value = MvMax.process(value, next);
        }
        boolean result = value;
        builder.appendBoolean(result);
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
    BooleanBlock v = (BooleanBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (BooleanVector.FixedBuilder builder = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        int end = first + valueCount;
        boolean value = v.getBoolean(first);
        for (int i = first + 1; i < end; i++) {
          boolean next = v.getBoolean(i);
          value = MvMax.process(value, next);
        }
        boolean result = value;
        builder.appendBoolean(result);
      }
      return builder.build().asBlock();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNullable(Block fieldVal) {
    BooleanBlock v = (BooleanBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        int idx = MvMax.ascendingIndex(valueCount);
        boolean result = v.getBoolean(first + idx);
        builder.appendBoolean(result);
      }
      return builder.build();
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNotNullable(Block fieldVal) {
    BooleanBlock v = (BooleanBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (BooleanVector.FixedBuilder builder = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        int idx = MvMax.ascendingIndex(valueCount);
        boolean result = v.getBoolean(first + idx);
        builder.appendBoolean(result);
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
    public MvMaxBooleanEvaluator get(DriverContext context) {
      return new MvMaxBooleanEvaluator(field.get(context), context);
    }

    @Override
    public String toString() {
      return "MvMax[field=" + field + "]";
    }
  }
}
