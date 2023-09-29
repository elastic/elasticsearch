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
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMin}.
 * This class is generated. Do not edit it.
 */
public final class MvMinDoubleEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  private final DriverContext driverContext;

  public MvMinDoubleEvaluator(EvalOperator.ExpressionEvaluator field, DriverContext driverContext) {
    super(field);
    this.driverContext = driverContext;
  }

  @Override
  public String name() {
    return "MvMin";
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
    DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(positionCount, driverContext.blockFactory());
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      double value = v.getDouble(first);
      for (int i = first + 1; i < end; i++) {
        double next = v.getDouble(i);
        value = MvMin.process(value, next);
      }
      double result = value;
      builder.appendDouble(result);
    }
    return builder.build();
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Vector evalNotNullable(Block fieldVal) {
    if (fieldVal.mvSortedAscending()) {
      return evalAscendingNotNullable(fieldVal);
    }
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    DoubleVector.FixedBuilder builder = DoubleVector.newVectorFixedBuilder(positionCount, driverContext.blockFactory());
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      double value = v.getDouble(first);
      for (int i = first + 1; i < end; i++) {
        double next = v.getDouble(i);
        value = MvMin.process(value, next);
      }
      double result = value;
      builder.appendDouble(result);
    }
    return builder.build();
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(positionCount, driverContext.blockFactory());
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int first = v.getFirstValueIndex(p);
      int idx = MvMin.ascendingIndex(valueCount);
      double result = v.getDouble(first + idx);
      builder.appendDouble(result);
    }
    return builder.build();
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Vector evalAscendingNotNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    DoubleVector.FixedBuilder builder = DoubleVector.newVectorFixedBuilder(positionCount, driverContext.blockFactory());
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int idx = MvMin.ascendingIndex(valueCount);
      double result = v.getDouble(first + idx);
      builder.appendDouble(result);
    }
    return builder.build();
  }
}
