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
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedian}.
 * This class is generated. Do not edit it.
 */
public final class MvMedianIntEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  private final DriverContext driverContext;

  public MvMedianIntEvaluator(EvalOperator.ExpressionEvaluator field, DriverContext driverContext) {
    super(field);
    this.driverContext = driverContext;
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
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    IntBlock.Builder builder = IntBlock.newBlockBuilder(positionCount, driverContext.blockFactory());
    MvMedian.Ints work = new MvMedian.Ints();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      for (int i = first; i < end; i++) {
        int value = v.getInt(i);
        MvMedian.process(work, value);
      }
      int result = MvMedian.finish(work);
      builder.appendInt(result);
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
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    IntVector.FixedBuilder builder = IntVector.newVectorFixedBuilder(positionCount, driverContext.blockFactory());
    MvMedian.Ints work = new MvMedian.Ints();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      for (int i = first; i < end; i++) {
        int value = v.getInt(i);
        MvMedian.process(work, value);
      }
      int result = MvMedian.finish(work);
      builder.appendInt(result);
    }
    return builder.build();
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Block evalAscendingNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    IntBlock.Builder builder = IntBlock.newBlockBuilder(positionCount, driverContext.blockFactory());
    MvMedian.Ints work = new MvMedian.Ints();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int first = v.getFirstValueIndex(p);
      int result = MvMedian.ascending(v, first, valueCount);
      builder.appendInt(result);
    }
    return builder.build();
  }

  /**
   * Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.
   */
  private Vector evalAscendingNotNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    IntVector.FixedBuilder builder = IntVector.newVectorFixedBuilder(positionCount, driverContext.blockFactory());
    MvMedian.Ints work = new MvMedian.Ints();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int result = MvMedian.ascending(v, first, valueCount);
      builder.appendInt(result);
    }
    return builder.build();
  }
}
