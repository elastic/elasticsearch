// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvSum}.
 * This class is generated. Do not edit it.
 */
public final class MvSumIntEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvSumIntEvaluator(EvalOperator.ExpressionEvaluator field) {
    super(field);
  }

  @Override
  public String name() {
    return "MvSum";
  }

  @Override
  public Block evalNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    IntBlock.Builder builder = IntBlock.newBlockBuilder(positionCount);
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      int value = v.getInt(first);
      for (int i = first + 1; i < end; i++) {
        int next = v.getInt(i);
        value = MvSum.process(value, next);
      }
      int result = value;
      builder.appendInt(result);
    }
    return builder.build();
  }

  @Override
  public Vector evalNotNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    int[] values = new int[positionCount];
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      int value = v.getInt(first);
      for (int i = first + 1; i < end; i++) {
        int next = v.getInt(i);
        value = MvSum.process(value, next);
      }
      int result = value;
      values[p] = result;
    }
    return new IntArrayVector(values, positionCount);
  }
}
