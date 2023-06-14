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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedian}.
 * This class is generated. Do not edit it.
 */
public final class MvMedianIntEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMedianIntEvaluator(EvalOperator.ExpressionEvaluator field) {
    super(field);
  }

  @Override
  public String name() {
    return "MvMedian";
  }

  @Override
  public Block evalNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    IntBlock.Builder builder = IntBlock.newBlockBuilder(positionCount);
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

  @Override
  public Vector evalNotNullable(Block fieldVal) {
    IntBlock v = (IntBlock) fieldVal;
    int positionCount = v.getPositionCount();
    int[] values = new int[positionCount];
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
      values[p] = result;
    }
    return new IntArrayVector(values, positionCount);
  }
}
