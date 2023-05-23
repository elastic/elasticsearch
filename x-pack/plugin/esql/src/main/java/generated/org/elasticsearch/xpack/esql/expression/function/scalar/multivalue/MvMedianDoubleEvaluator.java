// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedian}.
 * This class is generated. Do not edit it.
 */
public final class MvMedianDoubleEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMedianDoubleEvaluator(EvalOperator.ExpressionEvaluator field) {
    super(field);
  }

  @Override
  public String name() {
    return "MvMedian";
  }

  @Override
  public Block evalNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(positionCount);
    MvMedian.Doubles work = new MvMedian.Doubles();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      if (valueCount == 0) {
        builder.appendNull();
        continue;
      }
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      for (int i = first; i < end; i++) {
        double value = v.getDouble(i);
        MvMedian.process(work, value);
      }
      double result = MvMedian.finish(work);
      builder.appendDouble(result);
    }
    return builder.build();
  }

  @Override
  public Vector evalNotNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    double[] values = new double[positionCount];
    MvMedian.Doubles work = new MvMedian.Doubles();
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      for (int i = first; i < end; i++) {
        double value = v.getDouble(i);
        MvMedian.process(work, value);
      }
      double result = MvMedian.finish(work);
      values[p] = result;
    }
    return new DoubleArrayVector(values, positionCount);
  }
}
