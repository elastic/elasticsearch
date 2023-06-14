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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMax}.
 * This class is generated. Do not edit it.
 */
public final class MvMaxDoubleEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMaxDoubleEvaluator(EvalOperator.ExpressionEvaluator field) {
    super(field);
  }

  @Override
  public String name() {
    return "MvMax";
  }

  @Override
  public Block evalNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(positionCount);
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
        value = MvMax.process(value, next);
      }
      double result = value;
      builder.appendDouble(result);
    }
    return builder.build();
  }

  @Override
  public Vector evalNotNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    double[] values = new double[positionCount];
    for (int p = 0; p < positionCount; p++) {
      int valueCount = v.getValueCount(p);
      int first = v.getFirstValueIndex(p);
      int end = first + valueCount;
      double value = v.getDouble(first);
      for (int i = first + 1; i < end; i++) {
        double next = v.getDouble(i);
        value = MvMax.process(value, next);
      }
      double result = value;
      values[p] = result;
    }
    return new DoubleArrayVector(values, positionCount);
  }
}
