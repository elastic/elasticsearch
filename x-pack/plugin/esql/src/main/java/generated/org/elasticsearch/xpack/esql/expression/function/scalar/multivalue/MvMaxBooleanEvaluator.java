// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMax}.
 * This class is generated. Do not edit it.
 */
public final class MvMaxBooleanEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  public MvMaxBooleanEvaluator(EvalOperator.ExpressionEvaluator field) {
    super(field);
  }

  @Override
  public String name() {
    return "MvMax";
  }

  @Override
  public Block evalNullable(Block fieldVal) {
    BooleanBlock v = (BooleanBlock) fieldVal;
    int positionCount = v.getPositionCount();
    BooleanBlock.Builder builder = BooleanBlock.newBlockBuilder(positionCount);
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

  @Override
  public Vector evalNotNullable(Block fieldVal) {
    BooleanBlock v = (BooleanBlock) fieldVal;
    int positionCount = v.getPositionCount();
    boolean[] values = new boolean[positionCount];
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
      values[p] = result;
    }
    return new BooleanArrayVector(values, positionCount);
  }
}
