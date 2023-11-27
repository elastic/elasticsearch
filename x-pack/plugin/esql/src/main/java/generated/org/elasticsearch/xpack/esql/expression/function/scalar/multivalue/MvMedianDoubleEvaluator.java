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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedian}.
 * This class is generated. Do not edit it.
 */
public final class MvMedianDoubleEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  private final DriverContext driverContext;

  public MvMedianDoubleEvaluator(EvalOperator.ExpressionEvaluator field,
      DriverContext driverContext) {
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
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
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
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block evalNotNullable(Block fieldVal) {
    DoubleBlock v = (DoubleBlock) fieldVal;
    int positionCount = v.getPositionCount();
    try (DoubleVector.FixedBuilder builder = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
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
        builder.appendDouble(result);
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
    public MvMedianDoubleEvaluator get(DriverContext context) {
      return new MvMedianDoubleEvaluator(field.get(context), context);
    }

    @Override
    public String toString() {
      return "MvMedian[field=" + field + "]";
    }
  }
}
