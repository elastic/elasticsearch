// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Least}.
 * This class is generated. Do not edit it.
 */
public final class LeastLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator[] values;

  private final DriverContext driverContext;

  public LeastLongEvaluator(EvalOperator.ExpressionEvaluator[] values,
      DriverContext driverContext) {
    this.values = values;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    LongBlock[] valuesBlocks = new LongBlock[values.length];
    try (Releasable valuesRelease = Releasables.wrap(valuesBlocks)) {
      for (int i = 0; i < valuesBlocks.length; i++) {
        valuesBlocks[i] = (LongBlock)values[i].eval(page);
      }
      LongVector[] valuesVectors = new LongVector[values.length];
      for (int i = 0; i < valuesBlocks.length; i++) {
        valuesVectors[i] = valuesBlocks[i].asVector();
        if (valuesVectors[i] == null) {
          return eval(page.getPositionCount(), valuesBlocks);
        }
      }
      return eval(page.getPositionCount(), valuesVectors).asBlock();
    }
  }

  public LongBlock eval(int positionCount, LongBlock[] valuesBlocks) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      long[] valuesValues = new long[values.length];
      position: for (int p = 0; p < positionCount; p++) {
        for (int i = 0; i < valuesBlocks.length; i++) {
          if (valuesBlocks[i].isNull(p) || valuesBlocks[i].getValueCount(p) != 1) {
            result.appendNull();
            continue position;
          }
        }
        // unpack valuesBlocks into valuesValues
        for (int i = 0; i < valuesBlocks.length; i++) {
          int o = valuesBlocks[i].getFirstValueIndex(p);
          valuesValues[i] = valuesBlocks[i].getLong(o);
        }
        result.appendLong(Least.process(valuesValues));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector[] valuesVectors) {
    try(LongVector.Builder result = driverContext.blockFactory().newLongVectorBuilder(positionCount)) {
      long[] valuesValues = new long[values.length];
      position: for (int p = 0; p < positionCount; p++) {
        // unpack valuesVectors into valuesValues
        for (int i = 0; i < valuesVectors.length; i++) {
          valuesValues[i] = valuesVectors[i].getLong(p);
        }
        result.appendLong(Least.process(valuesValues));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "LeastLongEvaluator[" + "values=" + Arrays.toString(values) + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(() -> Releasables.close(values));
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory[] values;

    public Factory(EvalOperator.ExpressionEvaluator.Factory[] values) {
      this.values = values;
    }

    @Override
    public LeastLongEvaluator get(DriverContext context) {
      EvalOperator.ExpressionEvaluator[] values = Arrays.stream(this.values).map(a -> a.get(context)).toArray(EvalOperator.ExpressionEvaluator[]::new);
      return new LeastLongEvaluator(values, context);
    }

    @Override
    public String toString() {
      return "LeastLongEvaluator[" + "values=" + Arrays.toString(values) + "]";
    }
  }
}
