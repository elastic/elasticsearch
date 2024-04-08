// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Least}.
 * This class is generated. Do not edit it.
 */
public final class LeastBooleanEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator[] values;

  private final DriverContext driverContext;

  public LeastBooleanEvaluator(Source source, EvalOperator.ExpressionEvaluator[] values,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.values = values;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    BooleanBlock[] valuesBlocks = new BooleanBlock[values.length];
    try (Releasable valuesRelease = Releasables.wrap(valuesBlocks)) {
      for (int i = 0; i < valuesBlocks.length; i++) {
        valuesBlocks[i] = (BooleanBlock)values[i].eval(page);
      }
      BooleanVector[] valuesVectors = new BooleanVector[values.length];
      for (int i = 0; i < valuesBlocks.length; i++) {
        valuesVectors[i] = valuesBlocks[i].asVector();
        if (valuesVectors[i] == null) {
          return eval(page.getPositionCount(), valuesBlocks);
        }
      }
      return eval(page.getPositionCount(), valuesVectors).asBlock();
    }
  }

  public BooleanBlock eval(int positionCount, BooleanBlock[] valuesBlocks) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      boolean[] valuesValues = new boolean[values.length];
      position: for (int p = 0; p < positionCount; p++) {
        for (int i = 0; i < valuesBlocks.length; i++) {
          if (valuesBlocks[i].isNull(p)) {
            result.appendNull();
            continue position;
          }
          if (valuesBlocks[i].getValueCount(p) != 1) {
            if (valuesBlocks[i].getValueCount(p) > 1) {
              warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
            }
            result.appendNull();
            continue position;
          }
        }
        // unpack valuesBlocks into valuesValues
        for (int i = 0; i < valuesBlocks.length; i++) {
          int o = valuesBlocks[i].getFirstValueIndex(p);
          valuesValues[i] = valuesBlocks[i].getBoolean(o);
        }
        result.appendBoolean(Least.process(valuesValues));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BooleanVector[] valuesVectors) {
    try(BooleanVector.Builder result = driverContext.blockFactory().newBooleanVectorBuilder(positionCount)) {
      boolean[] valuesValues = new boolean[values.length];
      position: for (int p = 0; p < positionCount; p++) {
        // unpack valuesVectors into valuesValues
        for (int i = 0; i < valuesVectors.length; i++) {
          valuesValues[i] = valuesVectors[i].getBoolean(p);
        }
        result.appendBoolean(Least.process(valuesValues));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "LeastBooleanEvaluator[" + "values=" + Arrays.toString(values) + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(() -> Releasables.close(values));
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory[] values;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory[] values) {
      this.source = source;
      this.values = values;
    }

    @Override
    public LeastBooleanEvaluator get(DriverContext context) {
      EvalOperator.ExpressionEvaluator[] values = Arrays.stream(this.values).map(a -> a.get(context)).toArray(EvalOperator.ExpressionEvaluator[]::new);
      return new LeastBooleanEvaluator(source, values, context);
    }

    @Override
    public String toString() {
      return "LeastBooleanEvaluator[" + "values=" + Arrays.toString(values) + "]";
    }
  }
}
