// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Abs}.
 * This class is generated. Do not edit it.
 */
public final class AbsLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator fieldVal;

  private final DriverContext driverContext;

  private Warnings warnings;

  public AbsLongEvaluator(Source source, EvalOperator.ExpressionEvaluator fieldVal,
      DriverContext driverContext) {
    this.source = source;
    this.fieldVal = fieldVal;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock fieldValBlock = (LongBlock) fieldVal.eval(page)) {
      LongVector fieldValVector = fieldValBlock.asVector();
      if (fieldValVector == null) {
        return eval(page.getPositionCount(), fieldValBlock);
      }
      return eval(page.getPositionCount(), fieldValVector).asBlock();
    }
  }

  public LongBlock eval(int positionCount, LongBlock fieldValBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      int accumulatedCost = 0;
      position: for (int p = 0; p < positionCount; p++) {
        if (fieldValBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (fieldValBlock.getValueCount(p) != 1) {
          if (fieldValBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        accumulatedCost += 1;
        if (accumulatedCost >= DriverContext.CHECK_FOR_EARLY_TERMINATION_COST_THRESHOLD) {
          accumulatedCost = 0;
          driverContext.checkForEarlyTermination();
        }
        result.appendLong(Abs.process(fieldValBlock.getLong(fieldValBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector fieldValVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      // generate a tight loop to allow vectorization
      int maxBatchSize = Math.max(DriverContext.CHECK_FOR_EARLY_TERMINATION_COST_THRESHOLD / 1, 1);
      for (int start = 0; start < positionCount; ) {
        int end = start + Math.min(positionCount - start, maxBatchSize);
        driverContext.checkForEarlyTermination();
        for (int p = start; p < end; p++) {
          result.appendLong(p, Abs.process(fieldValVector.getLong(p)));
        }
        start = end;
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "AbsLongEvaluator[" + "fieldVal=" + fieldVal + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(fieldVal);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory fieldVal;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory fieldVal) {
      this.source = source;
      this.fieldVal = fieldVal;
    }

    @Override
    public AbsLongEvaluator get(DriverContext context) {
      return new AbsLongEvaluator(source, fieldVal.get(context), context);
    }

    @Override
    public String toString() {
      return "AbsLongEvaluator[" + "fieldVal=" + fieldVal + "]";
    }
  }
}
