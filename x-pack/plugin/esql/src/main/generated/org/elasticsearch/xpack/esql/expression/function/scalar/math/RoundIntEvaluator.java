// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Round}.
 * This class is generated. Do not edit it.
 */
public final class RoundIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator decimals;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RoundIntEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator decimals, DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.decimals = decimals;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock valBlock = (IntBlock) val.eval(page)) {
      try (LongBlock decimalsBlock = (LongBlock) decimals.eval(page)) {
        IntVector valVector = valBlock.asVector();
        if (valVector == null) {
          return eval(page.getPositionCount(), valBlock, decimalsBlock);
        }
        LongVector decimalsVector = decimalsBlock.asVector();
        if (decimalsVector == null) {
          return eval(page.getPositionCount(), valBlock, decimalsBlock);
        }
        return eval(page.getPositionCount(), valVector, decimalsVector).asBlock();
      }
    }
  }

  public IntBlock eval(int positionCount, IntBlock valBlock, LongBlock decimalsBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      int accumulatedCost = 0;
      position: for (int p = 0; p < positionCount; p++) {
        if (valBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (valBlock.getValueCount(p) != 1) {
          if (valBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (decimalsBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (decimalsBlock.getValueCount(p) != 1) {
          if (decimalsBlock.getValueCount(p) > 1) {
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
        result.appendInt(Round.process(valBlock.getInt(valBlock.getFirstValueIndex(p)), decimalsBlock.getLong(decimalsBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, IntVector valVector, LongVector decimalsVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      // generate a tight loop to allow vectorization
      int maxBatchSize = Math.max(DriverContext.CHECK_FOR_EARLY_TERMINATION_COST_THRESHOLD / 1, 1);
      for (int start = 0; start < positionCount; ) {
        int end = start + Math.min(positionCount - start, maxBatchSize);
        driverContext.checkForEarlyTermination();
        for (int p = start; p < end; p++) {
          result.appendInt(p, Round.process(valVector.getInt(p), decimalsVector.getLong(p)));
        }
        start = end;
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RoundIntEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val, decimals);
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

    private final EvalOperator.ExpressionEvaluator.Factory val;

    private final EvalOperator.ExpressionEvaluator.Factory decimals;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val,
        EvalOperator.ExpressionEvaluator.Factory decimals) {
      this.source = source;
      this.val = val;
      this.decimals = decimals;
    }

    @Override
    public RoundIntEvaluator get(DriverContext context) {
      return new RoundIntEvaluator(source, val.get(context), decimals.get(context), context);
    }

    @Override
    public String toString() {
      return "RoundIntEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
    }
  }
}
