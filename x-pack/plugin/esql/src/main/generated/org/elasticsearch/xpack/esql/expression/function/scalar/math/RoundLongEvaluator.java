// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Round}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RoundLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RoundLongEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator decimals;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RoundLongEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator decimals, DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.decimals = decimals;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock valBlock = (LongBlock) val.eval(page)) {
      try (LongBlock decimalsBlock = (LongBlock) decimals.eval(page)) {
        LongVector valVector = valBlock.asVector();
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

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += val.baseRamBytesUsed();
    baseRamBytesUsed += decimals.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock valBlock, LongBlock decimalsBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (valBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        switch (decimalsBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        long val = valBlock.getLong(valBlock.getFirstValueIndex(p));
        long decimals = decimalsBlock.getLong(decimalsBlock.getFirstValueIndex(p));
        result.appendLong(Round.process(val, decimals));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector valVector, LongVector decimalsVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long val = valVector.getLong(p);
        long decimals = decimalsVector.getLong(p);
        result.appendLong(p, Round.process(val, decimals));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RoundLongEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
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
    public RoundLongEvaluator get(DriverContext context) {
      return new RoundLongEvaluator(source, val.get(context), decimals.get(context), context);
    }

    @Override
    public String toString() {
      return "RoundLongEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
    }
  }
}
