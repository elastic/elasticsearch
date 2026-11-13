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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleSingleArrayPool;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Atan2}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class Atan2Evaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Atan2Evaluator.class);

  private final Source source;

  private final ExpressionEvaluator y;

  private final ExpressionEvaluator x;

  private final DriverContext driverContext;

  private Warnings warnings;

  private final DoubleSingleArrayPool pool;

  public Atan2Evaluator(Source source, ExpressionEvaluator y, ExpressionEvaluator x,
      DriverContext driverContext) {
    this.source = source;
    this.y = y;
    this.x = x;
    this.driverContext = driverContext;
    this.pool = new DoubleSingleArrayPool(driverContext.breaker());
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock yBlock = (DoubleBlock) y.eval(page)) {
      try (DoubleBlock xBlock = (DoubleBlock) x.eval(page)) {
        DoubleVector yVector = yBlock.asVector();
        if (yVector == null) {
          return eval(page.getPositionCount(), yBlock, xBlock);
        }
        DoubleVector xVector = xBlock.asVector();
        if (xVector == null) {
          return eval(page.getPositionCount(), yBlock, xBlock);
        }
        return eval(page.getPositionCount(), yVector, xVector).asBlock();
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += y.baseRamBytesUsed();
    baseRamBytesUsed += x.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock yBlock, DoubleBlock xBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (yBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (yBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        if (xBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (xBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        double y = yBlock.getDouble(yBlock.getFirstValueIndex(p));
        double x = xBlock.getDouble(xBlock.getFirstValueIndex(p));
        result.appendDouble(Atan2.process(y, x));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, DoubleVector yVector, DoubleVector xVector) {
    final DoubleSingleArrayPool pool = this.pool;
    final double[] results = pool.getOrAllocate(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      double y = yVector.getDouble(p);
      double x = xVector.getDouble(p);
      results[p] = Atan2.process(y, x);
    }
    DoubleVector result = driverContext.blockFactory().newDoubleArrayVector(results, positionCount);
    result.attachReleasable(() -> pool.put(results));
    return result;
  }

  @Override
  public String toString() {
    return "Atan2Evaluator[" + "y=" + y + ", x=" + x + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(y, x);
    Releasables.closeExpectNoException(pool);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory y;

    private final ExpressionEvaluator.Factory x;

    public Factory(Source source, ExpressionEvaluator.Factory y, ExpressionEvaluator.Factory x) {
      this.source = source;
      this.y = y;
      this.x = x;
    }

    @Override
    public Atan2Evaluator get(DriverContext context) {
      return new Atan2Evaluator(source, y.get(context), x.get(context), context);
    }

    @Override
    public String toString() {
      return "Atan2Evaluator[" + "y=" + y + ", x=" + x + "]";
    }
  }
}
