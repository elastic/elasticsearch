// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.BitSet;
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
 * {@link ExpressionEvaluator} implementation for {@link Pow}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class PowEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PowEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator base;

  private final ExpressionEvaluator exponent;

  private final boolean allowNonFinite;

  private final DriverContext driverContext;

  private Warnings warnings;

  private final DoubleSingleArrayPool pool;

  public PowEvaluator(Source source, ExpressionEvaluator base, ExpressionEvaluator exponent,
      boolean allowNonFinite, DriverContext driverContext) {
    this.source = source;
    this.base = base;
    this.exponent = exponent;
    this.allowNonFinite = allowNonFinite;
    this.driverContext = driverContext;
    this.pool = new DoubleSingleArrayPool(driverContext.breaker());
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock baseBlock = (DoubleBlock) base.eval(page)) {
      try (DoubleBlock exponentBlock = (DoubleBlock) exponent.eval(page)) {
        DoubleVector baseVector = baseBlock.asVector();
        if (baseVector == null) {
          return eval(page.getPositionCount(), baseBlock, exponentBlock);
        }
        DoubleVector exponentVector = exponentBlock.asVector();
        if (exponentVector == null) {
          return eval(page.getPositionCount(), baseBlock, exponentBlock);
        }
        return eval(page.getPositionCount(), baseVector, exponentVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += base.baseRamBytesUsed();
    baseRamBytesUsed += exponent.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock baseBlock, DoubleBlock exponentBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (baseBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (baseBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        if (exponentBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (exponentBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        double base = baseBlock.getDouble(baseBlock.getFirstValueIndex(p));
        double exponent = exponentBlock.getDouble(exponentBlock.getFirstValueIndex(p));
        try {
          result.appendDouble(Pow.process(base, exponent, this.allowNonFinite));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleVector baseVector, DoubleVector exponentVector) {
    final DoubleSingleArrayPool pool = this.pool;
    final double[] results = pool.getOrAllocate(positionCount);
    BitSet nulls = null;
    position: for (int p = 0; p < positionCount; p++) {
      double base = baseVector.getDouble(p);
      double exponent = exponentVector.getDouble(p);
      try {
        results[p] = Pow.process(base, exponent, this.allowNonFinite);
      } catch (ArithmeticException e) {
        warnings().registerException(e);
        if (nulls == null) {
          nulls = new BitSet(positionCount);
        }
        nulls.set(p);
      }
    }
    if (nulls != null) {
      DoubleBlock block = driverContext.blockFactory().newDoubleArrayBlock(results, positionCount, null, nulls, Block.MvOrdering.UNORDERED);
      block.attachReleasable(() -> pool.put(results));
      return block;
    }
    DoubleVector result = driverContext.blockFactory().newDoubleArrayVector(results, positionCount);
    result.attachReleasable(() -> pool.put(results));
    return result.asBlock();
  }

  @Override
  public String toString() {
    return "PowEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(base, exponent);
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

    private final ExpressionEvaluator.Factory base;

    private final ExpressionEvaluator.Factory exponent;

    private final boolean allowNonFinite;

    public Factory(Source source, ExpressionEvaluator.Factory base,
        ExpressionEvaluator.Factory exponent, boolean allowNonFinite) {
      this.source = source;
      this.base = base;
      this.exponent = exponent;
      this.allowNonFinite = allowNonFinite;
    }

    @Override
    public PowEvaluator get(DriverContext context) {
      return new PowEvaluator(source, base.get(context), exponent.get(context), allowNonFinite, context);
    }

    @Override
    public String toString() {
      return "PowEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
    }
  }
}
