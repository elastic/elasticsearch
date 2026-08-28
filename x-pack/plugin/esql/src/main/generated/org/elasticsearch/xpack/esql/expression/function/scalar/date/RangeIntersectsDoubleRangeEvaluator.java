// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link RangeIntersects}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RangeIntersectsDoubleRangeEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RangeIntersectsDoubleRangeEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator a;

  private final ExpressionEvaluator b;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RangeIntersectsDoubleRangeEvaluator(Source source, ExpressionEvaluator a,
      ExpressionEvaluator b, DriverContext driverContext) {
    this.source = source;
    this.a = a;
    this.b = b;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleRangeBlock aBlock = (DoubleRangeBlock) a.eval(page)) {
      try (DoubleRangeBlock bBlock = (DoubleRangeBlock) b.eval(page)) {
        return eval(page.getPositionCount(), aBlock, bBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += a.baseRamBytesUsed();
    baseRamBytesUsed += b.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, DoubleRangeBlock aBlock, DoubleRangeBlock bBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      DoubleRangeBlockBuilder.DoubleRange aScratch = new DoubleRangeBlockBuilder.DoubleRange();
      DoubleRangeBlockBuilder.DoubleRange bScratch = new DoubleRangeBlockBuilder.DoubleRange();
      position: for (int p = 0; p < positionCount; p++) {
        if (aBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (aBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        if (bBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (bBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        DoubleRangeBlockBuilder.DoubleRange a = aBlock.getDoubleRange(aBlock.getFirstValueIndex(p), aScratch);
        DoubleRangeBlockBuilder.DoubleRange b = bBlock.getDoubleRange(bBlock.getFirstValueIndex(p), bScratch);
        result.appendBoolean(RangeIntersects.processRange(a, b));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RangeIntersectsDoubleRangeEvaluator[" + "a=" + a + ", b=" + b + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(a, b);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory a;

    private final ExpressionEvaluator.Factory b;

    public Factory(Source source, ExpressionEvaluator.Factory a, ExpressionEvaluator.Factory b) {
      this.source = source;
      this.a = a;
      this.b = b;
    }

    @Override
    public RangeIntersectsDoubleRangeEvaluator get(DriverContext context) {
      return new RangeIntersectsDoubleRangeEvaluator(source, a.get(context), b.get(context), context);
    }

    @Override
    public String toString() {
      return "RangeIntersectsDoubleRangeEvaluator[" + "a=" + a + ", b=" + b + "]";
    }
  }
}
