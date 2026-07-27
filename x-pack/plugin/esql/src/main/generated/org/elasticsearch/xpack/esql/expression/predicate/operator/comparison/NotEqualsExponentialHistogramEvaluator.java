// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link NotEquals}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class NotEqualsExponentialHistogramEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NotEqualsExponentialHistogramEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator lhs;

  private final ExpressionEvaluator rhs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public NotEqualsExponentialHistogramEvaluator(Source source, ExpressionEvaluator lhs,
      ExpressionEvaluator rhs, DriverContext driverContext) {
    this.source = source;
    this.lhs = lhs;
    this.rhs = rhs;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (ExponentialHistogramBlock lhsBlock = (ExponentialHistogramBlock) lhs.eval(page)) {
      try (ExponentialHistogramBlock rhsBlock = (ExponentialHistogramBlock) rhs.eval(page)) {
        return eval(page.getPositionCount(), lhsBlock, rhsBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += lhs.baseRamBytesUsed();
    baseRamBytesUsed += rhs.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, ExponentialHistogramBlock lhsBlock,
      ExponentialHistogramBlock rhsBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      ExponentialHistogramScratch lhsScratch = new ExponentialHistogramScratch();
      ExponentialHistogramScratch rhsScratch = new ExponentialHistogramScratch();
      position: for (int p = 0; p < positionCount; p++) {
        switch (lhsBlock.getValueCount(p)) {
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
        switch (rhsBlock.getValueCount(p)) {
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
        ExponentialHistogram lhs = lhsBlock.getExponentialHistogram(lhsBlock.getFirstValueIndex(p), lhsScratch);
        ExponentialHistogram rhs = rhsBlock.getExponentialHistogram(rhsBlock.getFirstValueIndex(p), rhsScratch);
        result.appendBoolean(NotEquals.processExponentialHistogram(lhs, rhs));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NotEqualsExponentialHistogramEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(lhs, rhs);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory lhs;

    private final ExpressionEvaluator.Factory rhs;

    public Factory(Source source, ExpressionEvaluator.Factory lhs,
        ExpressionEvaluator.Factory rhs) {
      this.source = source;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public NotEqualsExponentialHistogramEvaluator get(DriverContext context) {
      return new NotEqualsExponentialHistogramEvaluator(source, lhs.get(context), rhs.get(context), context);
    }

    @Override
    public String toString() {
      return "NotEqualsExponentialHistogramEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }
}
