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
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Equals}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class EqualsLongRangeEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(EqualsLongRangeEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator lhs;

  private final ExpressionEvaluator rhs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public EqualsLongRangeEvaluator(Source source, ExpressionEvaluator lhs, ExpressionEvaluator rhs,
      DriverContext driverContext) {
    this.source = source;
    this.lhs = lhs;
    this.rhs = rhs;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongRangeBlock lhsBlock = (LongRangeBlock) lhs.eval(page)) {
      try (LongRangeBlock rhsBlock = (LongRangeBlock) rhs.eval(page)) {
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

  public BooleanBlock eval(int positionCount, LongRangeBlock lhsBlock, LongRangeBlock rhsBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      LongRangeBlockBuilder.LongRange lhsScratch = new LongRangeBlockBuilder.LongRange();
      LongRangeBlockBuilder.LongRange rhsScratch = new LongRangeBlockBuilder.LongRange();
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
        LongRangeBlockBuilder.LongRange lhs = lhsBlock.getLongRange(lhsBlock.getFirstValueIndex(p), lhsScratch);
        LongRangeBlockBuilder.LongRange rhs = rhsBlock.getLongRange(rhsBlock.getFirstValueIndex(p), rhsScratch);
        result.appendBoolean(Equals.processLongRange(lhs, rhs));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "EqualsLongRangeEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
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
    public EqualsLongRangeEvaluator get(DriverContext context) {
      return new EqualsLongRangeEvaluator(source, lhs.get(context), rhs.get(context), context);
    }

    @Override
    public String toString() {
      return "EqualsLongRangeEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }
}
