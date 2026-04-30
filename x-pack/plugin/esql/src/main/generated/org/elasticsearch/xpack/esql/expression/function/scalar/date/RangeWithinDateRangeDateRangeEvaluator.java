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
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link RangeWithin}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RangeWithinDateRangeDateRangeEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RangeWithinDateRangeDateRangeEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator left;

  private final ExpressionEvaluator right;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RangeWithinDateRangeDateRangeEvaluator(Source source, ExpressionEvaluator left,
      ExpressionEvaluator right, DriverContext driverContext) {
    this.source = source;
    this.left = left;
    this.right = right;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongRangeBlock leftBlock = (LongRangeBlock) left.eval(page)) {
      try (LongRangeBlock rightBlock = (LongRangeBlock) right.eval(page)) {
        return eval(page.getPositionCount(), leftBlock, rightBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += left.baseRamBytesUsed();
    baseRamBytesUsed += right.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongRangeBlock leftBlock, LongRangeBlock rightBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (leftBlock.getValueCount(p)) {
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
        switch (rightBlock.getValueCount(p)) {
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
        LongRangeBlockBuilder.LongRange left = new LongRangeBlockBuilder.LongRange(leftBlock.getFromBlock().getLong(leftBlock.getFirstValueIndex(p)), leftBlock.getToBlock().getLong(leftBlock.getFirstValueIndex(p)));
        LongRangeBlockBuilder.LongRange right = new LongRangeBlockBuilder.LongRange(rightBlock.getFromBlock().getLong(rightBlock.getFirstValueIndex(p)), rightBlock.getToBlock().getLong(rightBlock.getFirstValueIndex(p)));
        result.appendBoolean(RangeWithin.processDateRangeDateRange(left, right));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RangeWithinDateRangeDateRangeEvaluator[" + "left=" + left + ", right=" + right + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(left, right);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory left;

    private final ExpressionEvaluator.Factory right;

    public Factory(Source source, ExpressionEvaluator.Factory left,
        ExpressionEvaluator.Factory right) {
      this.source = source;
      this.left = left;
      this.right = right;
    }

    @Override
    public RangeWithinDateRangeDateRangeEvaluator get(DriverContext context) {
      return new RangeWithinDateRangeDateRangeEvaluator(source, left.get(context), right.get(context), context);
    }

    @Override
    public String toString() {
      return "RangeWithinDateRangeDateRangeEvaluator[" + "left=" + left + ", right=" + right + "]";
    }
  }
}
