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
import org.elasticsearch.compute.data.LongBlock;
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
public final class RangeWithinPointEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RangeWithinPointEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator point;

  private final ExpressionEvaluator range;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RangeWithinPointEvaluator(Source source, ExpressionEvaluator point,
      ExpressionEvaluator range, DriverContext driverContext) {
    this.source = source;
    this.point = point;
    this.range = range;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock pointBlock = (LongBlock) point.eval(page)) {
      try (LongRangeBlock rangeBlock = (LongRangeBlock) range.eval(page)) {
        return eval(page.getPositionCount(), pointBlock, rangeBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += point.baseRamBytesUsed();
    baseRamBytesUsed += range.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock pointBlock, LongRangeBlock rangeBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      LongRangeBlockBuilder.LongRange rangeScratch = new LongRangeBlockBuilder.LongRange();
      position: for (int p = 0; p < positionCount; p++) {
        switch (pointBlock.getValueCount(p)) {
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
        switch (rangeBlock.getValueCount(p)) {
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
        long point = pointBlock.getLong(pointBlock.getFirstValueIndex(p));
        LongRangeBlockBuilder.LongRange range = rangeBlock.getLongRange(rangeBlock.getFirstValueIndex(p), rangeScratch);
        result.appendBoolean(RangeWithin.processPoint(point, range));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RangeWithinPointEvaluator[" + "point=" + point + ", range=" + range + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(point, range);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory point;

    private final ExpressionEvaluator.Factory range;

    public Factory(Source source, ExpressionEvaluator.Factory point,
        ExpressionEvaluator.Factory range) {
      this.source = source;
      this.point = point;
      this.range = range;
    }

    @Override
    public RangeWithinPointEvaluator get(DriverContext context) {
      return new RangeWithinPointEvaluator(source, point.get(context), range.get(context), context);
    }

    @Override
    public String toString() {
      return "RangeWithinPointEvaluator[" + "point=" + point + ", range=" + range + "]";
    }
  }
}
