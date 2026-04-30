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
public final class RangeWithinDateDateRangeEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RangeWithinDateDateRangeEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator date;

  private final ExpressionEvaluator range;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RangeWithinDateDateRangeEvaluator(Source source, ExpressionEvaluator date,
      ExpressionEvaluator range, DriverContext driverContext) {
    this.source = source;
    this.date = date;
    this.range = range;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock dateBlock = (LongBlock) date.eval(page)) {
      try (LongRangeBlock rangeBlock = (LongRangeBlock) range.eval(page)) {
        return eval(page.getPositionCount(), dateBlock, rangeBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += date.baseRamBytesUsed();
    baseRamBytesUsed += range.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock dateBlock, LongRangeBlock rangeBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (dateBlock.getValueCount(p)) {
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
        long date = dateBlock.getLong(dateBlock.getFirstValueIndex(p));
        LongRangeBlockBuilder.LongRange range = new LongRangeBlockBuilder.LongRange(rangeBlock.getFromBlock().getLong(rangeBlock.getFirstValueIndex(p)), rangeBlock.getToBlock().getLong(rangeBlock.getFirstValueIndex(p)));
        result.appendBoolean(RangeWithin.processDateDateRange(date, range));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RangeWithinDateDateRangeEvaluator[" + "date=" + date + ", range=" + range + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(date, range);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory date;

    private final ExpressionEvaluator.Factory range;

    public Factory(Source source, ExpressionEvaluator.Factory date,
        ExpressionEvaluator.Factory range) {
      this.source = source;
      this.date = date;
      this.range = range;
    }

    @Override
    public RangeWithinDateDateRangeEvaluator get(DriverContext context) {
      return new RangeWithinDateDateRangeEvaluator(source, date.get(context), range.get(context), context);
    }

    @Override
    public String toString() {
      return "RangeWithinDateDateRangeEvaluator[" + "date=" + date + ", range=" + range + "]";
    }
  }
}
