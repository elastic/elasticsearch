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
 * {@link ExpressionEvaluator} implementation for {@link RangeMax}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RangeMaxEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RangeMaxEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator range;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RangeMaxEvaluator(Source source, ExpressionEvaluator range, DriverContext driverContext) {
    this.source = source;
    this.range = range;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongRangeBlock rangeBlock = (LongRangeBlock) range.eval(page)) {
      return eval(page.getPositionCount(), rangeBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += range.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongRangeBlock rangeBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      LongRangeBlockBuilder.LongRange rangeScratch = new LongRangeBlockBuilder.LongRange();
      position: for (int p = 0; p < positionCount; p++) {
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
        LongRangeBlockBuilder.LongRange range = rangeBlock.getLongRange(rangeBlock.getFirstValueIndex(p), rangeScratch);
        result.appendLong(RangeMax.process(range));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RangeMaxEvaluator[" + "range=" + range + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(range);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory range;

    public Factory(Source source, ExpressionEvaluator.Factory range) {
      this.source = source;
      this.range = range;
    }

    @Override
    public RangeMaxEvaluator get(DriverContext context) {
      return new RangeMaxEvaluator(source, range.get(context), context);
    }

    @Override
    public String toString() {
      return "RangeMaxEvaluator[" + "range=" + range + "]";
    }
  }
}
