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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToRange}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ToRangeLongEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToRangeLongEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator from;

  private final ExpressionEvaluator to;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ToRangeLongEvaluator(Source source, ExpressionEvaluator from, ExpressionEvaluator to,
      DriverContext driverContext) {
    this.source = source;
    this.from = from;
    this.to = to;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock fromBlock = (LongBlock) from.eval(page)) {
      try (LongBlock toBlock = (LongBlock) to.eval(page)) {
        return eval(page.getPositionCount(), fromBlock, toBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += from.baseRamBytesUsed();
    baseRamBytesUsed += to.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongRangeBlock eval(int positionCount, LongBlock fromBlock, LongBlock toBlock) {
    try(LongRangeBlock.Builder result = driverContext.blockFactory().newLongRangeBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (fromBlock.getValueCount(p)) {
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
        switch (toBlock.getValueCount(p)) {
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
        long from = fromBlock.getLong(fromBlock.getFirstValueIndex(p));
        long to = toBlock.getLong(toBlock.getFirstValueIndex(p));
        try {
          result.appendLongRange(ToRange.process(from, to));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ToRangeLongEvaluator[" + "from=" + from + ", to=" + to + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(from, to);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory from;

    private final ExpressionEvaluator.Factory to;

    public Factory(Source source, ExpressionEvaluator.Factory from,
        ExpressionEvaluator.Factory to) {
      this.source = source;
      this.from = from;
      this.to = to;
    }

    @Override
    public ToRangeLongEvaluator get(DriverContext context) {
      return new ToRangeLongEvaluator(source, from.get(context), to.get(context), context);
    }

    @Override
    public String toString() {
      return "ToRangeLongEvaluator[" + "from=" + from + ", to=" + to + "]";
    }
  }
}
