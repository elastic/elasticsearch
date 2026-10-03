// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link FmtDuration}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class FmtDurationFromIntEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FmtDurationFromIntEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator nanoseconds;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FmtDurationFromIntEvaluator(Source source, ExpressionEvaluator nanoseconds,
      DriverContext driverContext) {
    this.source = source;
    this.nanoseconds = nanoseconds;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock nanosecondsBlock = (IntBlock) nanoseconds.eval(page)) {
      IntVector nanosecondsVector = nanosecondsBlock.asVector();
      if (nanosecondsVector == null) {
        return eval(page.getPositionCount(), nanosecondsBlock);
      }
      return eval(page.getPositionCount(), nanosecondsVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += nanoseconds.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, IntBlock nanosecondsBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (nanosecondsBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (nanosecondsBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        int nanoseconds = nanosecondsBlock.getInt(nanosecondsBlock.getFirstValueIndex(p));
        try {
          result.appendBytesRef(FmtDuration.processInt(nanoseconds));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, IntVector nanosecondsVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int nanoseconds = nanosecondsVector.getInt(p);
        try {
          result.appendBytesRef(FmtDuration.processInt(nanoseconds));
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
    return "FmtDurationFromIntEvaluator[" + "nanoseconds=" + nanoseconds + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(nanoseconds);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory nanoseconds;

    public Factory(Source source, ExpressionEvaluator.Factory nanoseconds) {
      this.source = source;
      this.nanoseconds = nanoseconds;
    }

    @Override
    public FmtDurationFromIntEvaluator get(DriverContext context) {
      return new FmtDurationFromIntEvaluator(source, nanoseconds.get(context), context);
    }

    @Override
    public String toString() {
      return "FmtDurationFromIntEvaluator[" + "nanoseconds=" + nanoseconds + "]";
    }
  }
}
