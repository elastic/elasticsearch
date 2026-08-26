// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
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
public final class FmtDurationFromLongWithUnitEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FmtDurationFromLongWithUnitEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator nanoseconds;

  private final ExpressionEvaluator unit;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FmtDurationFromLongWithUnitEvaluator(Source source, ExpressionEvaluator nanoseconds,
      ExpressionEvaluator unit, DriverContext driverContext) {
    this.source = source;
    this.nanoseconds = nanoseconds;
    this.unit = unit;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock nanosecondsBlock = (LongBlock) nanoseconds.eval(page)) {
      try (BytesRefBlock unitBlock = (BytesRefBlock) unit.eval(page)) {
        LongVector nanosecondsVector = nanosecondsBlock.asVector();
        if (nanosecondsVector == null) {
          return eval(page.getPositionCount(), nanosecondsBlock, unitBlock);
        }
        BytesRefVector unitVector = unitBlock.asVector();
        if (unitVector == null) {
          return eval(page.getPositionCount(), nanosecondsBlock, unitBlock);
        }
        return eval(page.getPositionCount(), nanosecondsVector, unitVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += nanoseconds.baseRamBytesUsed();
    baseRamBytesUsed += unit.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, LongBlock nanosecondsBlock,
      BytesRefBlock unitBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (nanosecondsBlock.getValueCount(p)) {
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
        switch (unitBlock.getValueCount(p)) {
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
        long nanoseconds = nanosecondsBlock.getLong(nanosecondsBlock.getFirstValueIndex(p));
        BytesRef unit = unitBlock.getBytesRef(unitBlock.getFirstValueIndex(p), unitScratch);
        try {
          result.appendBytesRef(FmtDuration.processLongWithUnit(nanoseconds, unit));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, LongVector nanosecondsVector,
      BytesRefVector unitVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        long nanoseconds = nanosecondsVector.getLong(p);
        BytesRef unit = unitVector.getBytesRef(p, unitScratch);
        try {
          result.appendBytesRef(FmtDuration.processLongWithUnit(nanoseconds, unit));
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
    return "FmtDurationFromLongWithUnitEvaluator[" + "nanoseconds=" + nanoseconds + ", unit=" + unit + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(nanoseconds, unit);
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

    private final ExpressionEvaluator.Factory unit;

    public Factory(Source source, ExpressionEvaluator.Factory nanoseconds,
        ExpressionEvaluator.Factory unit) {
      this.source = source;
      this.nanoseconds = nanoseconds;
      this.unit = unit;
    }

    @Override
    public FmtDurationFromLongWithUnitEvaluator get(DriverContext context) {
      return new FmtDurationFromLongWithUnitEvaluator(source, nanoseconds.get(context), unit.get(context), context);
    }

    @Override
    public String toString() {
      return "FmtDurationFromLongWithUnitEvaluator[" + "nanoseconds=" + nanoseconds + ", unit=" + unit + "]";
    }
  }
}
