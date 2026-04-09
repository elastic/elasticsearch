// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
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
 * {@link ExpressionEvaluator} implementation for {@link DateUnitCount}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateUnitCountNanosEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateUnitCountNanosEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator dstUnit;

  private final ExpressionEvaluator srcUnit;

  private final ExpressionEvaluator date;

  private final ZoneId zoneId;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateUnitCountNanosEvaluator(Source source, ExpressionEvaluator dstUnit,
      ExpressionEvaluator srcUnit, ExpressionEvaluator date, ZoneId zoneId,
      DriverContext driverContext) {
    this.source = source;
    this.dstUnit = dstUnit;
    this.srcUnit = srcUnit;
    this.date = date;
    this.zoneId = zoneId;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock dstUnitBlock = (BytesRefBlock) dstUnit.eval(page)) {
      try (BytesRefBlock srcUnitBlock = (BytesRefBlock) srcUnit.eval(page)) {
        try (LongBlock dateBlock = (LongBlock) date.eval(page)) {
          BytesRefVector dstUnitVector = dstUnitBlock.asVector();
          if (dstUnitVector == null) {
            return eval(page.getPositionCount(), dstUnitBlock, srcUnitBlock, dateBlock);
          }
          BytesRefVector srcUnitVector = srcUnitBlock.asVector();
          if (srcUnitVector == null) {
            return eval(page.getPositionCount(), dstUnitBlock, srcUnitBlock, dateBlock);
          }
          LongVector dateVector = dateBlock.asVector();
          if (dateVector == null) {
            return eval(page.getPositionCount(), dstUnitBlock, srcUnitBlock, dateBlock);
          }
          return eval(page.getPositionCount(), dstUnitVector, srcUnitVector, dateVector).asBlock();
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += dstUnit.baseRamBytesUsed();
    baseRamBytesUsed += srcUnit.baseRamBytesUsed();
    baseRamBytesUsed += date.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, BytesRefBlock dstUnitBlock, BytesRefBlock srcUnitBlock,
      LongBlock dateBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef dstUnitScratch = new BytesRef();
      BytesRef srcUnitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (dstUnitBlock.getValueCount(p)) {
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
        switch (srcUnitBlock.getValueCount(p)) {
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
        BytesRef dstUnit = dstUnitBlock.getBytesRef(dstUnitBlock.getFirstValueIndex(p), dstUnitScratch);
        BytesRef srcUnit = srcUnitBlock.getBytesRef(srcUnitBlock.getFirstValueIndex(p), srcUnitScratch);
        long date = dateBlock.getLong(dateBlock.getFirstValueIndex(p));
        result.appendLong(DateUnitCount.processNanos(dstUnit, srcUnit, date, this.zoneId));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, BytesRefVector dstUnitVector,
      BytesRefVector srcUnitVector, LongVector dateVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      BytesRef dstUnitScratch = new BytesRef();
      BytesRef srcUnitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef dstUnit = dstUnitVector.getBytesRef(p, dstUnitScratch);
        BytesRef srcUnit = srcUnitVector.getBytesRef(p, srcUnitScratch);
        long date = dateVector.getLong(p);
        result.appendLong(p, DateUnitCount.processNanos(dstUnit, srcUnit, date, this.zoneId));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateUnitCountNanosEvaluator[" + "dstUnit=" + dstUnit + ", srcUnit=" + srcUnit + ", date=" + date + ", zoneId=" + zoneId + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(dstUnit, srcUnit, date);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory dstUnit;

    private final ExpressionEvaluator.Factory srcUnit;

    private final ExpressionEvaluator.Factory date;

    private final ZoneId zoneId;

    public Factory(Source source, ExpressionEvaluator.Factory dstUnit,
        ExpressionEvaluator.Factory srcUnit, ExpressionEvaluator.Factory date, ZoneId zoneId) {
      this.source = source;
      this.dstUnit = dstUnit;
      this.srcUnit = srcUnit;
      this.date = date;
      this.zoneId = zoneId;
    }

    @Override
    public DateUnitCountNanosEvaluator get(DriverContext context) {
      return new DateUnitCountNanosEvaluator(source, dstUnit.get(context), srcUnit.get(context), date.get(context), zoneId, context);
    }

    @Override
    public String toString() {
      return "DateUnitCountNanosEvaluator[" + "dstUnit=" + dstUnit + ", srcUnit=" + srcUnit + ", date=" + date + ", zoneId=" + zoneId + "]";
    }
  }
}
