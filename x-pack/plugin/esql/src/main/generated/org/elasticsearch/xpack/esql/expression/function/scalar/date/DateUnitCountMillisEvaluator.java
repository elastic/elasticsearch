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
public final class DateUnitCountMillisEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateUnitCountMillisEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator toUnit;

  private final ExpressionEvaluator fromUnit;

  private final ExpressionEvaluator date;

  private final ZoneId zoneId;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateUnitCountMillisEvaluator(Source source, ExpressionEvaluator toUnit,
      ExpressionEvaluator fromUnit, ExpressionEvaluator date, ZoneId zoneId,
      DriverContext driverContext) {
    this.source = source;
    this.toUnit = toUnit;
    this.fromUnit = fromUnit;
    this.date = date;
    this.zoneId = zoneId;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock toUnitBlock = (BytesRefBlock) toUnit.eval(page)) {
      try (BytesRefBlock fromUnitBlock = (BytesRefBlock) fromUnit.eval(page)) {
        try (LongBlock dateBlock = (LongBlock) date.eval(page)) {
          BytesRefVector toUnitVector = toUnitBlock.asVector();
          if (toUnitVector == null) {
            return eval(page.getPositionCount(), toUnitBlock, fromUnitBlock, dateBlock);
          }
          BytesRefVector fromUnitVector = fromUnitBlock.asVector();
          if (fromUnitVector == null) {
            return eval(page.getPositionCount(), toUnitBlock, fromUnitBlock, dateBlock);
          }
          LongVector dateVector = dateBlock.asVector();
          if (dateVector == null) {
            return eval(page.getPositionCount(), toUnitBlock, fromUnitBlock, dateBlock);
          }
          return eval(page.getPositionCount(), toUnitVector, fromUnitVector, dateVector).asBlock();
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += toUnit.baseRamBytesUsed();
    baseRamBytesUsed += fromUnit.baseRamBytesUsed();
    baseRamBytesUsed += date.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, BytesRefBlock toUnitBlock, BytesRefBlock fromUnitBlock,
      LongBlock dateBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef toUnitScratch = new BytesRef();
      BytesRef fromUnitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (toUnitBlock.getValueCount(p)) {
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
        switch (fromUnitBlock.getValueCount(p)) {
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
        BytesRef toUnit = toUnitBlock.getBytesRef(toUnitBlock.getFirstValueIndex(p), toUnitScratch);
        BytesRef fromUnit = fromUnitBlock.getBytesRef(fromUnitBlock.getFirstValueIndex(p), fromUnitScratch);
        long date = dateBlock.getLong(dateBlock.getFirstValueIndex(p));
        result.appendLong(DateUnitCount.processMillis(toUnit, fromUnit, date, this.zoneId));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, BytesRefVector toUnitVector,
      BytesRefVector fromUnitVector, LongVector dateVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      BytesRef toUnitScratch = new BytesRef();
      BytesRef fromUnitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef toUnit = toUnitVector.getBytesRef(p, toUnitScratch);
        BytesRef fromUnit = fromUnitVector.getBytesRef(p, fromUnitScratch);
        long date = dateVector.getLong(p);
        result.appendLong(p, DateUnitCount.processMillis(toUnit, fromUnit, date, this.zoneId));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateUnitCountMillisEvaluator[" + "toUnit=" + toUnit + ", fromUnit=" + fromUnit + ", date=" + date + ", zoneId=" + zoneId + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(toUnit, fromUnit, date);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory toUnit;

    private final ExpressionEvaluator.Factory fromUnit;

    private final ExpressionEvaluator.Factory date;

    private final ZoneId zoneId;

    public Factory(Source source, ExpressionEvaluator.Factory toUnit,
        ExpressionEvaluator.Factory fromUnit, ExpressionEvaluator.Factory date, ZoneId zoneId) {
      this.source = source;
      this.toUnit = toUnit;
      this.fromUnit = fromUnit;
      this.date = date;
      this.zoneId = zoneId;
    }

    @Override
    public DateUnitCountMillisEvaluator get(DriverContext context) {
      return new DateUnitCountMillisEvaluator(source, toUnit.get(context), fromUnit.get(context), date.get(context), zoneId, context);
    }

    @Override
    public String toString() {
      return "DateUnitCountMillisEvaluator[" + "toUnit=" + toUnit + ", fromUnit=" + fromUnit + ", date=" + date + ", zoneId=" + zoneId + "]";
    }
  }
}
