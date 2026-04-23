// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.time.ZoneId;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
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
public final class DateUnitCountConstantNanosEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateUnitCountConstantNanosEvaluator.class);

  private final Source source;

  private final DateDiff.Part toUnit;

  private final DateDiff.Part fromUnit;

  private final ExpressionEvaluator date;

  private final ZoneId zoneId;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateUnitCountConstantNanosEvaluator(Source source, DateDiff.Part toUnit,
      DateDiff.Part fromUnit, ExpressionEvaluator date, ZoneId zoneId,
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
    try (LongBlock dateBlock = (LongBlock) date.eval(page)) {
      LongVector dateVector = dateBlock.asVector();
      if (dateVector == null) {
        return eval(page.getPositionCount(), dateBlock);
      }
      return eval(page.getPositionCount(), dateVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += date.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock dateBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
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
        long date = dateBlock.getLong(dateBlock.getFirstValueIndex(p));
        result.appendLong(DateUnitCount.processNanos(this.toUnit, this.fromUnit, date, this.zoneId));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector dateVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long date = dateVector.getLong(p);
        result.appendLong(p, DateUnitCount.processNanos(this.toUnit, this.fromUnit, date, this.zoneId));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateUnitCountConstantNanosEvaluator[" + "toUnit=" + toUnit + ", fromUnit=" + fromUnit + ", date=" + date + ", zoneId=" + zoneId + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(date);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final DateDiff.Part toUnit;

    private final DateDiff.Part fromUnit;

    private final ExpressionEvaluator.Factory date;

    private final ZoneId zoneId;

    public Factory(Source source, DateDiff.Part toUnit, DateDiff.Part fromUnit,
        ExpressionEvaluator.Factory date, ZoneId zoneId) {
      this.source = source;
      this.toUnit = toUnit;
      this.fromUnit = fromUnit;
      this.date = date;
      this.zoneId = zoneId;
    }

    @Override
    public DateUnitCountConstantNanosEvaluator get(DriverContext context) {
      return new DateUnitCountConstantNanosEvaluator(source, toUnit, fromUnit, date.get(context), zoneId, context);
    }

    @Override
    public String toString() {
      return "DateUnitCountConstantNanosEvaluator[" + "toUnit=" + toUnit + ", fromUnit=" + fromUnit + ", date=" + date + ", zoneId=" + zoneId + "]";
    }
  }
}
