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
 * {@link ExpressionEvaluator} implementation for {@link DaysInMonth}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DaysInMonthMillisEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DaysInMonthMillisEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator val;

  private final ZoneId zoneId;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DaysInMonthMillisEvaluator(Source source, ExpressionEvaluator val, ZoneId zoneId,
      DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.zoneId = zoneId;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock valBlock = (LongBlock) val.eval(page)) {
      LongVector valVector = valBlock.asVector();
      if (valVector == null) {
        return eval(page.getPositionCount(), valBlock);
      }
      return eval(page.getPositionCount(), valVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += val.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock valBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (valBlock.getValueCount(p)) {
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
        long val = valBlock.getLong(valBlock.getFirstValueIndex(p));
        result.appendLong(DaysInMonth.processMillis(val, this.zoneId));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector valVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long val = valVector.getLong(p);
        result.appendLong(p, DaysInMonth.processMillis(val, this.zoneId));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DaysInMonthMillisEvaluator[" + "val=" + val + ", zoneId=" + zoneId + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory val;

    private final ZoneId zoneId;

    public Factory(Source source, ExpressionEvaluator.Factory val, ZoneId zoneId) {
      this.source = source;
      this.val = val;
      this.zoneId = zoneId;
    }

    @Override
    public DaysInMonthMillisEvaluator get(DriverContext context) {
      return new DaysInMonthMillisEvaluator(source, val.get(context), zoneId, context);
    }

    @Override
    public String toString() {
      return "DaysInMonthMillisEvaluator[" + "val=" + val + ", zoneId=" + zoneId + "]";
    }
  }
}
