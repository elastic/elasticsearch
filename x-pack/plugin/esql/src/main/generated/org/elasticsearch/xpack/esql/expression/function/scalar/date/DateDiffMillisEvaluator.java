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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link DateDiff}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class DateDiffMillisEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateDiffMillisEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator unit;

  private final EvalOperator.ExpressionEvaluator startTimestamp;

  private final EvalOperator.ExpressionEvaluator endTimestamp;

  private final ZoneId zoneId;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateDiffMillisEvaluator(Source source, EvalOperator.ExpressionEvaluator unit,
      EvalOperator.ExpressionEvaluator startTimestamp,
      EvalOperator.ExpressionEvaluator endTimestamp, ZoneId zoneId, DriverContext driverContext) {
    this.source = source;
    this.unit = unit;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.zoneId = zoneId;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock unitBlock = (BytesRefBlock) unit.eval(page)) {
      try (LongBlock startTimestampBlock = (LongBlock) startTimestamp.eval(page)) {
        try (LongBlock endTimestampBlock = (LongBlock) endTimestamp.eval(page)) {
          BytesRefVector unitVector = unitBlock.asVector();
          if (unitVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampBlock, endTimestampBlock);
          }
          LongVector startTimestampVector = startTimestampBlock.asVector();
          if (startTimestampVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampBlock, endTimestampBlock);
          }
          LongVector endTimestampVector = endTimestampBlock.asVector();
          if (endTimestampVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampBlock, endTimestampBlock);
          }
          return eval(page.getPositionCount(), unitVector, startTimestampVector, endTimestampVector);
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += unit.baseRamBytesUsed();
    baseRamBytesUsed += startTimestamp.baseRamBytesUsed();
    baseRamBytesUsed += endTimestamp.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, BytesRefBlock unitBlock, LongBlock startTimestampBlock,
      LongBlock endTimestampBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
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
        switch (startTimestampBlock.getValueCount(p)) {
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
        switch (endTimestampBlock.getValueCount(p)) {
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
        BytesRef unit = unitBlock.getBytesRef(unitBlock.getFirstValueIndex(p), unitScratch);
        long startTimestamp = startTimestampBlock.getLong(startTimestampBlock.getFirstValueIndex(p));
        long endTimestamp = endTimestampBlock.getLong(endTimestampBlock.getFirstValueIndex(p));
        try {
          result.appendInt(DateDiff.processMillis(unit, startTimestamp, endTimestamp, this.zoneId));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public IntBlock eval(int positionCount, BytesRefVector unitVector,
      LongVector startTimestampVector, LongVector endTimestampVector) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef unit = unitVector.getBytesRef(p, unitScratch);
        long startTimestamp = startTimestampVector.getLong(p);
        long endTimestamp = endTimestampVector.getLong(p);
        try {
          result.appendInt(DateDiff.processMillis(unit, startTimestamp, endTimestamp, this.zoneId));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DateDiffMillisEvaluator[" + "unit=" + unit + ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + ", zoneId=" + zoneId + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(unit, startTimestamp, endTimestamp);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory unit;

    private final EvalOperator.ExpressionEvaluator.Factory startTimestamp;

    private final EvalOperator.ExpressionEvaluator.Factory endTimestamp;

    private final ZoneId zoneId;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory unit,
        EvalOperator.ExpressionEvaluator.Factory startTimestamp,
        EvalOperator.ExpressionEvaluator.Factory endTimestamp, ZoneId zoneId) {
      this.source = source;
      this.unit = unit;
      this.startTimestamp = startTimestamp;
      this.endTimestamp = endTimestamp;
      this.zoneId = zoneId;
    }

    @Override
    public DateDiffMillisEvaluator get(DriverContext context) {
      return new DateDiffMillisEvaluator(source, unit.get(context), startTimestamp.get(context), endTimestamp.get(context), zoneId, context);
    }

    @Override
    public String toString() {
      return "DateDiffMillisEvaluator[" + "unit=" + unit + ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + ", zoneId=" + zoneId + "]";
    }
  }
}
