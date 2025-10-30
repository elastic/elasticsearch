// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
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
public final class DateDiffMillisNanosEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateDiffMillisNanosEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator unit;

  private final EvalOperator.ExpressionEvaluator startTimestampMillis;

  private final EvalOperator.ExpressionEvaluator endTimestampNanos;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateDiffMillisNanosEvaluator(Source source, EvalOperator.ExpressionEvaluator unit,
      EvalOperator.ExpressionEvaluator startTimestampMillis,
      EvalOperator.ExpressionEvaluator endTimestampNanos, DriverContext driverContext) {
    this.source = source;
    this.unit = unit;
    this.startTimestampMillis = startTimestampMillis;
    this.endTimestampNanos = endTimestampNanos;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock unitBlock = (BytesRefBlock) unit.eval(page)) {
      try (LongBlock startTimestampMillisBlock = (LongBlock) startTimestampMillis.eval(page)) {
        try (LongBlock endTimestampNanosBlock = (LongBlock) endTimestampNanos.eval(page)) {
          BytesRefVector unitVector = unitBlock.asVector();
          if (unitVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampMillisBlock, endTimestampNanosBlock);
          }
          LongVector startTimestampMillisVector = startTimestampMillisBlock.asVector();
          if (startTimestampMillisVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampMillisBlock, endTimestampNanosBlock);
          }
          LongVector endTimestampNanosVector = endTimestampNanosBlock.asVector();
          if (endTimestampNanosVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampMillisBlock, endTimestampNanosBlock);
          }
          return eval(page.getPositionCount(), unitVector, startTimestampMillisVector, endTimestampNanosVector);
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += unit.baseRamBytesUsed();
    baseRamBytesUsed += startTimestampMillis.baseRamBytesUsed();
    baseRamBytesUsed += endTimestampNanos.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, BytesRefBlock unitBlock,
      LongBlock startTimestampMillisBlock, LongBlock endTimestampNanosBlock) {
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
        switch (startTimestampMillisBlock.getValueCount(p)) {
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
        switch (endTimestampNanosBlock.getValueCount(p)) {
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
        long startTimestampMillis = startTimestampMillisBlock.getLong(startTimestampMillisBlock.getFirstValueIndex(p));
        long endTimestampNanos = endTimestampNanosBlock.getLong(endTimestampNanosBlock.getFirstValueIndex(p));
        try {
          result.appendInt(DateDiff.processMillisNanos(unit, startTimestampMillis, endTimestampNanos));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public IntBlock eval(int positionCount, BytesRefVector unitVector,
      LongVector startTimestampMillisVector, LongVector endTimestampNanosVector) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef unit = unitVector.getBytesRef(p, unitScratch);
        long startTimestampMillis = startTimestampMillisVector.getLong(p);
        long endTimestampNanos = endTimestampNanosVector.getLong(p);
        try {
          result.appendInt(DateDiff.processMillisNanos(unit, startTimestampMillis, endTimestampNanos));
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
    return "DateDiffMillisNanosEvaluator[" + "unit=" + unit + ", startTimestampMillis=" + startTimestampMillis + ", endTimestampNanos=" + endTimestampNanos + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(unit, startTimestampMillis, endTimestampNanos);
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

    private final EvalOperator.ExpressionEvaluator.Factory startTimestampMillis;

    private final EvalOperator.ExpressionEvaluator.Factory endTimestampNanos;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory unit,
        EvalOperator.ExpressionEvaluator.Factory startTimestampMillis,
        EvalOperator.ExpressionEvaluator.Factory endTimestampNanos) {
      this.source = source;
      this.unit = unit;
      this.startTimestampMillis = startTimestampMillis;
      this.endTimestampNanos = endTimestampNanos;
    }

    @Override
    public DateDiffMillisNanosEvaluator get(DriverContext context) {
      return new DateDiffMillisNanosEvaluator(source, unit.get(context), startTimestampMillis.get(context), endTimestampNanos.get(context), context);
    }

    @Override
    public String toString() {
      return "DateDiffMillisNanosEvaluator[" + "unit=" + unit + ", startTimestampMillis=" + startTimestampMillis + ", endTimestampNanos=" + endTimestampNanos + "]";
    }
  }
}
