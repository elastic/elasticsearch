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
public final class DateDiffNanosMillisEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DateDiffNanosMillisEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator unit;

  private final EvalOperator.ExpressionEvaluator startTimestampNanos;

  private final EvalOperator.ExpressionEvaluator endTimestampMillis;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DateDiffNanosMillisEvaluator(Source source, EvalOperator.ExpressionEvaluator unit,
      EvalOperator.ExpressionEvaluator startTimestampNanos,
      EvalOperator.ExpressionEvaluator endTimestampMillis, DriverContext driverContext) {
    this.source = source;
    this.unit = unit;
    this.startTimestampNanos = startTimestampNanos;
    this.endTimestampMillis = endTimestampMillis;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock unitBlock = (BytesRefBlock) unit.eval(page)) {
      try (LongBlock startTimestampNanosBlock = (LongBlock) startTimestampNanos.eval(page)) {
        try (LongBlock endTimestampMillisBlock = (LongBlock) endTimestampMillis.eval(page)) {
          BytesRefVector unitVector = unitBlock.asVector();
          if (unitVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampNanosBlock, endTimestampMillisBlock);
          }
          LongVector startTimestampNanosVector = startTimestampNanosBlock.asVector();
          if (startTimestampNanosVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampNanosBlock, endTimestampMillisBlock);
          }
          LongVector endTimestampMillisVector = endTimestampMillisBlock.asVector();
          if (endTimestampMillisVector == null) {
            return eval(page.getPositionCount(), unitBlock, startTimestampNanosBlock, endTimestampMillisBlock);
          }
          return eval(page.getPositionCount(), unitVector, startTimestampNanosVector, endTimestampMillisVector);
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += unit.baseRamBytesUsed();
    baseRamBytesUsed += startTimestampNanos.baseRamBytesUsed();
    baseRamBytesUsed += endTimestampMillis.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, BytesRefBlock unitBlock,
      LongBlock startTimestampNanosBlock, LongBlock endTimestampMillisBlock) {
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
        switch (startTimestampNanosBlock.getValueCount(p)) {
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
        switch (endTimestampMillisBlock.getValueCount(p)) {
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
        long startTimestampNanos = startTimestampNanosBlock.getLong(startTimestampNanosBlock.getFirstValueIndex(p));
        long endTimestampMillis = endTimestampMillisBlock.getLong(endTimestampMillisBlock.getFirstValueIndex(p));
        try {
          result.appendInt(DateDiff.processNanosMillis(unit, startTimestampNanos, endTimestampMillis));
        } catch (IllegalArgumentException | InvalidArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public IntBlock eval(int positionCount, BytesRefVector unitVector,
      LongVector startTimestampNanosVector, LongVector endTimestampMillisVector) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef unitScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef unit = unitVector.getBytesRef(p, unitScratch);
        long startTimestampNanos = startTimestampNanosVector.getLong(p);
        long endTimestampMillis = endTimestampMillisVector.getLong(p);
        try {
          result.appendInt(DateDiff.processNanosMillis(unit, startTimestampNanos, endTimestampMillis));
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
    return "DateDiffNanosMillisEvaluator[" + "unit=" + unit + ", startTimestampNanos=" + startTimestampNanos + ", endTimestampMillis=" + endTimestampMillis + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(unit, startTimestampNanos, endTimestampMillis);
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

    private final EvalOperator.ExpressionEvaluator.Factory startTimestampNanos;

    private final EvalOperator.ExpressionEvaluator.Factory endTimestampMillis;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory unit,
        EvalOperator.ExpressionEvaluator.Factory startTimestampNanos,
        EvalOperator.ExpressionEvaluator.Factory endTimestampMillis) {
      this.source = source;
      this.unit = unit;
      this.startTimestampNanos = startTimestampNanos;
      this.endTimestampMillis = endTimestampMillis;
    }

    @Override
    public DateDiffNanosMillisEvaluator get(DriverContext context) {
      return new DateDiffNanosMillisEvaluator(source, unit.get(context), startTimestampNanos.get(context), endTimestampMillis.get(context), context);
    }

    @Override
    public String toString() {
      return "DateDiffNanosMillisEvaluator[" + "unit=" + unit + ", startTimestampNanos=" + startTimestampNanos + ", endTimestampMillis=" + endTimestampMillis + "]";
    }
  }
}
