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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link TRange}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class TRangeEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TRangeEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator timestamp;

  private final long startTimestamp;

  private final long endTimestamp;

  private final DriverContext driverContext;

  private Warnings warnings;

  public TRangeEvaluator(Source source, EvalOperator.ExpressionEvaluator timestamp,
      long startTimestamp, long endTimestamp, DriverContext driverContext) {
    this.source = source;
    this.timestamp = timestamp;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock timestampBlock = (LongBlock) timestamp.eval(page)) {
      LongVector timestampVector = timestampBlock.asVector();
      if (timestampVector == null) {
        return eval(page.getPositionCount(), timestampBlock);
      }
      return eval(page.getPositionCount(), timestampVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += timestamp.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock timestampBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (timestampBlock.getValueCount(p)) {
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
        long timestamp = timestampBlock.getLong(timestampBlock.getFirstValueIndex(p));
        result.appendBoolean(TRange.process(timestamp, this.startTimestamp, this.endTimestamp));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, LongVector timestampVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long timestamp = timestampVector.getLong(p);
        result.appendBoolean(p, TRange.process(timestamp, this.startTimestamp, this.endTimestamp));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "TRangeEvaluator[" + "timestamp=" + timestamp + ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(timestamp);
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

    private final EvalOperator.ExpressionEvaluator.Factory timestamp;

    private final long startTimestamp;

    private final long endTimestamp;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory timestamp,
        long startTimestamp, long endTimestamp) {
      this.source = source;
      this.timestamp = timestamp;
      this.startTimestamp = startTimestamp;
      this.endTimestamp = endTimestamp;
    }

    @Override
    public TRangeEvaluator get(DriverContext context) {
      return new TRangeEvaluator(source, timestamp.get(context), startTimestamp, endTimestamp, context);
    }

    @Override
    public String toString() {
      return "TRangeEvaluator[" + "timestamp=" + timestamp + ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + "]";
    }
  }
}
