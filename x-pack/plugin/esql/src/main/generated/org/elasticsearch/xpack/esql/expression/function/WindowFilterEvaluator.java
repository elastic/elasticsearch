// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function;

import java.lang.IllegalArgumentException;
import java.lang.Long;
import java.lang.Override;
import java.lang.String;
import java.util.Map;
import java.util.function.Function;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link WindowFilter}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class WindowFilterEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(WindowFilterEvaluator.class);

  private final Source source;

  private final long window;

  private final Rounding.Prepared bucket;

  private final Map<Long, Long> nextTimestamps;

  private final ExpressionEvaluator timestamp;

  private final DriverContext driverContext;

  private Warnings warnings;

  public WindowFilterEvaluator(Source source, long window, Rounding.Prepared bucket,
      Map<Long, Long> nextTimestamps, ExpressionEvaluator timestamp, DriverContext driverContext) {
    this.source = source;
    this.window = window;
    this.bucket = bucket;
    this.nextTimestamps = nextTimestamps;
    this.timestamp = timestamp;
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
        result.appendBoolean(WindowFilter.process(this.window, this.bucket, this.nextTimestamps, timestamp));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, LongVector timestampVector) {
    try(BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long timestamp = timestampVector.getLong(p);
        result.appendBoolean(p, WindowFilter.process(this.window, this.bucket, this.nextTimestamps, timestamp));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "WindowFilterEvaluator[" + "window=" + window + ", bucket=" + bucket + ", nextTimestamps=" + nextTimestamps + ", timestamp=" + timestamp + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(timestamp);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final long window;

    private final Rounding.Prepared bucket;

    private final Function<DriverContext, Map<Long, Long>> nextTimestamps;

    private final ExpressionEvaluator.Factory timestamp;

    public Factory(Source source, long window, Rounding.Prepared bucket,
        Function<DriverContext, Map<Long, Long>> nextTimestamps,
        ExpressionEvaluator.Factory timestamp) {
      this.source = source;
      this.window = window;
      this.bucket = bucket;
      this.nextTimestamps = nextTimestamps;
      this.timestamp = timestamp;
    }

    @Override
    public WindowFilterEvaluator get(DriverContext context) {
      return new WindowFilterEvaluator(source, window, bucket, nextTimestamps.apply(context), timestamp.get(context), context);
    }

    @Override
    public String toString() {
      return "WindowFilterEvaluator[" + "window=" + window + ", bucket=" + bucket + ", nextTimestamps=" + nextTimestamps + ", timestamp=" + timestamp + "]";
    }
  }
}
