// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import java.lang.IllegalArgumentException;
import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link HistogramFraction}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class HistogramFractionExponentialHistogramEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HistogramFractionExponentialHistogramEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator histogram;

  private final ExpressionEvaluator bucket;

  private final Integer decimals;

  private final DriverContext driverContext;

  private Warnings warnings;

  public HistogramFractionExponentialHistogramEvaluator(Source source,
      ExpressionEvaluator histogram, ExpressionEvaluator bucket, Integer decimals,
      DriverContext driverContext) {
    this.source = source;
    this.histogram = histogram;
    this.bucket = bucket;
    this.decimals = decimals;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (ExponentialHistogramBlock histogramBlock = (ExponentialHistogramBlock) histogram.eval(page)) {
      try (DoubleRangeBlock bucketBlock = (DoubleRangeBlock) bucket.eval(page)) {
        return eval(page.getPositionCount(), histogramBlock, bucketBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += histogram.baseRamBytesUsed();
    baseRamBytesUsed += bucket.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, ExponentialHistogramBlock histogramBlock,
      DoubleRangeBlock bucketBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      ExponentialHistogramScratch histogramScratch = new ExponentialHistogramScratch();
      DoubleRangeBlockBuilder.DoubleRange bucketScratch = new DoubleRangeBlockBuilder.DoubleRange();
      position: for (int p = 0; p < positionCount; p++) {
        if (histogramBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (histogramBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        if (bucketBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        switch (bucketBlock.getValueCount(p)) {
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        ExponentialHistogram histogram = histogramBlock.getExponentialHistogram(histogramBlock.getFirstValueIndex(p), histogramScratch);
        DoubleRangeBlockBuilder.DoubleRange bucket = bucketBlock.getDoubleRange(bucketBlock.getFirstValueIndex(p), bucketScratch);
        result.appendDouble(HistogramFraction.process(histogram, bucket, this.decimals));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "HistogramFractionExponentialHistogramEvaluator[" + "histogram=" + histogram + ", bucket=" + bucket + ", decimals=" + decimals + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(histogram, bucket);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = driverContext.createWarnings(source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory histogram;

    private final ExpressionEvaluator.Factory bucket;

    private final Integer decimals;

    public Factory(Source source, ExpressionEvaluator.Factory histogram,
        ExpressionEvaluator.Factory bucket, Integer decimals) {
      this.source = source;
      this.histogram = histogram;
      this.bucket = bucket;
      this.decimals = decimals;
    }

    @Override
    public HistogramFractionExponentialHistogramEvaluator get(DriverContext context) {
      return new HistogramFractionExponentialHistogramEvaluator(source, histogram.get(context), bucket.get(context), decimals, context);
    }

    @Override
    public String toString() {
      return "HistogramFractionExponentialHistogramEvaluator[" + "histogram=" + histogram + ", bucket=" + bucket + ", decimals=" + decimals + "]";
    }
  }
}
