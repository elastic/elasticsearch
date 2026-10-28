// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import java.lang.IllegalArgumentException;
import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link HistogramFraction}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class HistogramFractionTDigestEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HistogramFractionTDigestEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator histogram;

  private final ExpressionEvaluator bucket;

  private final Integer decimals;

  private final MemoryTrackingTDigestArrays tdigestArrays;

  private final DriverContext driverContext;

  private Warnings warnings;

  public HistogramFractionTDigestEvaluator(Source source, ExpressionEvaluator histogram,
      ExpressionEvaluator bucket, Integer decimals, MemoryTrackingTDigestArrays tdigestArrays,
      DriverContext driverContext) {
    this.source = source;
    this.histogram = histogram;
    this.bucket = bucket;
    this.decimals = decimals;
    this.tdigestArrays = tdigestArrays;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (TDigestBlock histogramBlock = (TDigestBlock) histogram.eval(page)) {
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

  public DoubleBlock eval(int positionCount, TDigestBlock histogramBlock,
      DoubleRangeBlock bucketBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      TDigestHolder histogramScratch = new TDigestHolder();
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
        TDigestHolder histogram = histogramBlock.getTDigestHolder(histogramBlock.getFirstValueIndex(p), histogramScratch);
        DoubleRangeBlockBuilder.DoubleRange bucket = bucketBlock.getDoubleRange(bucketBlock.getFirstValueIndex(p), bucketScratch);
        result.appendDouble(HistogramFraction.process(histogram, bucket, this.decimals, this.tdigestArrays));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "HistogramFractionTDigestEvaluator[" + "histogram=" + histogram + ", bucket=" + bucket + ", decimals=" + decimals + ", tdigestArrays=" + tdigestArrays + "]";
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

    private final Function<DriverContext, MemoryTrackingTDigestArrays> tdigestArrays;

    public Factory(Source source, ExpressionEvaluator.Factory histogram,
        ExpressionEvaluator.Factory bucket, Integer decimals,
        Function<DriverContext, MemoryTrackingTDigestArrays> tdigestArrays) {
      this.source = source;
      this.histogram = histogram;
      this.bucket = bucket;
      this.decimals = decimals;
      this.tdigestArrays = tdigestArrays;
    }

    @Override
    public HistogramFractionTDigestEvaluator get(DriverContext context) {
      return new HistogramFractionTDigestEvaluator(source, histogram.get(context), bucket.get(context), decimals, tdigestArrays.apply(context), context);
    }

    @Override
    public String toString() {
      return "HistogramFractionTDigestEvaluator[" + "histogram=" + histogram + ", bucket=" + bucket + ", decimals=" + decimals + ", tdigestArrays=" + tdigestArrays + "]";
    }
  }
}
