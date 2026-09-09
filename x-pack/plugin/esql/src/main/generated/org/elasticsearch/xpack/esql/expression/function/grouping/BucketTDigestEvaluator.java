// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.grouping;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Bucket}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class BucketTDigestEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BucketTDigestEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator histogram;

  private final double roundTo;

  private final DriverContext driverContext;

  private Warnings warnings;

  public BucketTDigestEvaluator(Source source, ExpressionEvaluator histogram, double roundTo,
      DriverContext driverContext) {
    this.source = source;
    this.histogram = histogram;
    this.roundTo = roundTo;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (TDigestBlock histogramBlock = (TDigestBlock) histogram.eval(page)) {
      return eval(page.getPositionCount(), histogramBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += histogram.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleRangeBlock eval(int positionCount, TDigestBlock histogramBlock) {
    try(DoubleRangeBlock.Builder result = driverContext.blockFactory().newDoubleRangeBlockBuilder(positionCount)) {
      TDigestHolder histogramScratch = new TDigestHolder();
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
        TDigestHolder histogram = histogramBlock.getTDigestHolder(histogramBlock.getFirstValueIndex(p), histogramScratch);
        try {
          Bucket.process(result, histogram, this.roundTo);
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "BucketTDigestEvaluator[" + "histogram=" + histogram + ", roundTo=" + roundTo + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(histogram);
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

    private final double roundTo;

    public Factory(Source source, ExpressionEvaluator.Factory histogram, double roundTo) {
      this.source = source;
      this.histogram = histogram;
      this.roundTo = roundTo;
    }

    @Override
    public BucketTDigestEvaluator get(DriverContext context) {
      return new BucketTDigestEvaluator(source, histogram.get(context), roundTo, context);
    }

    @Override
    public String toString() {
      return "BucketTDigestEvaluator[" + "histogram=" + histogram + ", roundTo=" + roundTo + "]";
    }
  }
}
