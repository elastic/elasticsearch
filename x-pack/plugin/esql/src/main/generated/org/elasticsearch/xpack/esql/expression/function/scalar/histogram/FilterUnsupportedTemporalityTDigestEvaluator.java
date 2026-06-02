// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.aggregation.InvalidTemporalityException;
import org.elasticsearch.compute.aggregation.TemporalityAccessor;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link FilterUnsupportedTemporality}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class FilterUnsupportedTemporalityTDigestEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FilterUnsupportedTemporalityTDigestEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator histogram;

  private final ExpressionEvaluator temporality;

  private final TemporalityAccessor[] accessor;

  private final DriverContext driverContext;

  private Warnings warnings;

  public FilterUnsupportedTemporalityTDigestEvaluator(Source source, ExpressionEvaluator histogram,
      ExpressionEvaluator temporality, TemporalityAccessor[] accessor,
      DriverContext driverContext) {
    this.source = source;
    this.histogram = histogram;
    this.temporality = temporality;
    this.accessor = accessor;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (TDigestBlock histogramBlock = (TDigestBlock) histogram.eval(page)) {
      try (BytesRefBlock temporalityBlock = (BytesRefBlock) temporality.eval(page)) {
        return eval(page.getPositionCount(), histogramBlock, temporalityBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += histogram.baseRamBytesUsed();
    baseRamBytesUsed += temporality.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public TDigestBlock eval(int positionCount, TDigestBlock histogramBlock,
      BytesRefBlock temporalityBlock) {
    try(TDigestBlock.Builder result = driverContext.blockFactory().newTDigestBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          FilterUnsupportedTemporality.processTDigest(result, p, histogramBlock, temporalityBlock, this.accessor);
        } catch (InvalidTemporalityException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "FilterUnsupportedTemporalityTDigestEvaluator[" + "histogram=" + histogram + ", temporality=" + temporality + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(histogram, temporality);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory histogram;

    private final ExpressionEvaluator.Factory temporality;

    private final Function<DriverContext, TemporalityAccessor[]> accessor;

    public Factory(Source source, ExpressionEvaluator.Factory histogram,
        ExpressionEvaluator.Factory temporality,
        Function<DriverContext, TemporalityAccessor[]> accessor) {
      this.source = source;
      this.histogram = histogram;
      this.temporality = temporality;
      this.accessor = accessor;
    }

    @Override
    public FilterUnsupportedTemporalityTDigestEvaluator get(DriverContext context) {
      return new FilterUnsupportedTemporalityTDigestEvaluator(source, histogram.get(context), temporality.get(context), accessor.apply(context), context);
    }

    @Override
    public String toString() {
      return "FilterUnsupportedTemporalityTDigestEvaluator[" + "histogram=" + histogram + ", temporality=" + temporality + "]";
    }
  }
}
