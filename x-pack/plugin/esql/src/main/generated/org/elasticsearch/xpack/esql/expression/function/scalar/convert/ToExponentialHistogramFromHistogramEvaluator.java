// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToExponentialHistogram}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ToExponentialHistogramFromHistogramEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToExponentialHistogramFromHistogramEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator in;

  private final EncodedTDigest decoder;

  private final TDigestHolder scratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ToExponentialHistogramFromHistogramEvaluator(Source source, ExpressionEvaluator in,
      EncodedTDigest decoder, TDigestHolder scratch, DriverContext driverContext) {
    this.source = source;
    this.in = in;
    this.decoder = decoder;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock inBlock = (BytesRefBlock) in.eval(page)) {
      return eval(page.getPositionCount(), inBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += in.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public ExponentialHistogramBlock eval(int positionCount, BytesRefBlock inBlock) {
    try(ExponentialHistogramBlock.Builder result = driverContext.blockFactory().newExponentialHistogramBlockBuilder(positionCount)) {
      BytesRef inScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        switch (inBlock.getValueCount(p)) {
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
        BytesRef in = inBlock.getBytesRef(inBlock.getFirstValueIndex(p), inScratch);
        ToExponentialHistogram.fromHistogram(result, in, this.decoder, this.scratch);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ToExponentialHistogramFromHistogramEvaluator[" + "in=" + in + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(in);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory in;

    private final Function<DriverContext, EncodedTDigest> decoder;

    private final Function<DriverContext, TDigestHolder> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory in,
        Function<DriverContext, EncodedTDigest> decoder,
        Function<DriverContext, TDigestHolder> scratch) {
      this.source = source;
      this.in = in;
      this.decoder = decoder;
      this.scratch = scratch;
    }

    @Override
    public ToExponentialHistogramFromHistogramEvaluator get(DriverContext context) {
      return new ToExponentialHistogramFromHistogramEvaluator(source, in.get(context), decoder.apply(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "ToExponentialHistogramFromHistogramEvaluator[" + "in=" + in + "]";
    }
  }
}
