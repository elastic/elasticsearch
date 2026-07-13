// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToExponentialHistogram}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ToExponentialHistogramFromTDigestEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToExponentialHistogramFromTDigestEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator in;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ToExponentialHistogramFromTDigestEvaluator(Source source, ExpressionEvaluator in,
      DriverContext driverContext) {
    this.source = source;
    this.in = in;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (TDigestBlock inBlock = (TDigestBlock) in.eval(page)) {
      return eval(page.getPositionCount(), inBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += in.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public ExponentialHistogramBlock eval(int positionCount, TDigestBlock inBlock) {
    try(ExponentialHistogramBlock.Builder result = driverContext.blockFactory().newExponentialHistogramBlockBuilder(positionCount)) {
      TDigestHolder inScratch = new TDigestHolder();
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
        TDigestHolder in = inBlock.getTDigestHolder(inBlock.getFirstValueIndex(p), inScratch);
        ToExponentialHistogram.fromTDigest(result, in);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ToExponentialHistogramFromTDigestEvaluator[" + "in=" + in + "]";
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

    public Factory(Source source, ExpressionEvaluator.Factory in) {
      this.source = source;
      this.in = in;
    }

    @Override
    public ToExponentialHistogramFromTDigestEvaluator get(DriverContext context) {
      return new ToExponentialHistogramFromTDigestEvaluator(source, in.get(context), context);
    }

    @Override
    public String toString() {
      return "ToExponentialHistogramFromTDigestEvaluator[" + "in=" + in + "]";
    }
  }
}
