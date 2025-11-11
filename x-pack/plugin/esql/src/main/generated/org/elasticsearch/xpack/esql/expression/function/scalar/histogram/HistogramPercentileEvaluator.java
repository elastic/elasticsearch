// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link HistogramPercentile}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class HistogramPercentileEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HistogramPercentileEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator value;

  private final EvalOperator.ExpressionEvaluator percentile;

  private final DriverContext driverContext;

  private Warnings warnings;

  public HistogramPercentileEvaluator(Source source, EvalOperator.ExpressionEvaluator value,
      EvalOperator.ExpressionEvaluator percentile, DriverContext driverContext) {
    this.source = source;
    this.value = value;
    this.percentile = percentile;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (ExponentialHistogramBlock valueBlock = (ExponentialHistogramBlock) value.eval(page)) {
      try (DoubleBlock percentileBlock = (DoubleBlock) percentile.eval(page)) {
        return eval(page.getPositionCount(), valueBlock, percentileBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += value.baseRamBytesUsed();
    baseRamBytesUsed += percentile.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, ExponentialHistogramBlock valueBlock,
      DoubleBlock percentileBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      ExponentialHistogramScratch valueScratch = new ExponentialHistogramScratch();
      position: for (int p = 0; p < positionCount; p++) {
        switch (valueBlock.getValueCount(p)) {
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
        switch (percentileBlock.getValueCount(p)) {
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
        ExponentialHistogram value = valueBlock.getExponentialHistogram(valueBlock.getFirstValueIndex(p), valueScratch);
        double percentile = percentileBlock.getDouble(percentileBlock.getFirstValueIndex(p));
        try {
          HistogramPercentile.process(result, value, percentile);
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
    return "HistogramPercentileEvaluator[" + "value=" + value + ", percentile=" + percentile + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(value, percentile);
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

    private final EvalOperator.ExpressionEvaluator.Factory value;

    private final EvalOperator.ExpressionEvaluator.Factory percentile;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory value,
        EvalOperator.ExpressionEvaluator.Factory percentile) {
      this.source = source;
      this.value = value;
      this.percentile = percentile;
    }

    @Override
    public HistogramPercentileEvaluator get(DriverContext context) {
      return new HistogramPercentileEvaluator(source, value.get(context), percentile.get(context), context);
    }

    @Override
    public String toString() {
      return "HistogramPercentileEvaluator[" + "value=" + value + ", percentile=" + percentile + "]";
    }
  }
}
