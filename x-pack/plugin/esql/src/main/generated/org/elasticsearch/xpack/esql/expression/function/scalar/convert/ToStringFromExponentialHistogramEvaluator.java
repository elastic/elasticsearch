// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToString}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToStringFromExponentialHistogramEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToStringFromExponentialHistogramEvaluator.class);

  private final EvalOperator.ExpressionEvaluator histogram;

  public ToStringFromExponentialHistogramEvaluator(Source source,
      EvalOperator.ExpressionEvaluator histogram, DriverContext driverContext) {
    super(driverContext, source);
    this.histogram = histogram;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return histogram;
  }

  @Override
  public Block evalVector(Vector v) {
    throw new UnsupportedOperationException("vectors are unsupported for this evaluator");
  }

  @Override
  public Block evalBlock(Block b) {
    ExponentialHistogramBlock block = (ExponentialHistogramBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      ExponentialHistogramScratch scratchPad = new ExponentialHistogramScratch();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          BytesRef value = evalValue(block, i, scratchPad);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendBytesRef(value);
          valuesAppended = true;
        }
        if (valuesAppended == false) {
          builder.appendNull();
        } else if (positionOpened) {
          builder.endPositionEntry();
        }
      }
      return builder.build();
    }
  }

  private BytesRef evalValue(ExponentialHistogramBlock container, int index,
      ExponentialHistogramScratch scratchPad) {
    ExponentialHistogram value = container.getExponentialHistogram(index, scratchPad);
    return ToString.fromExponentialHistogram(value);
  }

  @Override
  public String toString() {
    return "ToStringFromExponentialHistogramEvaluator[" + "histogram=" + histogram + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(histogram);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += histogram.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory histogram;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory histogram) {
      this.source = source;
      this.histogram = histogram;
    }

    @Override
    public ToStringFromExponentialHistogramEvaluator get(DriverContext context) {
      return new ToStringFromExponentialHistogramEvaluator(source, histogram.get(context), context);
    }

    @Override
    public String toString() {
      return "ToStringFromExponentialHistogramEvaluator[" + "histogram=" + histogram + "]";
    }
  }
}
