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
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToString}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToStringFromDoubleRangeEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToStringFromDoubleRangeEvaluator.class);

  private final ExpressionEvaluator range;

  public ToStringFromDoubleRangeEvaluator(Source source, ExpressionEvaluator range,
      DriverContext driverContext) {
    super(driverContext, source);
    this.range = range;
  }

  @Override
  public ExpressionEvaluator next() {
    return range;
  }

  @Override
  public Block evalVector(Vector v) {
    throw new UnsupportedOperationException("vectors are unsupported for this evaluator");
  }

  @Override
  public Block evalBlock(Block b) {
    DoubleRangeBlock block = (DoubleRangeBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      DoubleRangeBlockBuilder.DoubleRange scratchPad = new DoubleRangeBlockBuilder.DoubleRange();
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

  private BytesRef evalValue(DoubleRangeBlock container, int index,
      DoubleRangeBlockBuilder.DoubleRange scratchPad) {
    DoubleRangeBlockBuilder.DoubleRange value = container.getDoubleRange(index, scratchPad);
    return ToString.fromDoubleRange(value);
  }

  @Override
  public String toString() {
    return "ToStringFromDoubleRangeEvaluator[" + "range=" + range + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(range);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += range.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory range;

    public Factory(Source source, ExpressionEvaluator.Factory range) {
      this.source = source;
      this.range = range;
    }

    @Override
    public ToStringFromDoubleRangeEvaluator get(DriverContext context) {
      return new ToStringFromDoubleRangeEvaluator(source, range.get(context), context);
    }

    @Override
    public String toString() {
      return "ToStringFromDoubleRangeEvaluator[" + "range=" + range + "]";
    }
  }
}
