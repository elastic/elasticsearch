// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToString}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToStringFromDateRangeEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToStringFromDateRangeEvaluator.class);

  private final ExpressionEvaluator range;

  private final DateFormatter formatter;

  public ToStringFromDateRangeEvaluator(Source source, ExpressionEvaluator range,
      DateFormatter formatter, DriverContext driverContext) {
    super(driverContext, source);
    this.range = range;
    this.formatter = formatter;
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
    LongRangeBlock block = (LongRangeBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      LongRangeBlockBuilder.LongRange scratchPad = new LongRangeBlockBuilder.LongRange();
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

  private BytesRef evalValue(LongRangeBlock container, int index,
      LongRangeBlockBuilder.LongRange scratchPad) {
    LongRangeBlockBuilder.LongRange value = container.getLongRange(index, scratchPad);
    return ToString.fromDateRange(value, this.formatter);
  }

  @Override
  public String toString() {
    return "ToStringFromDateRangeEvaluator[" + "range=" + range + ", formatter=" + formatter + "]";
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

    private final DateFormatter formatter;

    public Factory(Source source, ExpressionEvaluator.Factory range, DateFormatter formatter) {
      this.source = source;
      this.range = range;
      this.formatter = formatter;
    }

    @Override
    public ToStringFromDateRangeEvaluator get(DriverContext context) {
      return new ToStringFromDateRangeEvaluator(source, range.get(context), formatter, context);
    }

    @Override
    public String toString() {
      return "ToStringFromDateRangeEvaluator[" + "range=" + range + ", formatter=" + formatter + "]";
    }
  }
}
