// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToDateRange}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToDateRangeFromStringEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToDateRangeFromStringEvaluator.class);

  private final ExpressionEvaluator field;

  public ToDateRangeFromStringEvaluator(Source source, ExpressionEvaluator field,
      DriverContext driverContext) {
    super(driverContext, source);
    this.field = field;
  }

  @Override
  public ExpressionEvaluator next() {
    return field;
  }

  @Override
  public Block evalVector(Vector v) {
    BytesRefVector vector = (BytesRefVector) v;
    int positionCount = v.getPositionCount();
    BytesRef scratchPad = new BytesRef();
    if (vector.isConstant()) {
      try {
        try (LongRangeBlockBuilder constBuilder = driverContext.blockFactory().newLongRangeBlockBuilder(1)) {
          constBuilder.appendLongRange(evalValue(vector, 0, scratchPad));
          return constBuilder.build();
        }
      } catch (IllegalArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (LongRangeBlockBuilder builder = driverContext.blockFactory().newLongRangeBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendLongRange(evalValue(vector, p, scratchPad));
        } catch (IllegalArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private LongRangeBlockBuilder.LongRange evalValue(BytesRefVector container, int index,
      BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ToDateRange.processFromString(value);
  }

  @Override
  public Block evalBlock(Block b) {
    BytesRefBlock block = (BytesRefBlock) b;
    int positionCount = block.getPositionCount();
    try (LongRangeBlockBuilder builder = driverContext.blockFactory().newLongRangeBlockBuilder(positionCount)) {
      BytesRef scratchPad = new BytesRef();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            LongRangeBlockBuilder.LongRange value = evalValue(block, i, scratchPad);
            if (positionOpened == false && valueCount > 1) {
              builder.beginPositionEntry();
              positionOpened = true;
            }
            builder.appendLongRange(value);
            valuesAppended = true;
          } catch (IllegalArgumentException  e) {
            registerException(e);
          }
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

  private LongRangeBlockBuilder.LongRange evalValue(BytesRefBlock container, int index,
      BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ToDateRange.processFromString(value);
  }

  @Override
  public String toString() {
    return "ToDateRangeFromStringEvaluator[" + "field=" + field + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory field;

    public Factory(Source source, ExpressionEvaluator.Factory field) {
      this.source = source;
      this.field = field;
    }

    @Override
    public ToDateRangeFromStringEvaluator get(DriverContext context) {
      return new ToDateRangeFromStringEvaluator(source, field.get(context), context);
    }

    @Override
    public String toString() {
      return "ToDateRangeFromStringEvaluator[" + "field=" + field + "]";
    }
  }
}
