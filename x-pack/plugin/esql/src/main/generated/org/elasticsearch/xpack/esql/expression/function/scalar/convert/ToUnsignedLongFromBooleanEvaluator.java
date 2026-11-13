// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongSingleArrayPool;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ToUnsignedLong}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToUnsignedLongFromBooleanEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToUnsignedLongFromBooleanEvaluator.class);

  private final ExpressionEvaluator bool;

  private final LongSingleArrayPool pool;

  public ToUnsignedLongFromBooleanEvaluator(Source source, ExpressionEvaluator bool,
      DriverContext driverContext) {
    super(driverContext, source);
    this.bool = bool;
    this.pool = new LongSingleArrayPool(driverContext.breaker());
  }

  @Override
  public ExpressionEvaluator next() {
    return bool;
  }

  @Override
  public Block evalVector(Vector v) {
    BooleanVector vector = (BooleanVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      return driverContext.blockFactory().newConstantLongBlockWith(evalValue(vector, 0), positionCount);
    }
    final LongSingleArrayPool pool = this.pool;
    final long[] values = pool.getOrAllocate(positionCount);
    for (int p = 0; p < positionCount; p++) {
      values[p] = evalValue(vector, p);
    }
    LongVector result = driverContext.blockFactory().newLongArrayVector(values, positionCount);
    result.attachReleasable(() -> pool.put(values));
    return result.asBlock();
  }

  private long evalValue(BooleanVector container, int index) {
    boolean value = container.getBoolean(index);
    return ToUnsignedLong.fromBoolean(value);
  }

  @Override
  public Block evalBlock(Block b) {
    BooleanBlock block = (BooleanBlock) b;
    int positionCount = block.getPositionCount();
    try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          long value = evalValue(block, i);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendLong(value);
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

  private long evalValue(BooleanBlock container, int index) {
    boolean value = container.getBoolean(index);
    return ToUnsignedLong.fromBoolean(value);
  }

  @Override
  public String toString() {
    return "ToUnsignedLongFromBooleanEvaluator[" + "bool=" + bool + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(bool);
    Releasables.closeExpectNoException(pool);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += bool.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory bool;

    public Factory(Source source, ExpressionEvaluator.Factory bool) {
      this.source = source;
      this.bool = bool;
    }

    @Override
    public ToUnsignedLongFromBooleanEvaluator get(DriverContext context) {
      return new ToUnsignedLongFromBooleanEvaluator(source, bool.get(context), context);
    }

    @Override
    public String toString() {
      return "ToUnsignedLongFromBooleanEvaluator[" + "bool=" + bool + "]";
    }
  }
}
