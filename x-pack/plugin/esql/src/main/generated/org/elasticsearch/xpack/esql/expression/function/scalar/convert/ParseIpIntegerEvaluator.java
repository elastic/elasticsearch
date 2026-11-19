// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ParseIp}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ParseIpIntegerEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ParseIpIntegerEvaluator.class);

  private final ExpressionEvaluator i;

  private final BreakingBytesRefBuilder scratch;

  public ParseIpIntegerEvaluator(Source source, ExpressionEvaluator i,
      BreakingBytesRefBuilder scratch, DriverContext driverContext) {
    super(driverContext, source);
    this.i = i;
    this.scratch = scratch;
  }

  @Override
  public ExpressionEvaluator next() {
    return i;
  }

  @Override
  public Block evalVector(Vector v) {
    IntVector vector = (IntVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      return driverContext.blockFactory().newConstantBytesRefBlockWith(evalValue(vector, 0), positionCount);
    }
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendBytesRef(evalValue(vector, p));
      }
      return builder.build();
    }
  }

  private BytesRef evalValue(IntVector container, int index) {
    int value = container.getInt(index);
    return ParseIp.fromInteger(value, this.scratch);
  }

  @Override
  public Block evalBlock(Block b) {
    IntBlock block = (IntBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          BytesRef value = evalValue(block, i);
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

  private BytesRef evalValue(IntBlock container, int index) {
    int value = container.getInt(index);
    return ParseIp.fromInteger(value, this.scratch);
  }

  @Override
  public String toString() {
    return "ParseIpIntegerEvaluator[" + "i=" + i + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(i, scratch);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += i.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory i;

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory i,
        Function<DriverContext, BreakingBytesRefBuilder> scratch) {
      this.source = source;
      this.i = i;
      this.scratch = scratch;
    }

    @Override
    public ParseIpIntegerEvaluator get(DriverContext context) {
      return new ParseIpIntegerEvaluator(source, i.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "ParseIpIntegerEvaluator[" + "i=" + i + "]";
    }
  }
}
