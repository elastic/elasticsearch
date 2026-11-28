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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ParseIp}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ParseIpLongEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ParseIpLongEvaluator.class);

  private final ExpressionEvaluator l;

  private final BreakingBytesRefBuilder scratch;

  public ParseIpLongEvaluator(Source source, ExpressionEvaluator l, BreakingBytesRefBuilder scratch,
      DriverContext driverContext) {
    super(driverContext, source);
    this.l = l;
    this.scratch = scratch;
  }

  @Override
  public ExpressionEvaluator next() {
    return l;
  }

  @Override
  public Block evalVector(Vector v) {
    LongVector vector = (LongVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      try {
        return driverContext.blockFactory().newConstantBytesRefBlockWith(evalValue(vector, 0), positionCount);
      } catch (InvalidArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendBytesRef(evalValue(vector, p));
        } catch (InvalidArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private BytesRef evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return ParseIp.fromLong(value, this.scratch);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            BytesRef value = evalValue(block, i);
            if (positionOpened == false && valueCount > 1) {
              builder.beginPositionEntry();
              positionOpened = true;
            }
            builder.appendBytesRef(value);
            valuesAppended = true;
          } catch (InvalidArgumentException  e) {
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

  private BytesRef evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return ParseIp.fromLong(value, this.scratch);
  }

  @Override
  public String toString() {
    return "ParseIpLongEvaluator[" + "l=" + l + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(l, scratch);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += l.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory l;

    private final Function<DriverContext, BreakingBytesRefBuilder> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory l,
        Function<DriverContext, BreakingBytesRefBuilder> scratch) {
      this.source = source;
      this.l = l;
      this.scratch = scratch;
    }

    @Override
    public ParseIpLongEvaluator get(DriverContext context) {
      return new ParseIpLongEvaluator(source, l.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "ParseIpLongEvaluator[" + "l=" + l + "]";
    }
  }
}
