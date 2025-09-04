// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToDenseVector}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToDenseVectorFromLongEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToDenseVectorFromLongEvaluator.class);

  private final EvalOperator.ExpressionEvaluator l;

  public ToDenseVectorFromLongEvaluator(Source source, EvalOperator.ExpressionEvaluator l,
      DriverContext driverContext) {
    super(driverContext, source);
    this.l = l;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return l;
  }

  @Override
  public Block evalVector(Vector v) {
    LongVector vector = (LongVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      return driverContext.blockFactory().newConstantFloatBlockWith(evalValue(vector, 0), positionCount);
    }
    try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendFloat(evalValue(vector, p));
      }
      return builder.build();
    }
  }

  private float evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return ToDenseVector.fromLong(value);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
    int positionCount = block.getPositionCount();
    try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          float value = evalValue(block, i);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendFloat(value);
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

  private float evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return ToDenseVector.fromLong(value);
  }

  @Override
  public String toString() {
    return "ToDenseVectorFromLongEvaluator[" + "l=" + l + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(l);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += l.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory l;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory l) {
      this.source = source;
      this.l = l;
    }

    @Override
    public ToDenseVectorFromLongEvaluator get(DriverContext context) {
      return new ToDenseVectorFromLongEvaluator(source, l.get(context), context);
    }

    @Override
    public String toString() {
      return "ToDenseVectorFromLongEvaluator[" + "l=" + l + "]";
    }
  }
}
