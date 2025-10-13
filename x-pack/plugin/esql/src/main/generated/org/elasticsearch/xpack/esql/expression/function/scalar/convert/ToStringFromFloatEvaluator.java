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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToString}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToStringFromFloatEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToStringFromFloatEvaluator.class);

  private final EvalOperator.ExpressionEvaluator flt;

  public ToStringFromFloatEvaluator(Source source, EvalOperator.ExpressionEvaluator flt,
      DriverContext driverContext) {
    super(driverContext, source);
    this.flt = flt;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return flt;
  }

  @Override
  public Block evalVector(Vector v) {
    FloatVector vector = (FloatVector) v;
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

  private BytesRef evalValue(FloatVector container, int index) {
    float value = container.getFloat(index);
    return ToString.fromFloat(value);
  }

  @Override
  public Block evalBlock(Block b) {
    FloatBlock block = (FloatBlock) b;
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

  private BytesRef evalValue(FloatBlock container, int index) {
    float value = container.getFloat(index);
    return ToString.fromFloat(value);
  }

  @Override
  public String toString() {
    return "ToStringFromFloatEvaluator[" + "flt=" + flt + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(flt);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += flt.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory flt;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory flt) {
      this.source = source;
      this.flt = flt;
    }

    @Override
    public ToStringFromFloatEvaluator get(DriverContext context) {
      return new ToStringFromFloatEvaluator(source, flt.get(context), context);
    }

    @Override
    public String toString() {
      return "ToStringFromFloatEvaluator[" + "flt=" + flt + "]";
    }
  }
}
