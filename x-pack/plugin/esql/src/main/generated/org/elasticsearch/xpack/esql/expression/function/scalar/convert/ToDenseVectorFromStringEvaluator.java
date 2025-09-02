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
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToDenseVector}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToDenseVectorFromStringEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToDenseVectorFromStringEvaluator.class);

  private final EvalOperator.ExpressionEvaluator in;

  public ToDenseVectorFromStringEvaluator(Source source, EvalOperator.ExpressionEvaluator in,
      DriverContext driverContext) {
    super(driverContext, source);
    this.in = in;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return in;
  }

  @Override
  public Block evalVector(Vector v) {
    BytesRefVector vector = (BytesRefVector) v;
    int positionCount = v.getPositionCount();
    BytesRef scratchPad = new BytesRef();
    if (vector.isConstant()) {
      try {
        return driverContext.blockFactory().newConstantFloatBlockWith(evalValue(vector, 0, scratchPad), positionCount);
      } catch (InvalidArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendFloat(evalValue(vector, p, scratchPad));
        } catch (InvalidArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private float evalValue(BytesRefVector container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ToDenseVector.fromString(value);
  }

  @Override
  public Block evalBlock(Block b) {
    BytesRefBlock block = (BytesRefBlock) b;
    int positionCount = block.getPositionCount();
    try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
      BytesRef scratchPad = new BytesRef();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            float value = evalValue(block, i, scratchPad);
            if (positionOpened == false && valueCount > 1) {
              builder.beginPositionEntry();
              positionOpened = true;
            }
            builder.appendFloat(value);
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

  private float evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return ToDenseVector.fromString(value);
  }

  @Override
  public String toString() {
    return "ToDenseVectorFromStringEvaluator[" + "in=" + in + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(in);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += in.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory in;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory in) {
      this.source = source;
      this.in = in;
    }

    @Override
    public ToDenseVectorFromStringEvaluator get(DriverContext context) {
      return new ToDenseVectorFromStringEvaluator(source, in.get(context), context);
    }

    @Override
    public String toString() {
      return "ToDenseVectorFromStringEvaluator[" + "in=" + in + "]";
    }
  }
}
