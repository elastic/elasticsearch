// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StX}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class StXFromGeoDocValuesEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StXFromGeoDocValuesEvaluator.class);

  private final EvalOperator.ExpressionEvaluator encoded;

  public StXFromGeoDocValuesEvaluator(Source source, EvalOperator.ExpressionEvaluator encoded,
      DriverContext driverContext) {
    super(driverContext, source);
    this.encoded = encoded;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return encoded;
  }

  @Override
  public Block evalVector(Vector v) {
    LongVector vector = (LongVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      try {
        return driverContext.blockFactory().newConstantDoubleBlockWith(evalValue(vector, 0), positionCount);
      } catch (IllegalArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendDouble(evalValue(vector, p));
        } catch (IllegalArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private double evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return StX.fromGeoDocValues(value);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
    int positionCount = block.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            double value = evalValue(block, i);
            if (positionOpened == false && valueCount > 1) {
              builder.beginPositionEntry();
              positionOpened = true;
            }
            builder.appendDouble(value);
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

  private double evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return StX.fromGeoDocValues(value);
  }

  @Override
  public String toString() {
    return "StXFromGeoDocValuesEvaluator[" + "encoded=" + encoded + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(encoded);
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += encoded.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory encoded;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory encoded) {
      this.source = source;
      this.encoded = encoded;
    }

    @Override
    public StXFromGeoDocValuesEvaluator get(DriverContext context) {
      return new StXFromGeoDocValuesEvaluator(source, encoded.get(context), context);
    }

    @Override
    public String toString() {
      return "StXFromGeoDocValuesEvaluator[" + "encoded=" + encoded + "]";
    }
  }
}
