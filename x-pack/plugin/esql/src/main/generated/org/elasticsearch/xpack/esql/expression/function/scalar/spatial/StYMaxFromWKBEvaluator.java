// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StYMax}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class StYMaxFromWKBEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator wkb;

  public StYMaxFromWKBEvaluator(Source source, EvalOperator.ExpressionEvaluator wkb,
      DriverContext driverContext) {
    super(driverContext, source);
    this.wkb = wkb;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return wkb;
  }

  @Override
  public Block evalVector(Vector v) {
    BytesRefVector vector = (BytesRefVector) v;
    int positionCount = v.getPositionCount();
    BytesRef scratchPad = new BytesRef();
    if (vector.isConstant()) {
      try {
        return driverContext.blockFactory().newConstantDoubleBlockWith(evalValue(vector, 0, scratchPad), positionCount);
      } catch (IllegalArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendDouble(evalValue(vector, p, scratchPad));
        } catch (IllegalArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private double evalValue(BytesRefVector container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return StYMax.fromWellKnownBinary(value);
  }

  @Override
  public Block evalBlock(Block b) {
    BytesRefBlock block = (BytesRefBlock) b;
    int positionCount = block.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      BytesRef scratchPad = new BytesRef();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            double value = evalValue(block, i, scratchPad);
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

  private double evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return StYMax.fromWellKnownBinary(value);
  }

  @Override
  public String toString() {
    return "StYMaxFromWKBEvaluator[" + "wkb=" + wkb + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(wkb);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory wkb;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory wkb) {
      this.source = source;
      this.wkb = wkb;
    }

    @Override
    public StYMaxFromWKBEvaluator get(DriverContext context) {
      return new StYMaxFromWKBEvaluator(source, wkb.get(context), context);
    }

    @Override
    public String toString() {
      return "StYMaxFromWKBEvaluator[" + "wkb=" + wkb + "]";
    }
  }
}
