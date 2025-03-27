// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToBoolean}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToBooleanFromIntEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator i;

  public ToBooleanFromIntEvaluator(Source source, EvalOperator.ExpressionEvaluator i,
      DriverContext driverContext) {
    super(driverContext, source);
    this.i = i;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return i;
  }

  @Override
  public Block evalVector(Vector v) {
    IntVector vector = (IntVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      return driverContext.blockFactory().newConstantBooleanBlockWith(evalValue(vector, 0), positionCount);
    }
    try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendBoolean(evalValue(vector, p));
      }
      return builder.build();
    }
  }

  private boolean evalValue(IntVector container, int index) {
    int value = container.getInt(index);
    return ToBoolean.fromInt(value);
  }

  @Override
  public Block evalBlock(Block b) {
    IntBlock block = (IntBlock) b;
    int positionCount = block.getPositionCount();
    try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          boolean value = evalValue(block, i);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendBoolean(value);
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

  private boolean evalValue(IntBlock container, int index) {
    int value = container.getInt(index);
    return ToBoolean.fromInt(value);
  }

  @Override
  public String toString() {
    return "ToBooleanFromIntEvaluator[" + "i=" + i + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(i);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory i;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory i) {
      this.source = source;
      this.i = i;
    }

    @Override
    public ToBooleanFromIntEvaluator get(DriverContext context) {
      return new ToBooleanFromIntEvaluator(source, i.get(context), context);
    }

    @Override
    public String toString() {
      return "ToBooleanFromIntEvaluator[" + "i=" + i + "]";
    }
  }
}
