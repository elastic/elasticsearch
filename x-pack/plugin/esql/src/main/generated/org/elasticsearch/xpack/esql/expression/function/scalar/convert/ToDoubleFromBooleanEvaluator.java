// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToDouble}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToDoubleFromBooleanEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator bool;

  public ToDoubleFromBooleanEvaluator(Source source, EvalOperator.ExpressionEvaluator bool,
      DriverContext driverContext) {
    super(driverContext, source);
    this.bool = bool;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return bool;
  }

  @Override
  public Block evalVector(Vector v) {
    BooleanVector vector = (BooleanVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      return driverContext.blockFactory().newConstantDoubleBlockWith(evalValue(vector, 0), positionCount);
    }
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendDouble(evalValue(vector, p));
      }
      return builder.build();
    }
  }

  private double evalValue(BooleanVector container, int index) {
    boolean value = container.getBoolean(index);
    return ToDouble.fromBoolean(value);
  }

  @Override
  public Block evalBlock(Block b) {
    BooleanBlock block = (BooleanBlock) b;
    int positionCount = block.getPositionCount();
    try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          double value = evalValue(block, i);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendDouble(value);
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

  private double evalValue(BooleanBlock container, int index) {
    boolean value = container.getBoolean(index);
    return ToDouble.fromBoolean(value);
  }

  @Override
  public String toString() {
    return "ToDoubleFromBooleanEvaluator[" + "bool=" + bool + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(bool);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory bool;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory bool) {
      this.source = source;
      this.bool = bool;
    }

    @Override
    public ToDoubleFromBooleanEvaluator get(DriverContext context) {
      return new ToDoubleFromBooleanEvaluator(source, bool.get(context), context);
    }

    @Override
    public String toString() {
      return "ToDoubleFromBooleanEvaluator[" + "bool=" + bool + "]";
    }
  }
}
