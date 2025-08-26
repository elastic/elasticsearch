// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToRadians}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToRadiansEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator deg;

  public ToRadiansEvaluator(Source source, EvalOperator.ExpressionEvaluator deg,
      DriverContext driverContext) {
    super(driverContext, source);
    this.deg = deg;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return deg;
  }

  @Override
  public Block evalVector(Vector v) {
    DoubleVector vector = (DoubleVector) v;
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

  private double evalValue(DoubleVector container, int index) {
    double value = container.getDouble(index);
    return ToRadians.process(value);
  }

  @Override
  public Block evalBlock(Block b) {
    DoubleBlock block = (DoubleBlock) b;
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

  private double evalValue(DoubleBlock container, int index) {
    double value = container.getDouble(index);
    return ToRadians.process(value);
  }

  @Override
  public String toString() {
    return "ToRadiansEvaluator[" + "deg=" + deg + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(deg);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory deg;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory deg) {
      this.source = source;
      this.deg = deg;
    }

    @Override
    public ToRadiansEvaluator get(DriverContext context) {
      return new ToRadiansEvaluator(source, deg.get(context), context);
    }

    @Override
    public String toString() {
      return "ToRadiansEvaluator[" + "deg=" + deg + "]";
    }
  }
}
