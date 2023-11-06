// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToDouble}.
 * This class is generated. Do not edit it.
 */
public final class ToDoubleFromLongEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  public ToDoubleFromLongEvaluator(EvalOperator.ExpressionEvaluator field, Source source,
      DriverContext driverContext) {
    super(driverContext, field, source);
  }

  @Override
  public String name() {
    return "ToDoubleFromLong";
  }

  @Override
  public Block evalVector(Vector v) {
    LongVector vector = (LongVector) v;
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

  private static double evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return ToDouble.fromLong(value);
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

  private static double evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return ToDouble.fromLong(value);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory field;

    public Factory(EvalOperator.ExpressionEvaluator.Factory field, Source source) {
      this.field = field;
      this.source = source;
    }

    @Override
    public ToDoubleFromLongEvaluator get(DriverContext context) {
      return new ToDoubleFromLongEvaluator(field.get(context), source, context);
    }

    @Override
    public String toString() {
      return "ToDoubleFromLongEvaluator[field=" + field + "]";
    }
  }
}
