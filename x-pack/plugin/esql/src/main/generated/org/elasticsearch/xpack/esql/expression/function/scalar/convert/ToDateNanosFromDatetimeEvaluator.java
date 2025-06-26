// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToDateNanos}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToDateNanosFromDatetimeEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator in;

  public ToDateNanosFromDatetimeEvaluator(Source source, EvalOperator.ExpressionEvaluator in,
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
    LongVector vector = (LongVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      try {
        return driverContext.blockFactory().newConstantLongBlockWith(evalValue(vector, 0), positionCount);
      } catch (IllegalArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendLong(evalValue(vector, p));
        } catch (IllegalArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private long evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return ToDateNanos.fromDatetime(value);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
    int positionCount = block.getPositionCount();
    try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            long value = evalValue(block, i);
            if (positionOpened == false && valueCount > 1) {
              builder.beginPositionEntry();
              positionOpened = true;
            }
            builder.appendLong(value);
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

  private long evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return ToDateNanos.fromDatetime(value);
  }

  @Override
  public String toString() {
    return "ToDateNanosFromDatetimeEvaluator[" + "in=" + in + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(in);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory in;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory in) {
      this.source = source;
      this.in = in;
    }

    @Override
    public ToDateNanosFromDatetimeEvaluator get(DriverContext context) {
      return new ToDateNanosFromDatetimeEvaluator(source, in.get(context), context);
    }

    @Override
    public String toString() {
      return "ToDateNanosFromDatetimeEvaluator[" + "in=" + in + "]";
    }
  }
}
