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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToInteger}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToIntegerFromDoubleEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator dbl;

  public ToIntegerFromDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator dbl,
      DriverContext driverContext) {
    super(driverContext, source);
    this.dbl = dbl;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return dbl;
  }

  @Override
  public Block evalVector(Vector v) {
    DoubleVector vector = (DoubleVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      try {
        return driverContext.blockFactory().newConstantIntBlockWith(evalValue(vector, 0), positionCount);
      } catch (InvalidArgumentException  e) {
        registerException(e);
        return driverContext.blockFactory().newConstantNullBlock(positionCount);
      }
    }
    try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        try {
          builder.appendInt(evalValue(vector, p));
        } catch (InvalidArgumentException  e) {
          registerException(e);
          builder.appendNull();
        }
      }
      return builder.build();
    }
  }

  private int evalValue(DoubleVector container, int index) {
    double value = container.getDouble(index);
    return ToInteger.fromDouble(value);
  }

  @Override
  public Block evalBlock(Block b) {
    DoubleBlock block = (DoubleBlock) b;
    int positionCount = block.getPositionCount();
    try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          try {
            int value = evalValue(block, i);
            if (positionOpened == false && valueCount > 1) {
              builder.beginPositionEntry();
              positionOpened = true;
            }
            builder.appendInt(value);
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

  private int evalValue(DoubleBlock container, int index) {
    double value = container.getDouble(index);
    return ToInteger.fromDouble(value);
  }

  @Override
  public String toString() {
    return "ToIntegerFromDoubleEvaluator[" + "dbl=" + dbl + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(dbl);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory dbl;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory dbl) {
      this.source = source;
      this.dbl = dbl;
    }

    @Override
    public ToIntegerFromDoubleEvaluator get(DriverContext context) {
      return new ToIntegerFromDoubleEvaluator(source, dbl.get(context), context);
    }

    @Override
    public String toString() {
      return "ToIntegerFromDoubleEvaluator[" + "dbl=" + dbl + "]";
    }
  }
}
