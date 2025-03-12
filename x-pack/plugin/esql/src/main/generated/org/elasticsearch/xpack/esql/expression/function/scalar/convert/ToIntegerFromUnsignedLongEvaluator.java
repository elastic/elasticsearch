// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
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
public final class ToIntegerFromUnsignedLongEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator ul;

  public ToIntegerFromUnsignedLongEvaluator(Source source, EvalOperator.ExpressionEvaluator ul,
      DriverContext driverContext) {
    super(driverContext, source);
    this.ul = ul;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return ul;
  }

  @Override
  public Block evalVector(Vector v) {
    LongVector vector = (LongVector) v;
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

  private int evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return ToInteger.fromUnsignedLong(value);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
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

  private int evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return ToInteger.fromUnsignedLong(value);
  }

  @Override
  public String toString() {
    return "ToIntegerFromUnsignedLongEvaluator[" + "ul=" + ul + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(ul);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory ul;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory ul) {
      this.source = source;
      this.ul = ul;
    }

    @Override
    public ToIntegerFromUnsignedLongEvaluator get(DriverContext context) {
      return new ToIntegerFromUnsignedLongEvaluator(source, ul.get(context), context);
    }

    @Override
    public String toString() {
      return "ToIntegerFromUnsignedLongEvaluator[" + "ul=" + ul + "]";
    }
  }
}
