// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToBoolean}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToBooleanFromUnsignedLongEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator ul;

  public ToBooleanFromUnsignedLongEvaluator(Source source, EvalOperator.ExpressionEvaluator ul,
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
      return driverContext.blockFactory().newConstantBooleanBlockWith(evalValue(vector, 0), positionCount);
    }
    try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendBoolean(evalValue(vector, p));
      }
      return builder.build();
    }
  }

  private boolean evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return ToBoolean.fromUnsignedLong(value);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
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

  private boolean evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return ToBoolean.fromUnsignedLong(value);
  }

  @Override
  public String toString() {
    return "ToBooleanFromUnsignedLongEvaluator[" + "ul=" + ul + "]";
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
    public ToBooleanFromUnsignedLongEvaluator get(DriverContext context) {
      return new ToBooleanFromUnsignedLongEvaluator(source, ul.get(context), context);
    }

    @Override
    public String toString() {
      return "ToBooleanFromUnsignedLongEvaluator[" + "ul=" + ul + "]";
    }
  }
}
