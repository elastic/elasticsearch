// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToString}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class ToStringFromIntEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator integer;

  public ToStringFromIntEvaluator(Source source, EvalOperator.ExpressionEvaluator integer,
      DriverContext driverContext) {
    super(driverContext, source);
    this.integer = integer;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return integer;
  }

  @Override
  public Block evalVector(Vector v) {
    IntVector vector = (IntVector) v;
    int positionCount = v.getPositionCount();
    if (vector.isConstant()) {
      return driverContext.blockFactory().newConstantBytesRefBlockWith(evalValue(vector, 0), positionCount);
    }
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendBytesRef(evalValue(vector, p));
      }
      return builder.build();
    }
  }

  private BytesRef evalValue(IntVector container, int index) {
    int value = container.getInt(index);
    return ToString.fromDouble(value);
  }

  @Override
  public Block evalBlock(Block b) {
    IntBlock block = (IntBlock) b;
    int positionCount = block.getPositionCount();
    try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          BytesRef value = evalValue(block, i);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendBytesRef(value);
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

  private BytesRef evalValue(IntBlock container, int index) {
    int value = container.getInt(index);
    return ToString.fromDouble(value);
  }

  @Override
  public String toString() {
    return "ToStringFromIntEvaluator[" + "integer=" + integer + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(integer);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory integer;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory integer) {
      this.source = source;
      this.integer = integer;
    }

    @Override
    public ToStringFromIntEvaluator get(DriverContext context) {
      return new ToStringFromIntEvaluator(source, integer.get(context), context);
    }

    @Override
    public String toString() {
      return "ToStringFromIntEvaluator[" + "integer=" + integer + "]";
    }
  }
}
