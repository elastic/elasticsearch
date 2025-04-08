// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StGeotileToString}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class StGeotileToStringFromLongEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator gridId;

  public StGeotileToStringFromLongEvaluator(Source source, EvalOperator.ExpressionEvaluator gridId,
      DriverContext driverContext) {
    super(driverContext, source);
    this.gridId = gridId;
  }

  @Override
  public EvalOperator.ExpressionEvaluator next() {
    return gridId;
  }

  @Override
  public Block evalVector(Vector v) {
    LongVector vector = (LongVector) v;
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

  private BytesRef evalValue(LongVector container, int index) {
    long value = container.getLong(index);
    return StGeotileToString.fromLong(value);
  }

  @Override
  public Block evalBlock(Block b) {
    LongBlock block = (LongBlock) b;
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

  private BytesRef evalValue(LongBlock container, int index) {
    long value = container.getLong(index);
    return StGeotileToString.fromLong(value);
  }

  @Override
  public String toString() {
    return "StGeotileToStringFromLongEvaluator[" + "gridId=" + gridId + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(gridId);
  }

  public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory gridId;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory gridId) {
      this.source = source;
      this.gridId = gridId;
    }

    @Override
    public StGeotileToStringFromLongEvaluator get(DriverContext context) {
      return new StGeotileToStringFromLongEvaluator(source, gridId.get(context), context);
    }

    @Override
    public String toString() {
      return "StGeotileToStringFromLongEvaluator[" + "gridId=" + gridId + "]";
    }
  }
}
