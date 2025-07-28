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
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StGeotileToLong}.
 * This class is generated. Edit {@code ConvertEvaluatorImplementer} instead.
 */
public final class StGeotileToLongFromStringEvaluator extends AbstractConvertFunction.AbstractEvaluator {
  private final EvalOperator.ExpressionEvaluator gridId;

  public StGeotileToLongFromStringEvaluator(Source source, EvalOperator.ExpressionEvaluator gridId,
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
    BytesRefVector vector = (BytesRefVector) v;
    int positionCount = v.getPositionCount();
    BytesRef scratchPad = new BytesRef();
    if (vector.isConstant()) {
      return driverContext.blockFactory().newConstantLongBlockWith(evalValue(vector, 0, scratchPad), positionCount);
    }
    try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      for (int p = 0; p < positionCount; p++) {
        builder.appendLong(evalValue(vector, p, scratchPad));
      }
      return builder.build();
    }
  }

  private long evalValue(BytesRefVector container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return StGeotileToLong.fromString(value);
  }

  @Override
  public Block evalBlock(Block b) {
    BytesRefBlock block = (BytesRefBlock) b;
    int positionCount = block.getPositionCount();
    try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      BytesRef scratchPad = new BytesRef();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = block.getValueCount(p);
        int start = block.getFirstValueIndex(p);
        int end = start + valueCount;
        boolean positionOpened = false;
        boolean valuesAppended = false;
        for (int i = start; i < end; i++) {
          long value = evalValue(block, i, scratchPad);
          if (positionOpened == false && valueCount > 1) {
            builder.beginPositionEntry();
            positionOpened = true;
          }
          builder.appendLong(value);
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

  private long evalValue(BytesRefBlock container, int index, BytesRef scratchPad) {
    BytesRef value = container.getBytesRef(index, scratchPad);
    return StGeotileToLong.fromString(value);
  }

  @Override
  public String toString() {
    return "StGeotileToLongFromStringEvaluator[" + "gridId=" + gridId + "]";
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
    public StGeotileToLongFromStringEvaluator get(DriverContext context) {
      return new StGeotileToLongFromStringEvaluator(source, gridId.get(context), context);
    }

    @Override
    public String toString() {
      return "StGeotileToLongFromStringEvaluator[" + "gridId=" + gridId + "]";
    }
  }
}
