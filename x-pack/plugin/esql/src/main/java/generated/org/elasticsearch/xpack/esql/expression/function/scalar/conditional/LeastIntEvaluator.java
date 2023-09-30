// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Least}.
 * This class is generated. Do not edit it.
 */
public final class LeastIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator[] values;

  private final DriverContext driverContext;

  public LeastIntEvaluator(EvalOperator.ExpressionEvaluator[] values, DriverContext driverContext) {
    this.values = values;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    Block.Ref[] valuesRefs = new Block.Ref[values.length];
    try (Releasable valuesRelease = Releasables.wrap(valuesRefs)) {
      IntBlock[] valuesBlocks = new IntBlock[values.length];
      for (int i = 0; i < valuesBlocks.length; i++) {
        valuesRefs[i] = values[i].eval(page);
        Block block = valuesRefs[i].block();
        if (block.areAllValuesNull()) {
          return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
        }
        valuesBlocks[i] = (IntBlock) block;
      }
      IntVector[] valuesVectors = new IntVector[values.length];
      for (int i = 0; i < valuesBlocks.length; i++) {
        valuesVectors[i] = valuesBlocks[i].asVector();
        if (valuesVectors[i] == null) {
          return Block.Ref.floating(eval(page.getPositionCount(), valuesBlocks));
        }
      }
      return Block.Ref.floating(eval(page.getPositionCount(), valuesVectors).asBlock());
    }
  }

  public IntBlock eval(int positionCount, IntBlock[] valuesBlocks) {
    IntBlock.Builder result = IntBlock.newBlockBuilder(positionCount);
    int[] valuesValues = new int[values.length];
    position: for (int p = 0; p < positionCount; p++) {
      for (int i = 0; i < valuesBlocks.length; i++) {
        if (valuesBlocks[i].isNull(p) || valuesBlocks[i].getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
      }
      // unpack valuesBlocks into valuesValues
      for (int i = 0; i < valuesBlocks.length; i++) {
        int o = valuesBlocks[i].getFirstValueIndex(p);
        valuesValues[i] = valuesBlocks[i].getInt(o);
      }
      result.appendInt(Least.process(valuesValues));
    }
    return result.build();
  }

  public IntVector eval(int positionCount, IntVector[] valuesVectors) {
    IntVector.Builder result = IntVector.newVectorBuilder(positionCount);
    int[] valuesValues = new int[values.length];
    position: for (int p = 0; p < positionCount; p++) {
      // unpack valuesVectors into valuesValues
      for (int i = 0; i < valuesVectors.length; i++) {
        valuesValues[i] = valuesVectors[i].getInt(p);
      }
      result.appendInt(Least.process(valuesValues));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "LeastIntEvaluator[" + "values=" + Arrays.toString(values) + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(() -> Releasables.close(values));
  }
}
