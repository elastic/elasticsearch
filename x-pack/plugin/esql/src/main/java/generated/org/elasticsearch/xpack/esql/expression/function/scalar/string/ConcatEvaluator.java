// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Override;
import java.lang.String;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Concat}.
 * This class is generated. Do not edit it.
 */
public final class ConcatEvaluator implements EvalOperator.ExpressionEvaluator {
  private final BytesRefBuilder scratch;

  private final EvalOperator.ExpressionEvaluator[] values;

  public ConcatEvaluator(BytesRefBuilder scratch, EvalOperator.ExpressionEvaluator[] values) {
    this.scratch = scratch;
    this.values = values;
  }

  static BytesRef fold(BytesRefBuilder scratch, List<Expression> values) {
    BytesRef[] valuesVal = new BytesRef[values.size()];
    for (int i = 0; i < valuesVal.length; i++) {
      valuesVal[i] = (BytesRef) values.get(i).fold();
      if (valuesVal[i] == null) {
        return null;
      }
    }
    return Concat.process(scratch, valuesVal);
  }

  @Override
  public Block eval(Page page) {
    BytesRefBlock[] valuesBlocks = new BytesRefBlock[values.length];
    for (int i = 0; i < valuesBlocks.length; i++) {
      Block block = values[i].eval(page);
      if (block.areAllValuesNull()) {
        return Block.constantNullBlock(page.getPositionCount());
      }
      valuesBlocks[i] = (BytesRefBlock) block;
    }
    BytesRefVector[] valuesVectors = new BytesRefVector[values.length];
    for (int i = 0; i < valuesBlocks.length; i++) {
      valuesVectors[i] = valuesBlocks[i].asVector();
      if (valuesVectors[i] == null) {
        return eval(page.getPositionCount(), scratch, valuesBlocks);
      }
    }
    return eval(page.getPositionCount(), scratch, valuesVectors).asBlock();
  }

  public BytesRefBlock eval(int positionCount, BytesRefBuilder scratch,
      BytesRefBlock[] valuesBlocks) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef[] valuesValues = new BytesRef[values.length];
    BytesRef[] valuesScratch = new BytesRef[values.length];
    for (int i = 0; i < values.length; i++) {
      valuesScratch[i] = new BytesRef();
    }
    position: for (int p = 0; p < positionCount; p++) {
      for (int i = 0; i < values.length; i++) {
        if (valuesBlocks[i].isNull(p) || valuesBlocks[i].getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
      }
      for (int i = 0; i < values.length; i++) {
        int o = valuesBlocks[i].getFirstValueIndex(p);
        valuesValues[i] = valuesBlocks[i].getBytesRef(o, valuesScratch[i]);
      }
      result.appendBytesRef(Concat.process(scratch, valuesValues));
    }
    return result.build();
  }

  public BytesRefVector eval(int positionCount, BytesRefBuilder scratch,
      BytesRefVector[] valuesVectors) {
    BytesRefVector.Builder result = BytesRefVector.newVectorBuilder(positionCount);
    BytesRef[] valuesValues = new BytesRef[values.length];
    BytesRef[] valuesScratch = new BytesRef[values.length];
    for (int i = 0; i < values.length; i++) {
      valuesScratch[i] = new BytesRef();
    }
    position: for (int p = 0; p < positionCount; p++) {
      for (int i = 0; i < values.length; i++) {
        valuesValues[i] = valuesVectors[i].getBytesRef(p, valuesScratch[i]);
      }
      result.appendBytesRef(Concat.process(scratch, valuesValues));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "ConcatEvaluator[" + "values=" + Arrays.toString(values) + "]";
  }
}
