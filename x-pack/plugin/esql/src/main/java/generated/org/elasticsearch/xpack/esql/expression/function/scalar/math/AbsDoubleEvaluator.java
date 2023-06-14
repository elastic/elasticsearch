// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Abs}.
 * This class is generated. Do not edit it.
 */
public final class AbsDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator fieldVal;

  public AbsDoubleEvaluator(EvalOperator.ExpressionEvaluator fieldVal) {
    this.fieldVal = fieldVal;
  }

  @Override
  public Block eval(Page page) {
    Block fieldValUncastBlock = fieldVal.eval(page);
    if (fieldValUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    DoubleBlock fieldValBlock = (DoubleBlock) fieldValUncastBlock;
    DoubleVector fieldValVector = fieldValBlock.asVector();
    if (fieldValVector == null) {
      return eval(page.getPositionCount(), fieldValBlock);
    }
    return eval(page.getPositionCount(), fieldValVector).asBlock();
  }

  public DoubleBlock eval(int positionCount, DoubleBlock fieldValBlock) {
    DoubleBlock.Builder result = DoubleBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (fieldValBlock.isNull(p) || fieldValBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendDouble(Abs.process(fieldValBlock.getDouble(fieldValBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public DoubleVector eval(int positionCount, DoubleVector fieldValVector) {
    DoubleVector.Builder result = DoubleVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendDouble(Abs.process(fieldValVector.getDouble(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "AbsDoubleEvaluator[" + "fieldVal=" + fieldVal + "]";
  }
}
