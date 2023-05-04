// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Round}.
 * This class is generated. Do not edit it.
 */
public final class RoundIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  private final EvalOperator.ExpressionEvaluator decimals;

  public RoundIntEvaluator(EvalOperator.ExpressionEvaluator val,
      EvalOperator.ExpressionEvaluator decimals) {
    this.val = val;
    this.decimals = decimals;
  }

  @Override
  public Block eval(Page page) {
    Block valUncastBlock = val.eval(page);
    if (valUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    IntBlock valBlock = (IntBlock) valUncastBlock;
    Block decimalsUncastBlock = decimals.eval(page);
    if (decimalsUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    LongBlock decimalsBlock = (LongBlock) decimalsUncastBlock;
    IntVector valVector = valBlock.asVector();
    if (valVector == null) {
      return eval(page.getPositionCount(), valBlock, decimalsBlock);
    }
    LongVector decimalsVector = decimalsBlock.asVector();
    if (decimalsVector == null) {
      return eval(page.getPositionCount(), valBlock, decimalsBlock);
    }
    return eval(page.getPositionCount(), valVector, decimalsVector).asBlock();
  }

  public IntBlock eval(int positionCount, IntBlock valBlock, LongBlock decimalsBlock) {
    IntBlock.Builder result = IntBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (valBlock.isNull(p) || valBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (decimalsBlock.isNull(p) || decimalsBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendInt(Round.process(valBlock.getInt(valBlock.getFirstValueIndex(p)), decimalsBlock.getLong(decimalsBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public IntVector eval(int positionCount, IntVector valVector, LongVector decimalsVector) {
    IntVector.Builder result = IntVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendInt(Round.process(valVector.getInt(p), decimalsVector.getLong(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "RoundIntEvaluator[" + "val=" + val + ", decimals=" + decimals + "]";
  }
}
