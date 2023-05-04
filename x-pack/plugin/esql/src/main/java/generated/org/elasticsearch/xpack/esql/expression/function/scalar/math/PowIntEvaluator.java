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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Pow}.
 * This class is generated. Do not edit it.
 */
public final class PowIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator base;

  private final EvalOperator.ExpressionEvaluator exponent;

  public PowIntEvaluator(EvalOperator.ExpressionEvaluator base,
      EvalOperator.ExpressionEvaluator exponent) {
    this.base = base;
    this.exponent = exponent;
  }

  @Override
  public Block eval(Page page) {
    Block baseUncastBlock = base.eval(page);
    if (baseUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    IntBlock baseBlock = (IntBlock) baseUncastBlock;
    Block exponentUncastBlock = exponent.eval(page);
    if (exponentUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    IntBlock exponentBlock = (IntBlock) exponentUncastBlock;
    IntVector baseVector = baseBlock.asVector();
    if (baseVector == null) {
      return eval(page.getPositionCount(), baseBlock, exponentBlock);
    }
    IntVector exponentVector = exponentBlock.asVector();
    if (exponentVector == null) {
      return eval(page.getPositionCount(), baseBlock, exponentBlock);
    }
    return eval(page.getPositionCount(), baseVector, exponentVector).asBlock();
  }

  public IntBlock eval(int positionCount, IntBlock baseBlock, IntBlock exponentBlock) {
    IntBlock.Builder result = IntBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (baseBlock.isNull(p) || baseBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (exponentBlock.isNull(p) || exponentBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendInt(Pow.process(baseBlock.getInt(baseBlock.getFirstValueIndex(p)), exponentBlock.getInt(exponentBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public IntVector eval(int positionCount, IntVector baseVector, IntVector exponentVector) {
    IntVector.Builder result = IntVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendInt(Pow.process(baseVector.getInt(p), exponentVector.getInt(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "PowIntEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
  }
}
