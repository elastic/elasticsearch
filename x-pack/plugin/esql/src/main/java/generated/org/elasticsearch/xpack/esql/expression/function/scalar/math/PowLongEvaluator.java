// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Pow}.
 * This class is generated. Do not edit it.
 */
public final class PowLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator base;

  private final EvalOperator.ExpressionEvaluator exponent;

  public PowLongEvaluator(EvalOperator.ExpressionEvaluator base,
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
    LongBlock baseBlock = (LongBlock) baseUncastBlock;
    Block exponentUncastBlock = exponent.eval(page);
    if (exponentUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    LongBlock exponentBlock = (LongBlock) exponentUncastBlock;
    LongVector baseVector = baseBlock.asVector();
    if (baseVector == null) {
      return eval(page.getPositionCount(), baseBlock, exponentBlock);
    }
    LongVector exponentVector = exponentBlock.asVector();
    if (exponentVector == null) {
      return eval(page.getPositionCount(), baseBlock, exponentBlock);
    }
    return eval(page.getPositionCount(), baseVector, exponentVector).asBlock();
  }

  public LongBlock eval(int positionCount, LongBlock baseBlock, LongBlock exponentBlock) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (baseBlock.isNull(p) || baseBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (exponentBlock.isNull(p) || exponentBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendLong(Pow.process(baseBlock.getLong(baseBlock.getFirstValueIndex(p)), exponentBlock.getLong(exponentBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public LongVector eval(int positionCount, LongVector baseVector, LongVector exponentVector) {
    LongVector.Builder result = LongVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendLong(Pow.process(baseVector.getLong(p), exponentVector.getLong(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "PowLongEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
  }
}
