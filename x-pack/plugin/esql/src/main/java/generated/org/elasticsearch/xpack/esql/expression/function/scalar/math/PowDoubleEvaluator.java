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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Pow}.
 * This class is generated. Do not edit it.
 */
public final class PowDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator base;

  private final EvalOperator.ExpressionEvaluator exponent;

  public PowDoubleEvaluator(EvalOperator.ExpressionEvaluator base,
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
    DoubleBlock baseBlock = (DoubleBlock) baseUncastBlock;
    Block exponentUncastBlock = exponent.eval(page);
    if (exponentUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    DoubleBlock exponentBlock = (DoubleBlock) exponentUncastBlock;
    DoubleVector baseVector = baseBlock.asVector();
    if (baseVector == null) {
      return eval(page.getPositionCount(), baseBlock, exponentBlock);
    }
    DoubleVector exponentVector = exponentBlock.asVector();
    if (exponentVector == null) {
      return eval(page.getPositionCount(), baseBlock, exponentBlock);
    }
    return eval(page.getPositionCount(), baseVector, exponentVector).asBlock();
  }

  public DoubleBlock eval(int positionCount, DoubleBlock baseBlock, DoubleBlock exponentBlock) {
    DoubleBlock.Builder result = DoubleBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (baseBlock.isNull(p) || baseBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (exponentBlock.isNull(p) || exponentBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendDouble(Pow.process(baseBlock.getDouble(baseBlock.getFirstValueIndex(p)), exponentBlock.getDouble(exponentBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public DoubleVector eval(int positionCount, DoubleVector baseVector,
      DoubleVector exponentVector) {
    DoubleVector.Builder result = DoubleVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendDouble(Pow.process(baseVector.getDouble(p), exponentVector.getDouble(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "PowDoubleEvaluator[" + "base=" + base + ", exponent=" + exponent + "]";
  }
}
