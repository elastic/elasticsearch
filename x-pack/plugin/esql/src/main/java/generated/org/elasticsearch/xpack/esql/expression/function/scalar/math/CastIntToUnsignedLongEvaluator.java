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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Cast}.
 * This class is generated. Do not edit it.
 */
public final class CastIntToUnsignedLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator v;

  public CastIntToUnsignedLongEvaluator(EvalOperator.ExpressionEvaluator v) {
    this.v = v;
  }

  @Override
  public Block eval(Page page) {
    Block vUncastBlock = v.eval(page);
    if (vUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    IntBlock vBlock = (IntBlock) vUncastBlock;
    IntVector vVector = vBlock.asVector();
    if (vVector == null) {
      return eval(page.getPositionCount(), vBlock);
    }
    return eval(page.getPositionCount(), vVector).asBlock();
  }

  public LongBlock eval(int positionCount, IntBlock vBlock) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (vBlock.isNull(p) || vBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendLong(Cast.castIntToUnsignedLong(vBlock.getInt(vBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public LongVector eval(int positionCount, IntVector vVector) {
    LongVector.Builder result = LongVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendLong(Cast.castIntToUnsignedLong(vVector.getInt(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "CastIntToUnsignedLongEvaluator[" + "v=" + v + "]";
  }
}
