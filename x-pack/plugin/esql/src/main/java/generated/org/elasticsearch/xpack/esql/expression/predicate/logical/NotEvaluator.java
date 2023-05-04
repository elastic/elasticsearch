// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Not}.
 * This class is generated. Do not edit it.
 */
public final class NotEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator v;

  public NotEvaluator(EvalOperator.ExpressionEvaluator v) {
    this.v = v;
  }

  @Override
  public Block eval(Page page) {
    Block vUncastBlock = v.eval(page);
    if (vUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BooleanBlock vBlock = (BooleanBlock) vUncastBlock;
    BooleanVector vVector = vBlock.asVector();
    if (vVector == null) {
      return eval(page.getPositionCount(), vBlock);
    }
    return eval(page.getPositionCount(), vVector).asBlock();
  }

  public BooleanBlock eval(int positionCount, BooleanBlock vBlock) {
    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (vBlock.isNull(p) || vBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBoolean(Not.process(vBlock.getBoolean(vBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public BooleanVector eval(int positionCount, BooleanVector vVector) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBoolean(Not.process(vVector.getBoolean(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "NotEvaluator[" + "v=" + v + "]";
  }
}
