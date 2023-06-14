// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link LessThanOrEqual}.
 * This class is generated. Do not edit it.
 */
public final class LessThanOrEqualKeywordsEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator lhs;

  private final EvalOperator.ExpressionEvaluator rhs;

  public LessThanOrEqualKeywordsEvaluator(EvalOperator.ExpressionEvaluator lhs,
      EvalOperator.ExpressionEvaluator rhs) {
    this.lhs = lhs;
    this.rhs = rhs;
  }

  @Override
  public Block eval(Page page) {
    Block lhsUncastBlock = lhs.eval(page);
    if (lhsUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock lhsBlock = (BytesRefBlock) lhsUncastBlock;
    Block rhsUncastBlock = rhs.eval(page);
    if (rhsUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock rhsBlock = (BytesRefBlock) rhsUncastBlock;
    BytesRefVector lhsVector = lhsBlock.asVector();
    if (lhsVector == null) {
      return eval(page.getPositionCount(), lhsBlock, rhsBlock);
    }
    BytesRefVector rhsVector = rhsBlock.asVector();
    if (rhsVector == null) {
      return eval(page.getPositionCount(), lhsBlock, rhsBlock);
    }
    return eval(page.getPositionCount(), lhsVector, rhsVector).asBlock();
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock lhsBlock, BytesRefBlock rhsBlock) {
    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
    BytesRef lhsScratch = new BytesRef();
    BytesRef rhsScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (lhsBlock.isNull(p) || lhsBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (rhsBlock.isNull(p) || rhsBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBoolean(LessThanOrEqual.processKeywords(lhsBlock.getBytesRef(lhsBlock.getFirstValueIndex(p), lhsScratch), rhsBlock.getBytesRef(rhsBlock.getFirstValueIndex(p), rhsScratch)));
    }
    return result.build();
  }

  public BooleanVector eval(int positionCount, BytesRefVector lhsVector, BytesRefVector rhsVector) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    BytesRef lhsScratch = new BytesRef();
    BytesRef rhsScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBoolean(LessThanOrEqual.processKeywords(lhsVector.getBytesRef(p, lhsScratch), rhsVector.getBytesRef(p, rhsScratch)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "LessThanOrEqualKeywordsEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
  }
}
