// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Trim}.
 * This class is generated. Do not edit it.
 */
public final class TrimEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator val;

  public TrimEvaluator(EvalOperator.ExpressionEvaluator val) {
    this.val = val;
  }

  @Override
  public Block eval(Page page) {
    Block valUncastBlock = val.eval(page);
    if (valUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock valBlock = (BytesRefBlock) valUncastBlock;
    BytesRefVector valVector = valBlock.asVector();
    if (valVector == null) {
      return eval(page.getPositionCount(), valBlock);
    }
    return eval(page.getPositionCount(), valVector).asBlock();
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock valBlock) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef valScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (valBlock.isNull(p) || valBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBytesRef(Trim.process(valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch)));
    }
    return result.build();
  }

  public BytesRefVector eval(int positionCount, BytesRefVector valVector) {
    BytesRefVector.Builder result = BytesRefVector.newVectorBuilder(positionCount);
    BytesRef valScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBytesRef(Trim.process(valVector.getBytesRef(p, valScratch)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "TrimEvaluator[" + "val=" + val + "]";
  }
}
