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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Substring}.
 * This class is generated. Do not edit it.
 */
public final class SubstringNoLengthEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator start;

  public SubstringNoLengthEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator start) {
    this.str = str;
    this.start = start;
  }

  @Override
  public Block eval(Page page) {
    Block strUncastBlock = str.eval(page);
    if (strUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock strBlock = (BytesRefBlock) strUncastBlock;
    Block startUncastBlock = start.eval(page);
    if (startUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    IntBlock startBlock = (IntBlock) startUncastBlock;
    BytesRefVector strVector = strBlock.asVector();
    if (strVector == null) {
      return eval(page.getPositionCount(), strBlock, startBlock);
    }
    IntVector startVector = startBlock.asVector();
    if (startVector == null) {
      return eval(page.getPositionCount(), strBlock, startBlock);
    }
    return eval(page.getPositionCount(), strVector, startVector).asBlock();
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, IntBlock startBlock) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (startBlock.isNull(p) || startBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBytesRef(Substring.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), startBlock.getInt(startBlock.getFirstValueIndex(p))));
    }
    return result.build();
  }

  public BytesRefVector eval(int positionCount, BytesRefVector strVector, IntVector startVector) {
    BytesRefVector.Builder result = BytesRefVector.newVectorBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBytesRef(Substring.process(strVector.getBytesRef(p, strScratch), startVector.getInt(p)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "SubstringNoLengthEvaluator[" + "str=" + str + ", start=" + start + "]";
  }
}
