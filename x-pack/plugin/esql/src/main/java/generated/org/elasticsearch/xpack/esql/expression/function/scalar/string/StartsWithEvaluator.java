// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StartsWith}.
 * This class is generated. Do not edit it.
 */
public final class StartsWithEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator prefix;

  public StartsWithEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator prefix) {
    this.str = str;
    this.prefix = prefix;
  }

  @Override
  public Block eval(Page page) {
    Block strUncastBlock = str.eval(page);
    if (strUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock strBlock = (BytesRefBlock) strUncastBlock;
    Block prefixUncastBlock = prefix.eval(page);
    if (prefixUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock prefixBlock = (BytesRefBlock) prefixUncastBlock;
    BytesRefVector strVector = strBlock.asVector();
    if (strVector == null) {
      return eval(page.getPositionCount(), strBlock, prefixBlock);
    }
    BytesRefVector prefixVector = prefixBlock.asVector();
    if (prefixVector == null) {
      return eval(page.getPositionCount(), strBlock, prefixBlock);
    }
    return eval(page.getPositionCount(), strVector, prefixVector).asBlock();
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock prefixBlock) {
    BooleanBlock.Builder result = BooleanBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef prefixScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (prefixBlock.isNull(p) || prefixBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBoolean(StartsWith.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), prefixBlock.getBytesRef(prefixBlock.getFirstValueIndex(p), prefixScratch)));
    }
    return result.build();
  }

  public BooleanVector eval(int positionCount, BytesRefVector strVector,
      BytesRefVector prefixVector) {
    BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef prefixScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBoolean(StartsWith.process(strVector.getBytesRef(p, strScratch), prefixVector.getBytesRef(p, prefixScratch)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "StartsWithEvaluator[" + "str=" + str + ", prefix=" + prefix + "]";
  }
}
