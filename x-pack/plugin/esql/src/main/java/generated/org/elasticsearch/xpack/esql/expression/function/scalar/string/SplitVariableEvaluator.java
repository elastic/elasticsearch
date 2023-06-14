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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Split}.
 * This class is generated. Do not edit it.
 */
public final class SplitVariableEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator delim;

  private final BytesRef scratch;

  public SplitVariableEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator delim, BytesRef scratch) {
    this.str = str;
    this.delim = delim;
    this.scratch = scratch;
  }

  @Override
  public Block eval(Page page) {
    Block strUncastBlock = str.eval(page);
    if (strUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock strBlock = (BytesRefBlock) strUncastBlock;
    Block delimUncastBlock = delim.eval(page);
    if (delimUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock delimBlock = (BytesRefBlock) delimUncastBlock;
    BytesRefVector strVector = strBlock.asVector();
    if (strVector == null) {
      return eval(page.getPositionCount(), strBlock, delimBlock);
    }
    BytesRefVector delimVector = delimBlock.asVector();
    if (delimVector == null) {
      return eval(page.getPositionCount(), strBlock, delimBlock);
    }
    return eval(page.getPositionCount(), strVector, delimVector);
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock delimBlock) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef delimScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (delimBlock.isNull(p) || delimBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      Split.process(result, strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), delimBlock.getBytesRef(delimBlock.getFirstValueIndex(p), delimScratch), scratch);
    }
    return result.build();
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector,
      BytesRefVector delimVector) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef delimScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      Split.process(result, strVector.getBytesRef(p, strScratch), delimVector.getBytesRef(p, delimScratch), scratch);
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "SplitVariableEvaluator[" + "str=" + str + ", delim=" + delim + "]";
  }
}
