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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Replace}.
 * This class is generated. Do not edit it.
 */
public final class ReplaceEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator regex;

  private final EvalOperator.ExpressionEvaluator newStr;

  public ReplaceEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator regex, EvalOperator.ExpressionEvaluator newStr) {
    this.str = str;
    this.regex = regex;
    this.newStr = newStr;
  }

  @Override
  public Block eval(Page page) {
    Block strUncastBlock = str.eval(page);
    if (strUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock strBlock = (BytesRefBlock) strUncastBlock;
    Block regexUncastBlock = regex.eval(page);
    if (regexUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock regexBlock = (BytesRefBlock) regexUncastBlock;
    Block newStrUncastBlock = newStr.eval(page);
    if (newStrUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock newStrBlock = (BytesRefBlock) newStrUncastBlock;
    BytesRefVector strVector = strBlock.asVector();
    if (strVector == null) {
      return eval(page.getPositionCount(), strBlock, regexBlock, newStrBlock);
    }
    BytesRefVector regexVector = regexBlock.asVector();
    if (regexVector == null) {
      return eval(page.getPositionCount(), strBlock, regexBlock, newStrBlock);
    }
    BytesRefVector newStrVector = newStrBlock.asVector();
    if (newStrVector == null) {
      return eval(page.getPositionCount(), strBlock, regexBlock, newStrBlock);
    }
    return eval(page.getPositionCount(), strVector, regexVector, newStrVector).asBlock();
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock regexBlock,
      BytesRefBlock newStrBlock) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef regexScratch = new BytesRef();
    BytesRef newStrScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (regexBlock.isNull(p) || regexBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (newStrBlock.isNull(p) || newStrBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      result.appendBytesRef(Replace.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), regexBlock.getBytesRef(regexBlock.getFirstValueIndex(p), regexScratch), newStrBlock.getBytesRef(newStrBlock.getFirstValueIndex(p), newStrScratch)));
    }
    return result.build();
  }

  public BytesRefVector eval(int positionCount, BytesRefVector strVector,
      BytesRefVector regexVector, BytesRefVector newStrVector) {
    BytesRefVector.Builder result = BytesRefVector.newVectorBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    BytesRef regexScratch = new BytesRef();
    BytesRef newStrScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      result.appendBytesRef(Replace.process(strVector.getBytesRef(p, strScratch), regexVector.getBytesRef(p, regexScratch), newStrVector.getBytesRef(p, newStrScratch)));
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "ReplaceEvaluator[" + "str=" + str + ", regex=" + regex + ", newStr=" + newStr + "]";
  }
}
