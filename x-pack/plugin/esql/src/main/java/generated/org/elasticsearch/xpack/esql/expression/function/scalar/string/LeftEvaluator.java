// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
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
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Left}.
 * This class is generated. Do not edit it.
 */
public final class LeftEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final BytesRef out;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator length;

  public LeftEvaluator(Source source, BytesRef out, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator length) {
    this.warnings = new Warnings(source);
    this.out = out;
    this.str = str;
    this.length = length;
  }

  @Override
  public Block eval(Page page) {
    Block strUncastBlock = str.eval(page);
    if (strUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    BytesRefBlock strBlock = (BytesRefBlock) strUncastBlock;
    Block lengthUncastBlock = length.eval(page);
    if (lengthUncastBlock.areAllValuesNull()) {
      return Block.constantNullBlock(page.getPositionCount());
    }
    IntBlock lengthBlock = (IntBlock) lengthUncastBlock;
    BytesRefVector strVector = strBlock.asVector();
    if (strVector == null) {
      return eval(page.getPositionCount(), strBlock, lengthBlock);
    }
    IntVector lengthVector = lengthBlock.asVector();
    if (lengthVector == null) {
      return eval(page.getPositionCount(), strBlock, lengthBlock);
    }
    return eval(page.getPositionCount(), strVector, lengthVector);
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, IntBlock lengthBlock) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      if (lengthBlock.isNull(p) || lengthBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      try {
        result.appendBytesRef(Left.process(out, strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), lengthBlock.getInt(lengthBlock.getFirstValueIndex(p))));
      } catch (IllegalArgumentException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector, IntVector lengthVector) {
    BytesRefBlock.Builder result = BytesRefBlock.newBlockBuilder(positionCount);
    BytesRef strScratch = new BytesRef();
    position: for (int p = 0; p < positionCount; p++) {
      try {
        result.appendBytesRef(Left.process(out, strVector.getBytesRef(p, strScratch), lengthVector.getInt(p)));
      } catch (IllegalArgumentException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "LeftEvaluator[" + "out=" + out + ", str=" + str + ", length=" + length + "]";
  }
}
