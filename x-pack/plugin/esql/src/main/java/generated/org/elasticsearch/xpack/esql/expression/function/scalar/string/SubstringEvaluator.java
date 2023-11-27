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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Substring}.
 * This class is generated. Do not edit it.
 */
public final class SubstringEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator start;

  private final EvalOperator.ExpressionEvaluator length;

  private final DriverContext driverContext;

  public SubstringEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator start, EvalOperator.ExpressionEvaluator length,
      DriverContext driverContext) {
    this.str = str;
    this.start = start;
    this.length = length;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (IntBlock startBlock = (IntBlock) start.eval(page)) {
        try (IntBlock lengthBlock = (IntBlock) length.eval(page)) {
          BytesRefVector strVector = strBlock.asVector();
          if (strVector == null) {
            return eval(page.getPositionCount(), strBlock, startBlock, lengthBlock);
          }
          IntVector startVector = startBlock.asVector();
          if (startVector == null) {
            return eval(page.getPositionCount(), strBlock, startBlock, lengthBlock);
          }
          IntVector lengthVector = lengthBlock.asVector();
          if (lengthVector == null) {
            return eval(page.getPositionCount(), strBlock, startBlock, lengthBlock);
          }
          return eval(page.getPositionCount(), strVector, startVector, lengthVector).asBlock();
        }
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, IntBlock startBlock,
      IntBlock lengthBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
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
        if (lengthBlock.isNull(p) || lengthBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(Substring.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), startBlock.getInt(startBlock.getFirstValueIndex(p)), lengthBlock.getInt(lengthBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector strVector, IntVector startVector,
      IntVector lengthVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(Substring.process(strVector.getBytesRef(p, strScratch), startVector.getInt(p), lengthVector.getInt(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "SubstringEvaluator[" + "str=" + str + ", start=" + start + ", length=" + length + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, start, length);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final EvalOperator.ExpressionEvaluator.Factory start;

    private final EvalOperator.ExpressionEvaluator.Factory length;

    public Factory(EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory start,
        EvalOperator.ExpressionEvaluator.Factory length) {
      this.str = str;
      this.start = start;
      this.length = length;
    }

    @Override
    public SubstringEvaluator get(DriverContext context) {
      return new SubstringEvaluator(str.get(context), start.get(context), length.get(context), context);
    }

    @Override
    public String toString() {
      return "SubstringEvaluator[" + "str=" + str + ", start=" + start + ", length=" + length + "]";
    }
  }
}
