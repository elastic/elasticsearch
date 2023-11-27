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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link EndsWith}.
 * This class is generated. Do not edit it.
 */
public final class EndsWithEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator suffix;

  private final DriverContext driverContext;

  public EndsWithEvaluator(EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator suffix, DriverContext driverContext) {
    this.str = str;
    this.suffix = suffix;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (BytesRefBlock suffixBlock = (BytesRefBlock) suffix.eval(page)) {
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return eval(page.getPositionCount(), strBlock, suffixBlock);
        }
        BytesRefVector suffixVector = suffixBlock.asVector();
        if (suffixVector == null) {
          return eval(page.getPositionCount(), strBlock, suffixBlock);
        }
        return eval(page.getPositionCount(), strVector, suffixVector).asBlock();
      }
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock suffixBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef suffixScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (strBlock.isNull(p) || strBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        if (suffixBlock.isNull(p) || suffixBlock.getValueCount(p) != 1) {
          result.appendNull();
          continue position;
        }
        result.appendBoolean(EndsWith.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), suffixBlock.getBytesRef(suffixBlock.getFirstValueIndex(p), suffixScratch)));
      }
      return result.build();
    }
  }

  public BooleanVector eval(int positionCount, BytesRefVector strVector,
      BytesRefVector suffixVector) {
    try(BooleanVector.Builder result = driverContext.blockFactory().newBooleanVectorBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef suffixScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(EndsWith.process(strVector.getBytesRef(p, strScratch), suffixVector.getBytesRef(p, suffixScratch)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "EndsWithEvaluator[" + "str=" + str + ", suffix=" + suffix + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, suffix);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final EvalOperator.ExpressionEvaluator.Factory suffix;

    public Factory(EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory suffix) {
      this.str = str;
      this.suffix = suffix;
    }

    @Override
    public EndsWithEvaluator get(DriverContext context) {
      return new EndsWithEvaluator(str.get(context), suffix.get(context), context);
    }

    @Override
    public String toString() {
      return "EndsWithEvaluator[" + "str=" + str + ", suffix=" + suffix + "]";
    }
  }
}
