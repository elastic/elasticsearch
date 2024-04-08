// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Split}.
 * This class is generated. Do not edit it.
 */
public final class SplitSingleByteEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator str;

  private final byte delim;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  public SplitSingleByteEvaluator(Source source, EvalOperator.ExpressionEvaluator str, byte delim,
      BytesRef scratch, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.str = str;
    this.delim = delim;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      BytesRefVector strVector = strBlock.asVector();
      if (strVector == null) {
        return eval(page.getPositionCount(), strBlock);
      }
      return eval(page.getPositionCount(), strVector);
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (strBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (strBlock.getValueCount(p) != 1) {
          if (strBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        Split.process(result, strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), delim, scratch);
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector strVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        Split.process(result, strVector.getBytesRef(p, strScratch), delim, scratch);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "SplitSingleByteEvaluator[" + "str=" + str + ", delim=" + delim + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final byte delim;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str, byte delim,
        Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.str = str;
      this.delim = delim;
      this.scratch = scratch;
    }

    @Override
    public SplitSingleByteEvaluator get(DriverContext context) {
      return new SplitSingleByteEvaluator(source, str.get(context), delim, scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "SplitSingleByteEvaluator[" + "str=" + str + ", delim=" + delim + "]";
    }
  }
}
