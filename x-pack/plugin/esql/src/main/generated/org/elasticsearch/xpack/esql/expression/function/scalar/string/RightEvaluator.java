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
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Right}.
 * This class is generated. Do not edit it.
 */
public final class RightEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final BytesRef out;

  private final UnicodeUtil.UTF8CodePoint cp;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator length;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RightEvaluator(Source source, BytesRef out, UnicodeUtil.UTF8CodePoint cp,
      EvalOperator.ExpressionEvaluator str, EvalOperator.ExpressionEvaluator length,
      DriverContext driverContext) {
    this.source = source;
    this.out = out;
    this.cp = cp;
    this.str = str;
    this.length = length;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (IntBlock lengthBlock = (IntBlock) length.eval(page)) {
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return eval(page.getPositionCount(), strBlock, lengthBlock);
        }
        IntVector lengthVector = lengthBlock.asVector();
        if (lengthVector == null) {
          return eval(page.getPositionCount(), strBlock, lengthBlock);
        }
        return eval(page.getPositionCount(), strVector, lengthVector).asBlock();
      }
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock strBlock, IntBlock lengthBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (strBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (strBlock.getValueCount(p) != 1) {
          if (strBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (lengthBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (lengthBlock.getValueCount(p) != 1) {
          if (lengthBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(Right.process(this.out, this.cp, strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), lengthBlock.getInt(lengthBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector strVector, IntVector lengthVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(Right.process(this.out, this.cp, strVector.getBytesRef(p, strScratch), lengthVector.getInt(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RightEvaluator[" + "str=" + str + ", length=" + length + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, length);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final Function<DriverContext, BytesRef> out;

    private final Function<DriverContext, UnicodeUtil.UTF8CodePoint> cp;

    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final EvalOperator.ExpressionEvaluator.Factory length;

    public Factory(Source source, Function<DriverContext, BytesRef> out,
        Function<DriverContext, UnicodeUtil.UTF8CodePoint> cp,
        EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory length) {
      this.source = source;
      this.out = out;
      this.cp = cp;
      this.str = str;
      this.length = length;
    }

    @Override
    public RightEvaluator get(DriverContext context) {
      return new RightEvaluator(source, out.apply(context), cp.apply(context), str.get(context), length.get(context), context);
    }

    @Override
    public String toString() {
      return "RightEvaluator[" + "str=" + str + ", length=" + length + "]";
    }
  }
}
