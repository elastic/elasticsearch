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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Nothing}.
 * This class is generated. Do not edit it.
 */
public final class NothingEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final BytesRef out;

  private final UnicodeUtil.UTF8CodePoint cp;

  private final EvalOperator.ExpressionEvaluator str;

  private final DriverContext driverContext;

  public NothingEvaluator(Source source, BytesRef out, UnicodeUtil.UTF8CodePoint cp,
      EvalOperator.ExpressionEvaluator str, DriverContext driverContext) {
    this.out = out;
    this.cp = cp;
    this.str = str;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      BytesRefVector strVector = strBlock.asVector();
      if (strVector == null) {
        return eval(page.getPositionCount(), strBlock);
      }
      return eval(page.getPositionCount(), strVector).asBlock();
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
        result.appendBytesRef(Nothing.process(this.out, this.cp, strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch)));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector strVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(Nothing.process(this.out, this.cp, strVector.getBytesRef(p, strScratch)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "NothingEvaluator[" + "str=" + str + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final Function<DriverContext, BytesRef> out;

    private final Function<DriverContext, UnicodeUtil.UTF8CodePoint> cp;

    private final EvalOperator.ExpressionEvaluator.Factory str;

    public Factory(Source source, Function<DriverContext, BytesRef> out,
        Function<DriverContext, UnicodeUtil.UTF8CodePoint> cp,
        EvalOperator.ExpressionEvaluator.Factory str) {
      this.source = source;
      this.out = out;
      this.cp = cp;
      this.str = str;
    }

    @Override
    public NothingEvaluator get(DriverContext context) {
      return new NothingEvaluator(source, out.apply(context), cp.apply(context), str.get(context), context);
    }

    @Override
    public String toString() {
      return "NothingEvaluator[" + "str=" + str + "]";
    }
  }
}
