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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Locate}.
 * This class is generated. Do not edit it.
 */
public final class LocateNoStartEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator str;

  private final EvalOperator.ExpressionEvaluator substr;

  private final DriverContext driverContext;

  private Warnings warnings;

  public LocateNoStartEvaluator(Source source, EvalOperator.ExpressionEvaluator str,
      EvalOperator.ExpressionEvaluator substr, DriverContext driverContext) {
    this.source = source;
    this.str = str;
    this.substr = substr;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock strBlock = (BytesRefBlock) str.eval(page)) {
      try (BytesRefBlock substrBlock = (BytesRefBlock) substr.eval(page)) {
        BytesRefVector strVector = strBlock.asVector();
        if (strVector == null) {
          return eval(page.getPositionCount(), strBlock, substrBlock);
        }
        BytesRefVector substrVector = substrBlock.asVector();
        if (substrVector == null) {
          return eval(page.getPositionCount(), strBlock, substrBlock);
        }
        return eval(page.getPositionCount(), strVector, substrVector).asBlock();
      }
    }
  }

  public IntBlock eval(int positionCount, BytesRefBlock strBlock, BytesRefBlock substrBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef substrScratch = new BytesRef();
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
        if (substrBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (substrBlock.getValueCount(p) != 1) {
          if (substrBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendInt(Locate.process(strBlock.getBytesRef(strBlock.getFirstValueIndex(p), strScratch), substrBlock.getBytesRef(substrBlock.getFirstValueIndex(p), substrScratch)));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, BytesRefVector strVector, BytesRefVector substrVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      BytesRef strScratch = new BytesRef();
      BytesRef substrScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendInt(p, Locate.process(strVector.getBytesRef(p, strScratch), substrVector.getBytesRef(p, substrScratch)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "LocateNoStartEvaluator[" + "str=" + str + ", substr=" + substr + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(str, substr);
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

    private final EvalOperator.ExpressionEvaluator.Factory str;

    private final EvalOperator.ExpressionEvaluator.Factory substr;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory str,
        EvalOperator.ExpressionEvaluator.Factory substr) {
      this.source = source;
      this.str = str;
      this.substr = substr;
    }

    @Override
    public LocateNoStartEvaluator get(DriverContext context) {
      return new LocateNoStartEvaluator(source, str.get(context), substr.get(context), context);
    }

    @Override
    public String toString() {
      return "LocateNoStartEvaluator[" + "str=" + str + ", substr=" + substr + "]";
    }
  }
}
