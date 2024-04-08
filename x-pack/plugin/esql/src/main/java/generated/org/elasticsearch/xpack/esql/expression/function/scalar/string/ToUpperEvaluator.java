// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.Locale;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToUpper}.
 * This class is generated. Do not edit it.
 */
public final class ToUpperEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator val;

  private final Locale locale;

  private final DriverContext driverContext;

  public ToUpperEvaluator(Source source, EvalOperator.ExpressionEvaluator val, Locale locale,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.val = val;
    this.locale = locale;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock valBlock = (BytesRefBlock) val.eval(page)) {
      BytesRefVector valVector = valBlock.asVector();
      if (valVector == null) {
        return eval(page.getPositionCount(), valBlock);
      }
      return eval(page.getPositionCount(), valVector).asBlock();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock valBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef valScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (valBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (valBlock.getValueCount(p) != 1) {
          if (valBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(ToUpper.process(valBlock.getBytesRef(valBlock.getFirstValueIndex(p), valScratch), locale));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector valVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef valScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(ToUpper.process(valVector.getBytesRef(p, valScratch), locale));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ToUpperEvaluator[" + "val=" + val + ", locale=" + locale + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory val;

    private final Locale locale;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val, Locale locale) {
      this.source = source;
      this.val = val;
      this.locale = locale;
    }

    @Override
    public ToUpperEvaluator get(DriverContext context) {
      return new ToUpperEvaluator(source, val.get(context), locale, context);
    }

    @Override
    public String toString() {
      return "ToUpperEvaluator[" + "val=" + val + ", locale=" + locale + "]";
    }
  }
}
