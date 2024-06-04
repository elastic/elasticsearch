// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link FromBase64}.
 * This class is generated. Do not edit it.
 */
public final class FromBase64Evaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator field;

  private final BytesRefBuilder oScratch;

  private final DriverContext driverContext;

  public FromBase64Evaluator(Source source, EvalOperator.ExpressionEvaluator field,
      BytesRefBuilder oScratch, DriverContext driverContext) {
    this.field = field;
    this.oScratch = oScratch;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock fieldBlock = (BytesRefBlock) field.eval(page)) {
      BytesRefVector fieldVector = fieldBlock.asVector();
      if (fieldVector == null) {
        return eval(page.getPositionCount(), fieldBlock);
      }
      return eval(page.getPositionCount(), fieldVector).asBlock();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock fieldBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef fieldScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (fieldBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (fieldBlock.getValueCount(p) != 1) {
          if (fieldBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendBytesRef(FromBase64.process(fieldBlock.getBytesRef(fieldBlock.getFirstValueIndex(p), fieldScratch), oScratch));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector fieldVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef fieldScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(FromBase64.process(fieldVector.getBytesRef(p, fieldScratch), oScratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "FromBase64Evaluator[" + "field=" + field + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory field;

    private final Function<DriverContext, BytesRefBuilder> oScratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field,
        Function<DriverContext, BytesRefBuilder> oScratch) {
      this.source = source;
      this.field = field;
      this.oScratch = oScratch;
    }

    @Override
    public FromBase64Evaluator get(DriverContext context) {
      return new FromBase64Evaluator(source, field.get(context), oScratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "FromBase64Evaluator[" + "field=" + field + "]";
    }
  }
}
