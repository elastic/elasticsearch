// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ToBase64}.
 * This class is generated. Do not edit it.
 */
public final class ToBase64Evaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator field;

  private final DriverContext driverContext;

  public ToBase64Evaluator(Source source, EvalOperator.ExpressionEvaluator field,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.field = field;
    this.driverContext = driverContext;
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
      BytesRefBuilder oScratch = new BytesRefBuilder();
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
        result.appendBytesRef(ToBase64.process(fieldBlock.getBytesRef(fieldBlock.getFirstValueIndex(p), fieldScratch), oScratch));
      }
      return result.build();
    }
  }

  public BytesRefVector eval(int positionCount, BytesRefVector fieldVector) {
    try(BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
      BytesRef fieldScratch = new BytesRef();
      BytesRefBuilder oScratch = new BytesRefBuilder();
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBytesRef(ToBase64.process(fieldVector.getBytesRef(p, fieldScratch), oScratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ToBase64Evaluator[" + "field=" + field + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory field;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field) {
      this.source = source;
      this.field = field;
    }

    @Override
    public ToBase64Evaluator get(DriverContext context) {
      return new ToBase64Evaluator(source, field.get(context), context);
    }

    @Override
    public String toString() {
      return "ToBase64Evaluator[" + "field=" + field + "]";
    }
  }
}
