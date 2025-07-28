// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link RoundToLong}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RoundToLong7Evaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final long p0;

  private final long p1;

  private final long p2;

  private final long p3;

  private final long p4;

  private final long p5;

  private final long p6;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RoundToLong7Evaluator(Source source, EvalOperator.ExpressionEvaluator field, long p0,
      long p1, long p2, long p3, long p4, long p5, long p6, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.p0 = p0;
    this.p1 = p1;
    this.p2 = p2;
    this.p3 = p3;
    this.p4 = p4;
    this.p5 = p5;
    this.p6 = p6;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock fieldBlock = (LongBlock) field.eval(page)) {
      LongVector fieldVector = fieldBlock.asVector();
      if (fieldVector == null) {
        return eval(page.getPositionCount(), fieldBlock);
      }
      return eval(page.getPositionCount(), fieldVector).asBlock();
    }
  }

  public LongBlock eval(int positionCount, LongBlock fieldBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (fieldBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (fieldBlock.getValueCount(p) != 1) {
          if (fieldBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendLong(RoundToLong.process(fieldBlock.getLong(fieldBlock.getFirstValueIndex(p)), this.p0, this.p1, this.p2, this.p3, this.p4, this.p5, this.p6));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector fieldVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendLong(p, RoundToLong.process(fieldVector.getLong(p), this.p0, this.p1, this.p2, this.p3, this.p4, this.p5, this.p6));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RoundToLong7Evaluator[" + "field=" + field + ", p0=" + p0 + ", p1=" + p1 + ", p2=" + p2 + ", p3=" + p3 + ", p4=" + p4 + ", p5=" + p5 + ", p6=" + p6 + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
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

    private final EvalOperator.ExpressionEvaluator.Factory field;

    private final long p0;

    private final long p1;

    private final long p2;

    private final long p3;

    private final long p4;

    private final long p5;

    private final long p6;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field, long p0, long p1,
        long p2, long p3, long p4, long p5, long p6) {
      this.source = source;
      this.field = field;
      this.p0 = p0;
      this.p1 = p1;
      this.p2 = p2;
      this.p3 = p3;
      this.p4 = p4;
      this.p5 = p5;
      this.p6 = p6;
    }

    @Override
    public RoundToLong7Evaluator get(DriverContext context) {
      return new RoundToLong7Evaluator(source, field.get(context), p0, p1, p2, p3, p4, p5, p6, context);
    }

    @Override
    public String toString() {
      return "RoundToLong7Evaluator[" + "field=" + field + ", p0=" + p0 + ", p1=" + p1 + ", p2=" + p2 + ", p3=" + p3 + ", p4=" + p4 + ", p5=" + p5 + ", p6=" + p6 + "]";
    }
  }
}
