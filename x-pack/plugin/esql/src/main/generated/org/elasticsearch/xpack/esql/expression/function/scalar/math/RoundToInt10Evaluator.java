// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link RoundToInt}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RoundToInt10Evaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final int p0;

  private final int p1;

  private final int p2;

  private final int p3;

  private final int p4;

  private final int p5;

  private final int p6;

  private final int p7;

  private final int p8;

  private final int p9;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RoundToInt10Evaluator(Source source, EvalOperator.ExpressionEvaluator field, int p0,
      int p1, int p2, int p3, int p4, int p5, int p6, int p7, int p8, int p9,
      DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.p0 = p0;
    this.p1 = p1;
    this.p2 = p2;
    this.p3 = p3;
    this.p4 = p4;
    this.p5 = p5;
    this.p6 = p6;
    this.p7 = p7;
    this.p8 = p8;
    this.p9 = p9;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock fieldBlock = (IntBlock) field.eval(page)) {
      IntVector fieldVector = fieldBlock.asVector();
      if (fieldVector == null) {
        return eval(page.getPositionCount(), fieldBlock);
      }
      return eval(page.getPositionCount(), fieldVector).asBlock();
    }
  }

  public IntBlock eval(int positionCount, IntBlock fieldBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
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
        result.appendInt(RoundToInt.process(fieldBlock.getInt(fieldBlock.getFirstValueIndex(p)), this.p0, this.p1, this.p2, this.p3, this.p4, this.p5, this.p6, this.p7, this.p8, this.p9));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, IntVector fieldVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendInt(p, RoundToInt.process(fieldVector.getInt(p), this.p0, this.p1, this.p2, this.p3, this.p4, this.p5, this.p6, this.p7, this.p8, this.p9));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RoundToInt10Evaluator[" + "field=" + field + ", p0=" + p0 + ", p1=" + p1 + ", p2=" + p2 + ", p3=" + p3 + ", p4=" + p4 + ", p5=" + p5 + ", p6=" + p6 + ", p7=" + p7 + ", p8=" + p8 + ", p9=" + p9 + "]";
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

    private final int p0;

    private final int p1;

    private final int p2;

    private final int p3;

    private final int p4;

    private final int p5;

    private final int p6;

    private final int p7;

    private final int p8;

    private final int p9;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field, int p0, int p1,
        int p2, int p3, int p4, int p5, int p6, int p7, int p8, int p9) {
      this.source = source;
      this.field = field;
      this.p0 = p0;
      this.p1 = p1;
      this.p2 = p2;
      this.p3 = p3;
      this.p4 = p4;
      this.p5 = p5;
      this.p6 = p6;
      this.p7 = p7;
      this.p8 = p8;
      this.p9 = p9;
    }

    @Override
    public RoundToInt10Evaluator get(DriverContext context) {
      return new RoundToInt10Evaluator(source, field.get(context), p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, context);
    }

    @Override
    public String toString() {
      return "RoundToInt10Evaluator[" + "field=" + field + ", p0=" + p0 + ", p1=" + p1 + ", p2=" + p2 + ", p3=" + p3 + ", p4=" + p4 + ", p5=" + p5 + ", p6=" + p6 + ", p7=" + p7 + ", p8=" + p8 + ", p9=" + p9 + "]";
    }
  }
}
