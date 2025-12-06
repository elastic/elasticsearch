// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link RoundToDouble}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RoundToDouble4Evaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RoundToDouble4Evaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator field;

  private final double p0;

  private final double p1;

  private final double p2;

  private final double p3;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RoundToDouble4Evaluator(Source source, EvalOperator.ExpressionEvaluator field, double p0,
      double p1, double p2, double p3, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.p0 = p0;
    this.p1 = p1;
    this.p2 = p2;
    this.p3 = p3;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock fieldBlock = (DoubleBlock) field.eval(page)) {
      DoubleVector fieldVector = fieldBlock.asVector();
      if (fieldVector == null) {
        return eval(page.getPositionCount(), fieldBlock);
      }
      return eval(page.getPositionCount(), fieldVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock fieldBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (fieldBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        double field = fieldBlock.getDouble(fieldBlock.getFirstValueIndex(p));
        result.appendDouble(RoundToDouble.process(field, this.p0, this.p1, this.p2, this.p3));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, DoubleVector fieldVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        double field = fieldVector.getDouble(p);
        result.appendDouble(p, RoundToDouble.process(field, this.p0, this.p1, this.p2, this.p3));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RoundToDouble4Evaluator[" + "field=" + field + ", p0=" + p0 + ", p1=" + p1 + ", p2=" + p2 + ", p3=" + p3 + "]";
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

    private final double p0;

    private final double p1;

    private final double p2;

    private final double p3;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field, double p0,
        double p1, double p2, double p3) {
      this.source = source;
      this.field = field;
      this.p0 = p0;
      this.p1 = p1;
      this.p2 = p2;
      this.p3 = p3;
    }

    @Override
    public RoundToDouble4Evaluator get(DriverContext context) {
      return new RoundToDouble4Evaluator(source, field.get(context), p0, p1, p2, p3, context);
    }

    @Override
    public String toString() {
      return "RoundToDouble4Evaluator[" + "field=" + field + ", p0=" + p0 + ", p1=" + p1 + ", p2=" + p2 + ", p3=" + p3 + "]";
    }
  }
}
