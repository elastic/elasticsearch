// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Div}.
 * This class is generated. Edit {@code DenseVectorEvaluatorImplementer} instead.
 */
public final class DivDenseVectorIntEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DivDenseVectorIntEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator lhs;

  private final EvalOperator.ExpressionEvaluator rhs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public DivDenseVectorIntEvaluator(Source source, EvalOperator.ExpressionEvaluator lhs,
      EvalOperator.ExpressionEvaluator rhs, DriverContext driverContext) {
    this.source = source;
    this.lhs = lhs;
    this.rhs = rhs;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (FloatBlock lhsBlock = (FloatBlock) lhs.eval(page)) {
      try (IntBlock rhsBlock = (IntBlock) rhs.eval(page)) {
        return eval(page.getPositionCount(), lhsBlock, rhsBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += lhs.baseRamBytesUsed();
    baseRamBytesUsed += rhs.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public FloatBlock eval(int positionCount, FloatBlock lhsBlock, IntBlock rhsBlock) {
    try(FloatBlock.Builder result = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (rhsBlock.getValueCount(p)) {
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
        int rhs = rhsBlock.getInt(rhsBlock.getFirstValueIndex(p));
        int lhsValueCount = lhsBlock.getValueCount(p);
        float[] buffer = new float[lhsValueCount];
        try {
          int i = 0;
          int lhsStart = lhsBlock.getFirstValueIndex(p);
          int lhsEnd = lhsStart + lhsValueCount;
          for (int lhsOffset = lhsStart; lhsOffset < lhsEnd; lhsOffset++) {
            float lhsValue = lhsBlock.getFloat(lhsOffset);
            buffer[i++] = Div.processDenseVectorInt(lhsValue, rhs);
          }
          result.beginPositionEntry();
          for (i = 0; i < lhsValueCount; i++) {
            result.appendFloat(buffer[i]);
          }
          result.endPositionEntry();
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "DivDenseVectorIntEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(lhs, rhs);
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

    private final EvalOperator.ExpressionEvaluator.Factory lhs;

    private final EvalOperator.ExpressionEvaluator.Factory rhs;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory lhs,
        EvalOperator.ExpressionEvaluator.Factory rhs) {
      this.source = source;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public DivDenseVectorIntEvaluator get(DriverContext context) {
      return new DivDenseVectorIntEvaluator(source, lhs.get(context), rhs.get(context), context);
    }

    @Override
    public String toString() {
      return "DivDenseVectorIntEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }
}
