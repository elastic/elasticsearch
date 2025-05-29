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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link CopySign}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class CopySignLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator magnitude;

  private final EvalOperator.ExpressionEvaluator sign;

  private final DriverContext driverContext;

  private Warnings warnings;

  public CopySignLongEvaluator(Source source, EvalOperator.ExpressionEvaluator magnitude,
      EvalOperator.ExpressionEvaluator sign, DriverContext driverContext) {
    this.source = source;
    this.magnitude = magnitude;
    this.sign = sign;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock magnitudeBlock = (LongBlock) magnitude.eval(page)) {
      try (LongBlock signBlock = (LongBlock) sign.eval(page)) {
        LongVector magnitudeVector = magnitudeBlock.asVector();
        if (magnitudeVector == null) {
          return eval(page.getPositionCount(), magnitudeBlock, signBlock);
        }
        LongVector signVector = signBlock.asVector();
        if (signVector == null) {
          return eval(page.getPositionCount(), magnitudeBlock, signBlock);
        }
        return eval(page.getPositionCount(), magnitudeVector, signVector).asBlock();
      }
    }
  }

  public LongBlock eval(int positionCount, LongBlock magnitudeBlock, LongBlock signBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        if (magnitudeBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (magnitudeBlock.getValueCount(p) != 1) {
          if (magnitudeBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (signBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (signBlock.getValueCount(p) != 1) {
          if (signBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        result.appendLong(CopySign.processLong(magnitudeBlock.getLong(magnitudeBlock.getFirstValueIndex(p)), signBlock.getLong(signBlock.getFirstValueIndex(p))));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector magnitudeVector, LongVector signVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendLong(p, CopySign.processLong(magnitudeVector.getLong(p), signVector.getLong(p)));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "CopySignLongEvaluator[" + "magnitude=" + magnitude + ", sign=" + sign + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(magnitude, sign);
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

    private final EvalOperator.ExpressionEvaluator.Factory magnitude;

    private final EvalOperator.ExpressionEvaluator.Factory sign;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory magnitude,
        EvalOperator.ExpressionEvaluator.Factory sign) {
      this.source = source;
      this.magnitude = magnitude;
      this.sign = sign;
    }

    @Override
    public CopySignLongEvaluator get(DriverContext context) {
      return new CopySignLongEvaluator(source, magnitude.get(context), sign.get(context), context);
    }

    @Override
    public String toString() {
      return "CopySignLongEvaluator[" + "magnitude=" + magnitude + ", sign=" + sign + "]";
    }
  }
}
