// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.ArithmeticException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link CopySign}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class CopySignEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator magnitude;

  private final EvalOperator.ExpressionEvaluator sign;

  private final DriverContext driverContext;

  private Warnings warnings;

  public CopySignEvaluator(Source source, EvalOperator.ExpressionEvaluator magnitude,
      EvalOperator.ExpressionEvaluator sign, DriverContext driverContext) {
    this.source = source;
    this.magnitude = magnitude;
    this.sign = sign;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock magnitudeBlock = (DoubleBlock) magnitude.eval(page)) {
      try (DoubleBlock signBlock = (DoubleBlock) sign.eval(page)) {
        DoubleVector magnitudeVector = magnitudeBlock.asVector();
        if (magnitudeVector == null) {
          return eval(page.getPositionCount(), magnitudeBlock, signBlock);
        }
        DoubleVector signVector = signBlock.asVector();
        if (signVector == null) {
          return eval(page.getPositionCount(), magnitudeBlock, signBlock);
        }
        return eval(page.getPositionCount(), magnitudeVector, signVector);
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock magnitudeBlock, DoubleBlock signBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
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
        try {
          result.appendDouble(CopySign.process(magnitudeBlock.getDouble(magnitudeBlock.getFirstValueIndex(p)), signBlock.getDouble(signBlock.getFirstValueIndex(p))));
        } catch (ArithmeticException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, DoubleVector magnitudeVector,
      DoubleVector signVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendDouble(CopySign.process(magnitudeVector.getDouble(p), signVector.getDouble(p)));
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
    return "CopySignEvaluator[" + "magnitude=" + magnitude + ", sign=" + sign + "]";
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
    public CopySignEvaluator get(DriverContext context) {
      return new CopySignEvaluator(source, magnitude.get(context), sign.get(context), context);
    }

    @Override
    public String toString() {
      return "CopySignEvaluator[" + "magnitude=" + magnitude + ", sign=" + sign + "]";
    }
  }
}
