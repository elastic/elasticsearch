// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StDistance}.
 * This class is generated. Do not edit it.
 */
public final class StDistanceCartesianSourceAndConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final Point rightValue;

  private final DriverContext driverContext;

  public StDistanceCartesianSourceAndConstantEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, Point rightValue, DriverContext driverContext) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock leftValueBlock = (BytesRefBlock) leftValue.eval(page)) {
      BytesRefVector leftValueVector = leftValueBlock.asVector();
      if (leftValueVector == null) {
        return eval(page.getPositionCount(), leftValueBlock);
      }
      return eval(page.getPositionCount(), leftValueVector);
    }
  }

  public DoubleBlock eval(int positionCount, BytesRefBlock leftValueBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (leftValueBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (leftValueBlock.getValueCount(p) != 1) {
          if (leftValueBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendDouble(StDistance.processCartesianSourceAndConstant(leftValueBlock.getBytesRef(leftValueBlock.getFirstValueIndex(p), leftValueScratch), this.rightValue));
        } catch (IllegalArgumentException | IOException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public DoubleBlock eval(int positionCount, BytesRefVector leftValueVector) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendDouble(StDistance.processCartesianSourceAndConstant(leftValueVector.getBytesRef(p, leftValueScratch), this.rightValue));
        } catch (IllegalArgumentException | IOException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StDistanceCartesianSourceAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final Point rightValue;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        Point rightValue) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
    }

    @Override
    public StDistanceCartesianSourceAndConstantEvaluator get(DriverContext context) {
      return new StDistanceCartesianSourceAndConstantEvaluator(source, leftValue.get(context), rightValue, context);
    }

    @Override
    public String toString() {
      return "StDistanceCartesianSourceAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
    }
  }
}
