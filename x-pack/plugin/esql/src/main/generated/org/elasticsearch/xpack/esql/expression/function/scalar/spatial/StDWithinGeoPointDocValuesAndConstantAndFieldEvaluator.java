// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StDWithin}.
 * This class is generated. Do not edit it.
 */
public final class StDWithinGeoPointDocValuesAndConstantAndFieldEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final Point rightValue;

  private final EvalOperator.ExpressionEvaluator distance;

  private final DriverContext driverContext;

  public StDWithinGeoPointDocValuesAndConstantAndFieldEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, Point rightValue,
      EvalOperator.ExpressionEvaluator distance, DriverContext driverContext) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.distance = distance;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock leftValueBlock = (LongBlock) leftValue.eval(page)) {
      try (DoubleBlock distanceBlock = (DoubleBlock) distance.eval(page)) {
        LongVector leftValueVector = leftValueBlock.asVector();
        if (leftValueVector == null) {
          return eval(page.getPositionCount(), leftValueBlock, distanceBlock);
        }
        DoubleVector distanceVector = distanceBlock.asVector();
        if (distanceVector == null) {
          return eval(page.getPositionCount(), leftValueBlock, distanceBlock);
        }
        return eval(page.getPositionCount(), leftValueVector, distanceVector);
      }
    }
  }

  public BooleanBlock eval(int positionCount, LongBlock leftValueBlock, DoubleBlock distanceBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
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
        if (distanceBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (distanceBlock.getValueCount(p) != 1) {
          if (distanceBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        try {
          result.appendBoolean(StDWithin.processGeoPointDocValuesAndConstantAndField(leftValueBlock.getLong(leftValueBlock.getFirstValueIndex(p)), rightValue, distanceBlock.getDouble(distanceBlock.getFirstValueIndex(p))));
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BooleanBlock eval(int positionCount, LongVector leftValueVector,
      DoubleVector distanceVector) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBoolean(StDWithin.processGeoPointDocValuesAndConstantAndField(leftValueVector.getLong(p), rightValue, distanceVector.getDouble(p)));
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StDWithinGeoPointDocValuesAndConstantAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", distance=" + distance + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue, distance);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final Point rightValue;

    private final EvalOperator.ExpressionEvaluator.Factory distance;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        Point rightValue, EvalOperator.ExpressionEvaluator.Factory distance) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
      this.distance = distance;
    }

    @Override
    public StDWithinGeoPointDocValuesAndConstantAndFieldEvaluator get(DriverContext context) {
      return new StDWithinGeoPointDocValuesAndConstantAndFieldEvaluator(source, leftValue.get(context), rightValue, distance.get(context), context);
    }

    @Override
    public String toString() {
      return "StDWithinGeoPointDocValuesAndConstantAndFieldEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + ", distance=" + distance + "]";
    }
  }
}
