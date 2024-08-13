// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.geo.Component2D;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link SpatialDisjoint}.
 * This class is generated. Do not edit it.
 */
public final class SpatialDisjointGeoPointDocValuesAndConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final Component2D rightValue;

  private final DriverContext driverContext;

  public SpatialDisjointGeoPointDocValuesAndConstantEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, Component2D rightValue,
      DriverContext driverContext) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock leftValueBlock = (LongBlock) leftValue.eval(page)) {
      LongVector leftValueVector = leftValueBlock.asVector();
      if (leftValueVector == null) {
        return eval(page.getPositionCount(), leftValueBlock);
      }
      return eval(page.getPositionCount(), leftValueVector);
    }
  }

  public BooleanBlock eval(int positionCount, LongBlock leftValueBlock) {
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
        try {
          result.appendBoolean(SpatialDisjoint.processGeoPointDocValuesAndConstant(leftValueBlock.getLong(leftValueBlock.getFirstValueIndex(p)), this.rightValue));
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BooleanBlock eval(int positionCount, LongVector leftValueVector) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBoolean(SpatialDisjoint.processGeoPointDocValuesAndConstant(leftValueVector.getLong(p), this.rightValue));
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
    return "SpatialDisjointGeoPointDocValuesAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final Component2D rightValue;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        Component2D rightValue) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
    }

    @Override
    public SpatialDisjointGeoPointDocValuesAndConstantEvaluator get(DriverContext context) {
      return new SpatialDisjointGeoPointDocValuesAndConstantEvaluator(source, leftValue.get(context), rightValue, context);
    }

    @Override
    public String toString() {
      return "SpatialDisjointGeoPointDocValuesAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
    }
  }
}
