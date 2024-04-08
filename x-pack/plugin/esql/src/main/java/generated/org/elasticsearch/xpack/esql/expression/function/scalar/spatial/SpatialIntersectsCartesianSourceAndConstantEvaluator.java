// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link SpatialIntersects}.
 * This class is generated. Do not edit it.
 */
public final class SpatialIntersectsCartesianSourceAndConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final Component2D rightValue;

  private final DriverContext driverContext;

  public SpatialIntersectsCartesianSourceAndConstantEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, Component2D rightValue,
      DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.driverContext = driverContext;
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

  public BooleanBlock eval(int positionCount, BytesRefBlock leftValueBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
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
          result.appendBoolean(SpatialIntersects.processCartesianSourceAndConstant(leftValueBlock.getBytesRef(leftValueBlock.getFirstValueIndex(p), leftValueScratch), rightValue));
        } catch (IllegalArgumentException | IOException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefVector leftValueVector) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      BytesRef leftValueScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBoolean(SpatialIntersects.processCartesianSourceAndConstant(leftValueVector.getBytesRef(p, leftValueScratch), rightValue));
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
    return "SpatialIntersectsCartesianSourceAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
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
    public SpatialIntersectsCartesianSourceAndConstantEvaluator get(DriverContext context) {
      return new SpatialIntersectsCartesianSourceAndConstantEvaluator(source, leftValue.get(context), rightValue, context);
    }

    @Override
    public String toString() {
      return "SpatialIntersectsCartesianSourceAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
    }
  }
}
