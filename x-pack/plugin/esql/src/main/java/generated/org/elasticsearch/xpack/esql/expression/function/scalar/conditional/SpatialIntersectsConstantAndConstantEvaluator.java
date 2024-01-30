// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.geo.Component2D;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link SpatialIntersects}.
 * This class is generated. Do not edit it.
 */
public final class SpatialIntersectsConstantAndConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final GeometryDocValueReader leftValue;

  private final Component2D rightValue;

  private final DriverContext driverContext;

  public SpatialIntersectsConstantAndConstantEvaluator(Source source,
      GeometryDocValueReader leftValue, Component2D rightValue, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount());
  }

  public BooleanBlock eval(int positionCount) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBoolean(SpatialIntersects.processConstantAndConstant(leftValue, rightValue));
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
    return "SpatialIntersectsConstantAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }

  @Override
  public void close() {
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final GeometryDocValueReader leftValue;

    private final Component2D rightValue;

    public Factory(Source source, GeometryDocValueReader leftValue, Component2D rightValue) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
    }

    @Override
    public SpatialIntersectsConstantAndConstantEvaluator get(DriverContext context) {
      return new SpatialIntersectsConstantAndConstantEvaluator(source, leftValue, rightValue, context);
    }

    @Override
    public String toString() {
      return "SpatialIntersectsConstantAndConstantEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
    }
  }
}
