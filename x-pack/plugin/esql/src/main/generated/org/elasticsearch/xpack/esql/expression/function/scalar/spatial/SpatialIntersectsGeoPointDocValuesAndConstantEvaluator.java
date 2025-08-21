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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link SpatialIntersects}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class SpatialIntersectsGeoPointDocValuesAndConstantEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator left;

  private final Component2D right;

  private final DriverContext driverContext;

  private Warnings warnings;

  public SpatialIntersectsGeoPointDocValuesAndConstantEvaluator(Source source,
      EvalOperator.ExpressionEvaluator left, Component2D right, DriverContext driverContext) {
    this.source = source;
    this.left = left;
    this.right = right;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock leftBlock = (LongBlock) left.eval(page)) {
      return eval(page.getPositionCount(), leftBlock);
    }
  }

  public BooleanBlock eval(int positionCount, LongBlock leftBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!leftBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          SpatialIntersects.processGeoPointDocValuesAndConstant(result, p, leftBlock, this.right);
        } catch (IllegalArgumentException | IOException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "SpatialIntersectsGeoPointDocValuesAndConstantEvaluator[" + "left=" + left + ", right=" + right + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(left);
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

    private final EvalOperator.ExpressionEvaluator.Factory left;

    private final Component2D right;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory left,
        Component2D right) {
      this.source = source;
      this.left = left;
      this.right = right;
    }

    @Override
    public SpatialIntersectsGeoPointDocValuesAndConstantEvaluator get(DriverContext context) {
      return new SpatialIntersectsGeoPointDocValuesAndConstantEvaluator(source, left.get(context), right, context);
    }

    @Override
    public String toString() {
      return "SpatialIntersectsGeoPointDocValuesAndConstantEvaluator[" + "left=" + left + ", right=" + right + "]";
    }
  }
}
