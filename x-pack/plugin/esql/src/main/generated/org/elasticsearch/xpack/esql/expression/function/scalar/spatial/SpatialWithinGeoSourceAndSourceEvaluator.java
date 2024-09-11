// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link SpatialWithin}.
 * This class is generated. Do not edit it.
 */
public final class SpatialWithinGeoSourceAndSourceEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator leftValue;

  private final EvalOperator.ExpressionEvaluator rightValue;

  private final DriverContext driverContext;

  public SpatialWithinGeoSourceAndSourceEvaluator(Source source,
      EvalOperator.ExpressionEvaluator leftValue, EvalOperator.ExpressionEvaluator rightValue,
      DriverContext driverContext) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock leftValueBlock = (BytesRefBlock) leftValue.eval(page)) {
      try (BytesRefBlock rightValueBlock = (BytesRefBlock) rightValue.eval(page)) {
        return eval(page.getPositionCount(), leftValueBlock, rightValueBlock);
      }
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock leftValueBlock,
      BytesRefBlock rightValueBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!leftValueBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!rightValueBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          SpatialWithin.processGeoSourceAndSource(result, p, leftValueBlock, rightValueBlock);
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
    return "SpatialWithinGeoSourceAndSourceEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(leftValue, rightValue);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory leftValue;

    private final EvalOperator.ExpressionEvaluator.Factory rightValue;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory leftValue,
        EvalOperator.ExpressionEvaluator.Factory rightValue) {
      this.source = source;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
    }

    @Override
    public SpatialWithinGeoSourceAndSourceEvaluator get(DriverContext context) {
      return new SpatialWithinGeoSourceAndSourceEvaluator(source, leftValue.get(context), rightValue.get(context), context);
    }

    @Override
    public String toString() {
      return "SpatialWithinGeoSourceAndSourceEvaluator[" + "leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
    }
  }
}
