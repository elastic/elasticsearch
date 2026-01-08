// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StSimplify}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator point;

  private final double tolerance;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator(Source source,
      EvalOperator.ExpressionEvaluator point, double tolerance, DriverContext driverContext) {
    this.source = source;
    this.point = point;
    this.tolerance = tolerance;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock pointBlock = (LongBlock) point.eval(page)) {
      return eval(page.getPositionCount(), pointBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += point.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, LongBlock pointBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!pointBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          StSimplify.processGeoPointDocValuesAndConstantTolerance(result, p, pointBlock, this.tolerance);
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
    return "StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator[" + "point=" + point + ", tolerance=" + tolerance + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(point);
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

    private final EvalOperator.ExpressionEvaluator.Factory point;

    private final double tolerance;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory point,
        double tolerance) {
      this.source = source;
      this.point = point;
      this.tolerance = tolerance;
    }

    @Override
    public StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator get(
        DriverContext context) {
      return new StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator(source, point.get(context), tolerance, context);
    }

    @Override
    public String toString() {
      return "StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator[" + "point=" + point + ", tolerance=" + tolerance + "]";
    }
  }
}
