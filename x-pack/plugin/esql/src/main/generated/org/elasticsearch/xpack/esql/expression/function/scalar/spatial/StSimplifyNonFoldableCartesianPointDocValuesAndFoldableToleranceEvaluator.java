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
public final class StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator left;

  private final double tolerance;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator(Source source,
      EvalOperator.ExpressionEvaluator left, double tolerance, DriverContext driverContext) {
    this.source = source;
    this.left = left;
    this.tolerance = tolerance;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock leftBlock = (LongBlock) left.eval(page)) {
      return eval(page.getPositionCount(), leftBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += left.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, LongBlock leftBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
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
          StSimplify.processCartesianPointDocValuesAndConstantTolerance(result, p, leftBlock, this.tolerance);
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
    return "StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator[" + "left=" + left + ", tolerance=" + tolerance + "]";
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

    private final double tolerance;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory left, double tolerance) {
      this.source = source;
      this.left = left;
      this.tolerance = tolerance;
    }

    @Override
    public StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator get(
        DriverContext context) {
      return new StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator(source, left.get(context), tolerance, context);
    }

    @Override
    public String toString() {
      return "StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator[" + "left=" + left + ", tolerance=" + tolerance + "]";
    }
  }
}
