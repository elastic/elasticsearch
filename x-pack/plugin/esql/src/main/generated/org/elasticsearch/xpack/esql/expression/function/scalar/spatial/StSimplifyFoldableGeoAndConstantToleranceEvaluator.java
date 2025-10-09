// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StSimplify}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StSimplifyFoldableGeoAndConstantToleranceEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StSimplifyFoldableGeoAndConstantToleranceEvaluator.class);

  private final Source source;

  private final BytesRef inputGeometry;

  private final double inputTolerance;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StSimplifyFoldableGeoAndConstantToleranceEvaluator(Source source, BytesRef inputGeometry,
      double inputTolerance, DriverContext driverContext) {
    this.source = source;
    this.inputGeometry = inputGeometry;
    this.inputTolerance = inputTolerance;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    return eval(page.getPositionCount());
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        try {
          result.appendBytesRef(StSimplify.processFoldableGeoAndConstantTolerance(this.inputGeometry, this.inputTolerance));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "StSimplifyFoldableGeoAndConstantToleranceEvaluator[" + "inputGeometry=" + inputGeometry + ", inputTolerance=" + inputTolerance + "]";
  }

  @Override
  public void close() {
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

    private final BytesRef inputGeometry;

    private final double inputTolerance;

    public Factory(Source source, BytesRef inputGeometry, double inputTolerance) {
      this.source = source;
      this.inputGeometry = inputGeometry;
      this.inputTolerance = inputTolerance;
    }

    @Override
    public StSimplifyFoldableGeoAndConstantToleranceEvaluator get(DriverContext context) {
      return new StSimplifyFoldableGeoAndConstantToleranceEvaluator(source, inputGeometry, inputTolerance, context);
    }

    @Override
    public String toString() {
      return "StSimplifyFoldableGeoAndConstantToleranceEvaluator[" + "inputGeometry=" + inputGeometry + ", inputTolerance=" + inputTolerance + "]";
    }
  }
}
