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
import org.elasticsearch.compute.data.BytesRefVector;
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
public final class StSimplifyNonFoldableGeoAndConstantToleranceEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StSimplifyNonFoldableGeoAndConstantToleranceEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator inputGeometry;

  private final double inputTolerance;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StSimplifyNonFoldableGeoAndConstantToleranceEvaluator(Source source,
      EvalOperator.ExpressionEvaluator inputGeometry, double inputTolerance,
      DriverContext driverContext) {
    this.source = source;
    this.inputGeometry = inputGeometry;
    this.inputTolerance = inputTolerance;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock inputGeometryBlock = (BytesRefBlock) inputGeometry.eval(page)) {
      BytesRefVector inputGeometryVector = inputGeometryBlock.asVector();
      if (inputGeometryVector == null) {
        return eval(page.getPositionCount(), inputGeometryBlock);
      }
      return eval(page.getPositionCount(), inputGeometryVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += inputGeometry.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock inputGeometryBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef inputGeometryScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (inputGeometryBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (inputGeometryBlock.getValueCount(p) != 1) {
          if (inputGeometryBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        BytesRef inputGeometry = inputGeometryBlock.getBytesRef(inputGeometryBlock.getFirstValueIndex(p), inputGeometryScratch);
        try {
          result.appendBytesRef(StSimplify.processNonFoldableGeoAndConstantTolerance(inputGeometry, this.inputTolerance));
        } catch (IllegalArgumentException e) {
          warnings().registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector inputGeometryVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef inputGeometryScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        BytesRef inputGeometry = inputGeometryVector.getBytesRef(p, inputGeometryScratch);
        try {
          result.appendBytesRef(StSimplify.processNonFoldableGeoAndConstantTolerance(inputGeometry, this.inputTolerance));
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
    return "StSimplifyNonFoldableGeoAndConstantToleranceEvaluator[" + "inputGeometry=" + inputGeometry + ", inputTolerance=" + inputTolerance + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(inputGeometry);
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

    private final EvalOperator.ExpressionEvaluator.Factory inputGeometry;

    private final double inputTolerance;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory inputGeometry,
        double inputTolerance) {
      this.source = source;
      this.inputGeometry = inputGeometry;
      this.inputTolerance = inputTolerance;
    }

    @Override
    public StSimplifyNonFoldableGeoAndConstantToleranceEvaluator get(DriverContext context) {
      return new StSimplifyNonFoldableGeoAndConstantToleranceEvaluator(source, inputGeometry.get(context), inputTolerance, context);
    }

    @Override
    public String toString() {
      return "StSimplifyNonFoldableGeoAndConstantToleranceEvaluator[" + "inputGeometry=" + inputGeometry + ", inputTolerance=" + inputTolerance + "]";
    }
  }
}
