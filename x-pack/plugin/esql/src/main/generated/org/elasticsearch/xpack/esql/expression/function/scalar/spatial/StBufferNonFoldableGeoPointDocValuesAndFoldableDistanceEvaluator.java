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
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link StBuffer}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator point;

  private final double distance;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator(Source source,
      ExpressionEvaluator point, double distance, DriverContext driverContext) {
    this.source = source;
    this.point = point;
    this.distance = distance;
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
          StBuffer.processGeoPointDocValuesAndConstantDistance(result, p, pointBlock, this.distance);
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
    return "StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator[" + "point=" + point + ", distance=" + distance + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(point);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory point;

    private final double distance;

    public Factory(Source source, ExpressionEvaluator.Factory point, double distance) {
      this.source = source;
      this.point = point;
      this.distance = distance;
    }

    @Override
    public StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator get(
        DriverContext context) {
      return new StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator(source, point.get(context), distance, context);
    }

    @Override
    public String toString() {
      return "StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator[" + "point=" + point + ", distance=" + distance + "]";
    }
  }
}
