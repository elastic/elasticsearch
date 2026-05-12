// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.locationtech.jts.operation.buffer.BufferParameters;

/**
 * {@link ExpressionEvaluator} implementation for {@link StBuffer}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StBufferNonFoldableGeometryAndFoldableDistanceEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StBufferNonFoldableGeometryAndFoldableDistanceEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator geometry;

  private final double distance;

  private final BufferParameters bufferParameters;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StBufferNonFoldableGeometryAndFoldableDistanceEvaluator(Source source,
      ExpressionEvaluator geometry, double distance, BufferParameters bufferParameters,
      DriverContext driverContext) {
    this.source = source;
    this.geometry = geometry;
    this.distance = distance;
    this.bufferParameters = bufferParameters;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock geometryBlock = (BytesRefBlock) geometry.eval(page)) {
      return eval(page.getPositionCount(), geometryBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += geometry.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock geometryBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!geometryBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          StBuffer.processNonFoldableGeometryAndConstantDistance(result, p, geometryBlock, this.distance, this.bufferParameters);
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
    return "StBufferNonFoldableGeometryAndFoldableDistanceEvaluator[" + "geometry=" + geometry + ", distance=" + distance + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(geometry);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory geometry;

    private final double distance;

    private final BufferParameters bufferParameters;

    public Factory(Source source, ExpressionEvaluator.Factory geometry, double distance,
        BufferParameters bufferParameters) {
      this.source = source;
      this.geometry = geometry;
      this.distance = distance;
      this.bufferParameters = bufferParameters;
    }

    @Override
    public StBufferNonFoldableGeometryAndFoldableDistanceEvaluator get(DriverContext context) {
      return new StBufferNonFoldableGeometryAndFoldableDistanceEvaluator(source, geometry.get(context), distance, bufferParameters, context);
    }

    @Override
    public String toString() {
      return "StBufferNonFoldableGeometryAndFoldableDistanceEvaluator[" + "geometry=" + geometry + ", distance=" + distance + "]";
    }
  }
}
