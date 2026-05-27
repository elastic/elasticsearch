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
import org.locationtech.jts.operation.buffer.BufferParameters;

/**
 * {@link ExpressionEvaluator} implementation for {@link StBuffer}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator left;

  private final double distance;

  private final BufferParameters bufferParameters;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator(Source source,
      ExpressionEvaluator left, double distance, BufferParameters bufferParameters,
      DriverContext driverContext) {
    this.source = source;
    this.left = left;
    this.distance = distance;
    this.bufferParameters = bufferParameters;
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
          StBuffer.processCartesianPointDocValuesAndConstantDistance(result, p, leftBlock, this.distance, this.bufferParameters);
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
    return "StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator[" + "left=" + left + ", distance=" + distance + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(left);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory left;

    private final double distance;

    private final BufferParameters bufferParameters;

    public Factory(Source source, ExpressionEvaluator.Factory left, double distance,
        BufferParameters bufferParameters) {
      this.source = source;
      this.left = left;
      this.distance = distance;
      this.bufferParameters = bufferParameters;
    }

    @Override
    public StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator get(
        DriverContext context) {
      return new StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator(source, left.get(context), distance, bufferParameters, context);
    }

    @Override
    public String toString() {
      return "StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator[" + "left=" + left + ", distance=" + distance + "]";
    }
  }
}
