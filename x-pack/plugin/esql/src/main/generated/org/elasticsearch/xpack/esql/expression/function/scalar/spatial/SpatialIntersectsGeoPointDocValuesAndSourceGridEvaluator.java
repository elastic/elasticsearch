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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * {@link ExpressionEvaluator} implementation for {@link SpatialIntersects}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class SpatialIntersectsGeoPointDocValuesAndSourceGridEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SpatialIntersectsGeoPointDocValuesAndSourceGridEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator encodedPoints;

  private final ExpressionEvaluator gridIds;

  private final DataType gridType;

  private final DriverContext driverContext;

  private Warnings warnings;

  public SpatialIntersectsGeoPointDocValuesAndSourceGridEvaluator(Source source,
      ExpressionEvaluator encodedPoints, ExpressionEvaluator gridIds, DataType gridType,
      DriverContext driverContext) {
    this.source = source;
    this.encodedPoints = encodedPoints;
    this.gridIds = gridIds;
    this.gridType = gridType;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock encodedPointsBlock = (LongBlock) encodedPoints.eval(page)) {
      try (LongBlock gridIdsBlock = (LongBlock) gridIds.eval(page)) {
        return eval(page.getPositionCount(), encodedPointsBlock, gridIdsBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += encodedPoints.baseRamBytesUsed();
    baseRamBytesUsed += gridIds.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock encodedPointsBlock,
      LongBlock gridIdsBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!encodedPointsBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!gridIdsBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          SpatialIntersects.processGeoPointDocValuesAndSourceGrid(result, p, encodedPointsBlock, gridIdsBlock, this.gridType);
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
    return "SpatialIntersectsGeoPointDocValuesAndSourceGridEvaluator[" + "encodedPoints=" + encodedPoints + ", gridIds=" + gridIds + ", gridType=" + gridType + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(encodedPoints, gridIds);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory encodedPoints;

    private final ExpressionEvaluator.Factory gridIds;

    private final DataType gridType;

    public Factory(Source source, ExpressionEvaluator.Factory encodedPoints,
        ExpressionEvaluator.Factory gridIds, DataType gridType) {
      this.source = source;
      this.encodedPoints = encodedPoints;
      this.gridIds = gridIds;
      this.gridType = gridType;
    }

    @Override
    public SpatialIntersectsGeoPointDocValuesAndSourceGridEvaluator get(DriverContext context) {
      return new SpatialIntersectsGeoPointDocValuesAndSourceGridEvaluator(source, encodedPoints.get(context), gridIds.get(context), gridType, context);
    }

    @Override
    public String toString() {
      return "SpatialIntersectsGeoPointDocValuesAndSourceGridEvaluator[" + "encodedPoints=" + encodedPoints + ", gridIds=" + gridIds + ", gridType=" + gridType + "]";
    }
  }
}
