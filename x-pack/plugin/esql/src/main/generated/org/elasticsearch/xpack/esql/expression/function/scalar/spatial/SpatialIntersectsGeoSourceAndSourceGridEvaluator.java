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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link SpatialIntersects}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class SpatialIntersectsGeoSourceAndSourceGridEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Source source;

  private final EvalOperator.ExpressionEvaluator wkb;

  private final EvalOperator.ExpressionEvaluator gridId;

  private final DataType gridType;

  private final DriverContext driverContext;

  private Warnings warnings;

  public SpatialIntersectsGeoSourceAndSourceGridEvaluator(Source source,
      EvalOperator.ExpressionEvaluator wkb, EvalOperator.ExpressionEvaluator gridId,
      DataType gridType, DriverContext driverContext) {
    this.source = source;
    this.wkb = wkb;
    this.gridId = gridId;
    this.gridType = gridType;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock wkbBlock = (BytesRefBlock) wkb.eval(page)) {
      try (LongBlock gridIdBlock = (LongBlock) gridId.eval(page)) {
        return eval(page.getPositionCount(), wkbBlock, gridIdBlock);
      }
    }
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock wkbBlock, LongBlock gridIdBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!wkbBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!gridIdBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          SpatialIntersects.processGeoSourceAndSourceGrid(result, p, wkbBlock, gridIdBlock, this.gridType);
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
    return "SpatialIntersectsGeoSourceAndSourceGridEvaluator[" + "wkb=" + wkb + ", gridId=" + gridId + ", gridType=" + gridType + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(wkb, gridId);
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

    private final EvalOperator.ExpressionEvaluator.Factory wkb;

    private final EvalOperator.ExpressionEvaluator.Factory gridId;

    private final DataType gridType;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory wkb,
        EvalOperator.ExpressionEvaluator.Factory gridId, DataType gridType) {
      this.source = source;
      this.wkb = wkb;
      this.gridId = gridId;
      this.gridType = gridType;
    }

    @Override
    public SpatialIntersectsGeoSourceAndSourceGridEvaluator get(DriverContext context) {
      return new SpatialIntersectsGeoSourceAndSourceGridEvaluator(source, wkb.get(context), gridId.get(context), gridType, context);
    }

    @Override
    public String toString() {
      return "SpatialIntersectsGeoSourceAndSourceGridEvaluator[" + "wkb=" + wkb + ", gridId=" + gridId + ", gridType=" + gridType + "]";
    }
  }
}
