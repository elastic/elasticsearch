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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link StXMax}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class StXMaxFromWKBEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(StXMaxFromWKBEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator wkbBlock;

  private final DriverContext driverContext;

  private Warnings warnings;

  public StXMaxFromWKBEvaluator(Source source, EvalOperator.ExpressionEvaluator wkbBlock,
      DriverContext driverContext) {
    this.source = source;
    this.wkbBlock = wkbBlock;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock wkbBlockBlock = (BytesRefBlock) wkbBlock.eval(page)) {
      return eval(page.getPositionCount(), wkbBlockBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += wkbBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, BytesRefBlock wkbBlockBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!wkbBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          StXMax.fromWellKnownBinary(result, p, wkbBlockBlock);
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
    return "StXMaxFromWKBEvaluator[" + "wkbBlock=" + wkbBlock + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(wkbBlock);
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

    private final EvalOperator.ExpressionEvaluator.Factory wkbBlock;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory wkbBlock) {
      this.source = source;
      this.wkbBlock = wkbBlock;
    }

    @Override
    public StXMaxFromWKBEvaluator get(DriverContext context) {
      return new StXMaxFromWKBEvaluator(source, wkbBlock.get(context), context);
    }

    @Override
    public String toString() {
      return "StXMaxFromWKBEvaluator[" + "wkbBlock=" + wkbBlock + "]";
    }
  }
}
