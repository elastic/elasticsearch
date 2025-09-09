// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link ConfidenceInterval}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ConfidenceIntervalEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConfidenceIntervalEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator bestEstimateBlock;

  private final EvalOperator.ExpressionEvaluator estimates;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ConfidenceIntervalEvaluator(Source source,
      EvalOperator.ExpressionEvaluator bestEstimateBlock,
      EvalOperator.ExpressionEvaluator estimates, DriverContext driverContext) {
    this.source = source;
    this.bestEstimateBlock = bestEstimateBlock;
    this.estimates = estimates;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock bestEstimateBlockBlock = (DoubleBlock) bestEstimateBlock.eval(page)) {
      try (DoubleBlock estimatesBlock = (DoubleBlock) estimates.eval(page)) {
        return eval(page.getPositionCount(), bestEstimateBlockBlock, estimatesBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += bestEstimateBlock.baseRamBytesUsed();
    baseRamBytesUsed += estimates.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock bestEstimateBlockBlock,
      DoubleBlock estimatesBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!bestEstimateBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!estimatesBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        ConfidenceInterval.process(result, p, bestEstimateBlockBlock, estimatesBlock);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ConfidenceIntervalEvaluator[" + "bestEstimateBlock=" + bestEstimateBlock + ", estimates=" + estimates + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(bestEstimateBlock, estimates);
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

    private final EvalOperator.ExpressionEvaluator.Factory bestEstimateBlock;

    private final EvalOperator.ExpressionEvaluator.Factory estimates;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory bestEstimateBlock,
        EvalOperator.ExpressionEvaluator.Factory estimates) {
      this.source = source;
      this.bestEstimateBlock = bestEstimateBlock;
      this.estimates = estimates;
    }

    @Override
    public ConfidenceIntervalEvaluator get(DriverContext context) {
      return new ConfidenceIntervalEvaluator(source, bestEstimateBlock.get(context), estimates.get(context), context);
    }

    @Override
    public String toString() {
      return "ConfidenceIntervalEvaluator[" + "bestEstimateBlock=" + bestEstimateBlock + ", estimates=" + estimates + "]";
    }
  }
}
