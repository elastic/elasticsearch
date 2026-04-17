// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link ConfidenceInterval}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ConfidenceIntervalEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConfidenceIntervalEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator bestEstimateBlock;

  private final ExpressionEvaluator estimatesBlock;

  private final ExpressionEvaluator trialCountBlock;

  private final ExpressionEvaluator bucketCountBlock;

  private final ExpressionEvaluator confidenceLevelBlock;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ConfidenceIntervalEvaluator(Source source, ExpressionEvaluator bestEstimateBlock,
      ExpressionEvaluator estimatesBlock, ExpressionEvaluator trialCountBlock,
      ExpressionEvaluator bucketCountBlock, ExpressionEvaluator confidenceLevelBlock,
      DriverContext driverContext) {
    this.source = source;
    this.bestEstimateBlock = bestEstimateBlock;
    this.estimatesBlock = estimatesBlock;
    this.trialCountBlock = trialCountBlock;
    this.bucketCountBlock = bucketCountBlock;
    this.confidenceLevelBlock = confidenceLevelBlock;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock bestEstimateBlockBlock = (DoubleBlock) bestEstimateBlock.eval(page)) {
      try (DoubleBlock estimatesBlockBlock = (DoubleBlock) estimatesBlock.eval(page)) {
        try (IntBlock trialCountBlockBlock = (IntBlock) trialCountBlock.eval(page)) {
          try (IntBlock bucketCountBlockBlock = (IntBlock) bucketCountBlock.eval(page)) {
            try (DoubleBlock confidenceLevelBlockBlock = (DoubleBlock) confidenceLevelBlock.eval(page)) {
              return eval(page.getPositionCount(), bestEstimateBlockBlock, estimatesBlockBlock, trialCountBlockBlock, bucketCountBlockBlock, confidenceLevelBlockBlock);
            }
          }
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += bestEstimateBlock.baseRamBytesUsed();
    baseRamBytesUsed += estimatesBlock.baseRamBytesUsed();
    baseRamBytesUsed += trialCountBlock.baseRamBytesUsed();
    baseRamBytesUsed += bucketCountBlock.baseRamBytesUsed();
    baseRamBytesUsed += confidenceLevelBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock bestEstimateBlockBlock,
      DoubleBlock estimatesBlockBlock, IntBlock trialCountBlockBlock,
      IntBlock bucketCountBlockBlock, DoubleBlock confidenceLevelBlockBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!bestEstimateBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!estimatesBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!trialCountBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!bucketCountBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!confidenceLevelBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        ConfidenceInterval.process(result, p, bestEstimateBlockBlock, estimatesBlockBlock, trialCountBlockBlock, bucketCountBlockBlock, confidenceLevelBlockBlock);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ConfidenceIntervalEvaluator[" + "bestEstimateBlock=" + bestEstimateBlock + ", estimatesBlock=" + estimatesBlock + ", trialCountBlock=" + trialCountBlock + ", bucketCountBlock=" + bucketCountBlock + ", confidenceLevelBlock=" + confidenceLevelBlock + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(bestEstimateBlock, estimatesBlock, trialCountBlock, bucketCountBlock, confidenceLevelBlock);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory bestEstimateBlock;

    private final ExpressionEvaluator.Factory estimatesBlock;

    private final ExpressionEvaluator.Factory trialCountBlock;

    private final ExpressionEvaluator.Factory bucketCountBlock;

    private final ExpressionEvaluator.Factory confidenceLevelBlock;

    public Factory(Source source, ExpressionEvaluator.Factory bestEstimateBlock,
        ExpressionEvaluator.Factory estimatesBlock, ExpressionEvaluator.Factory trialCountBlock,
        ExpressionEvaluator.Factory bucketCountBlock,
        ExpressionEvaluator.Factory confidenceLevelBlock) {
      this.source = source;
      this.bestEstimateBlock = bestEstimateBlock;
      this.estimatesBlock = estimatesBlock;
      this.trialCountBlock = trialCountBlock;
      this.bucketCountBlock = bucketCountBlock;
      this.confidenceLevelBlock = confidenceLevelBlock;
    }

    @Override
    public ConfidenceIntervalEvaluator get(DriverContext context) {
      return new ConfidenceIntervalEvaluator(source, bestEstimateBlock.get(context), estimatesBlock.get(context), trialCountBlock.get(context), bucketCountBlock.get(context), confidenceLevelBlock.get(context), context);
    }

    @Override
    public String toString() {
      return "ConfidenceIntervalEvaluator[" + "bestEstimateBlock=" + bestEstimateBlock + ", estimatesBlock=" + estimatesBlock + ", trialCountBlock=" + trialCountBlock + ", bucketCountBlock=" + bucketCountBlock + ", confidenceLevelBlock=" + confidenceLevelBlock + "]";
    }
  }
}
