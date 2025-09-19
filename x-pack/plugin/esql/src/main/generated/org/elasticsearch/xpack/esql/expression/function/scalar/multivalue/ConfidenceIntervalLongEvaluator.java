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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
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
public final class ConfidenceIntervalLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConfidenceIntervalLongEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator bestEstimateBlock;

  private final EvalOperator.ExpressionEvaluator estimatesBlock;

  private final EvalOperator.ExpressionEvaluator bucketCountBlock;

  private final EvalOperator.ExpressionEvaluator emptyBucketValueBlock;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ConfidenceIntervalLongEvaluator(Source source,
      EvalOperator.ExpressionEvaluator bestEstimateBlock,
      EvalOperator.ExpressionEvaluator estimatesBlock,
      EvalOperator.ExpressionEvaluator bucketCountBlock,
      EvalOperator.ExpressionEvaluator emptyBucketValueBlock, DriverContext driverContext) {
    this.source = source;
    this.bestEstimateBlock = bestEstimateBlock;
    this.estimatesBlock = estimatesBlock;
    this.bucketCountBlock = bucketCountBlock;
    this.emptyBucketValueBlock = emptyBucketValueBlock;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock bestEstimateBlockBlock = (LongBlock) bestEstimateBlock.eval(page)) {
      try (LongBlock estimatesBlockBlock = (LongBlock) estimatesBlock.eval(page)) {
        try (IntBlock bucketCountBlockBlock = (IntBlock) bucketCountBlock.eval(page)) {
          try (DoubleBlock emptyBucketValueBlockBlock = (DoubleBlock) emptyBucketValueBlock.eval(page)) {
            return eval(page.getPositionCount(), bestEstimateBlockBlock, estimatesBlockBlock, bucketCountBlockBlock, emptyBucketValueBlockBlock);
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
    baseRamBytesUsed += bucketCountBlock.baseRamBytesUsed();
    baseRamBytesUsed += emptyBucketValueBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public LongBlock eval(int positionCount, LongBlock bestEstimateBlockBlock,
      LongBlock estimatesBlockBlock, IntBlock bucketCountBlockBlock,
      DoubleBlock emptyBucketValueBlockBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!bestEstimateBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!estimatesBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!bucketCountBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!emptyBucketValueBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        ConfidenceInterval.process(result, p, bestEstimateBlockBlock, estimatesBlockBlock, bucketCountBlockBlock, emptyBucketValueBlockBlock);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ConfidenceIntervalLongEvaluator[" + "bestEstimateBlock=" + bestEstimateBlock + ", estimatesBlock=" + estimatesBlock + ", bucketCountBlock=" + bucketCountBlock + ", emptyBucketValueBlock=" + emptyBucketValueBlock + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(bestEstimateBlock, estimatesBlock, bucketCountBlock, emptyBucketValueBlock);
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

    private final EvalOperator.ExpressionEvaluator.Factory estimatesBlock;

    private final EvalOperator.ExpressionEvaluator.Factory bucketCountBlock;

    private final EvalOperator.ExpressionEvaluator.Factory emptyBucketValueBlock;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory bestEstimateBlock,
        EvalOperator.ExpressionEvaluator.Factory estimatesBlock,
        EvalOperator.ExpressionEvaluator.Factory bucketCountBlock,
        EvalOperator.ExpressionEvaluator.Factory emptyBucketValueBlock) {
      this.source = source;
      this.bestEstimateBlock = bestEstimateBlock;
      this.estimatesBlock = estimatesBlock;
      this.bucketCountBlock = bucketCountBlock;
      this.emptyBucketValueBlock = emptyBucketValueBlock;
    }

    @Override
    public ConfidenceIntervalLongEvaluator get(DriverContext context) {
      return new ConfidenceIntervalLongEvaluator(source, bestEstimateBlock.get(context), estimatesBlock.get(context), bucketCountBlock.get(context), emptyBucketValueBlock.get(context), context);
    }

    @Override
    public String toString() {
      return "ConfidenceIntervalLongEvaluator[" + "bestEstimateBlock=" + bestEstimateBlock + ", estimatesBlock=" + estimatesBlock + ", bucketCountBlock=" + bucketCountBlock + ", emptyBucketValueBlock=" + emptyBucketValueBlock + "]";
    }
  }
}
