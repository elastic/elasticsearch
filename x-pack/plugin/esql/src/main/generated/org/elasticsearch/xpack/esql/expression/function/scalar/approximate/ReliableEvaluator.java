// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Reliable}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class ReliableEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReliableEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator estimatesBlock;

  private final EvalOperator.ExpressionEvaluator trialCountBlock;

  private final EvalOperator.ExpressionEvaluator bucketCountBlock;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ReliableEvaluator(Source source, EvalOperator.ExpressionEvaluator estimatesBlock,
      EvalOperator.ExpressionEvaluator trialCountBlock,
      EvalOperator.ExpressionEvaluator bucketCountBlock, DriverContext driverContext) {
    this.source = source;
    this.estimatesBlock = estimatesBlock;
    this.trialCountBlock = trialCountBlock;
    this.bucketCountBlock = bucketCountBlock;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock estimatesBlockBlock = (DoubleBlock) estimatesBlock.eval(page)) {
      try (IntBlock trialCountBlockBlock = (IntBlock) trialCountBlock.eval(page)) {
        try (IntBlock bucketCountBlockBlock = (IntBlock) bucketCountBlock.eval(page)) {
          return eval(page.getPositionCount(), estimatesBlockBlock, trialCountBlockBlock, bucketCountBlockBlock);
        }
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += estimatesBlock.baseRamBytesUsed();
    baseRamBytesUsed += trialCountBlock.baseRamBytesUsed();
    baseRamBytesUsed += bucketCountBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, DoubleBlock estimatesBlockBlock,
      IntBlock trialCountBlockBlock, IntBlock bucketCountBlockBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!estimatesBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!trialCountBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!bucketCountBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        Reliable.process(result, p, estimatesBlockBlock, trialCountBlockBlock, bucketCountBlockBlock);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ReliableEvaluator[" + "estimatesBlock=" + estimatesBlock + ", trialCountBlock=" + trialCountBlock + ", bucketCountBlock=" + bucketCountBlock + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(estimatesBlock, trialCountBlock, bucketCountBlock);
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

    private final EvalOperator.ExpressionEvaluator.Factory estimatesBlock;

    private final EvalOperator.ExpressionEvaluator.Factory trialCountBlock;

    private final EvalOperator.ExpressionEvaluator.Factory bucketCountBlock;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory estimatesBlock,
        EvalOperator.ExpressionEvaluator.Factory trialCountBlock,
        EvalOperator.ExpressionEvaluator.Factory bucketCountBlock) {
      this.source = source;
      this.estimatesBlock = estimatesBlock;
      this.trialCountBlock = trialCountBlock;
      this.bucketCountBlock = bucketCountBlock;
    }

    @Override
    public ReliableEvaluator get(DriverContext context) {
      return new ReliableEvaluator(source, estimatesBlock.get(context), trialCountBlock.get(context), bucketCountBlock.get(context), context);
    }

    @Override
    public String toString() {
      return "ReliableEvaluator[" + "estimatesBlock=" + estimatesBlock + ", trialCountBlock=" + trialCountBlock + ", bucketCountBlock=" + bucketCountBlock + "]";
    }
  }
}
