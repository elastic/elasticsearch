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
import org.elasticsearch.compute.data.LongBlock;
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
public final class ReliableLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ReliableLongEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator estimatesBlock;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ReliableLongEvaluator(Source source, EvalOperator.ExpressionEvaluator estimatesBlock,
      DriverContext driverContext) {
    this.source = source;
    this.estimatesBlock = estimatesBlock;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock estimatesBlockBlock = (LongBlock) estimatesBlock.eval(page)) {
      return eval(page.getPositionCount(), estimatesBlockBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += estimatesBlock.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock estimatesBlockBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!estimatesBlockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        Reliable.process(result, p, estimatesBlockBlock);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ReliableLongEvaluator[" + "estimatesBlock=" + estimatesBlock + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(estimatesBlock);
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

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory estimatesBlock) {
      this.source = source;
      this.estimatesBlock = estimatesBlock;
    }

    @Override
    public ReliableLongEvaluator get(DriverContext context) {
      return new ReliableLongEvaluator(source, estimatesBlock.get(context), context);
    }

    @Override
    public String toString() {
      return "ReliableLongEvaluator[" + "estimatesBlock=" + estimatesBlock + "]";
    }
  }
}
