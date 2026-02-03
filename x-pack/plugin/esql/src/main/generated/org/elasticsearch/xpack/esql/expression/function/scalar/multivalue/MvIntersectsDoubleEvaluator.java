// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvIntersects}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MvIntersectsDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvIntersectsDoubleEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator left;

  private final EvalOperator.ExpressionEvaluator right;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvIntersectsDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator left,
      EvalOperator.ExpressionEvaluator right, DriverContext driverContext) {
    this.source = source;
    this.left = left;
    this.right = right;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock leftBlock = (DoubleBlock) left.eval(page)) {
      try (DoubleBlock rightBlock = (DoubleBlock) right.eval(page)) {
        return eval(page.getPositionCount(), leftBlock, rightBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += left.baseRamBytesUsed();
    baseRamBytesUsed += right.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, DoubleBlock leftBlock, DoubleBlock rightBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(MvIntersects.process(p, leftBlock, rightBlock));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvIntersectsDoubleEvaluator[" + "left=" + left + ", right=" + right + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(left, right);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory left;

    private final EvalOperator.ExpressionEvaluator.Factory right;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory left,
        EvalOperator.ExpressionEvaluator.Factory right) {
      this.source = source;
      this.left = left;
      this.right = right;
    }

    @Override
    public MvIntersectsDoubleEvaluator get(DriverContext context) {
      return new MvIntersectsDoubleEvaluator(source, left.get(context), right.get(context), context);
    }

    @Override
    public String toString() {
      return "MvIntersectsDoubleEvaluator[" + "left=" + left + ", right=" + right + "]";
    }
  }
}
