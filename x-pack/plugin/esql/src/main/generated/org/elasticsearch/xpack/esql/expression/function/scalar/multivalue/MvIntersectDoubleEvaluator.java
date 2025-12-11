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
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvIntersect}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MvIntersectDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvIntersectDoubleEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator firstValue;

  private final EvalOperator.ExpressionEvaluator secondValue;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvIntersectDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator firstValue,
      EvalOperator.ExpressionEvaluator secondValue, DriverContext driverContext) {
    this.source = source;
    this.firstValue = firstValue;
    this.secondValue = secondValue;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock firstValueBlock = (DoubleBlock) firstValue.eval(page)) {
      try (DoubleBlock secondValueBlock = (DoubleBlock) secondValue.eval(page)) {
        return eval(page.getPositionCount(), firstValueBlock, secondValueBlock);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += firstValue.baseRamBytesUsed();
    baseRamBytesUsed += secondValue.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock firstValueBlock,
      DoubleBlock secondValueBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!firstValueBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (!secondValueBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        MvIntersect.process(result, p, firstValueBlock, secondValueBlock);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvIntersectDoubleEvaluator[" + "firstValue=" + firstValue + ", secondValue=" + secondValue + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(firstValue, secondValue);
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

    private final EvalOperator.ExpressionEvaluator.Factory firstValue;

    private final EvalOperator.ExpressionEvaluator.Factory secondValue;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory firstValue,
        EvalOperator.ExpressionEvaluator.Factory secondValue) {
      this.source = source;
      this.firstValue = firstValue;
      this.secondValue = secondValue;
    }

    @Override
    public MvIntersectDoubleEvaluator get(DriverContext context) {
      return new MvIntersectDoubleEvaluator(source, firstValue.get(context), secondValue.get(context), context);
    }

    @Override
    public String toString() {
      return "MvIntersectDoubleEvaluator[" + "firstValue=" + firstValue + ", secondValue=" + secondValue + "]";
    }
  }
}
