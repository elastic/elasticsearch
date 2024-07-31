// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvPSeriesWeightedSum}.
 * This class is generated. Do not edit it.
 */
public final class MvPSeriesWeightedSumDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator block;

  private final CompensatedSum sum;

  private final double p;

  private final DriverContext driverContext;

  public MvPSeriesWeightedSumDoubleEvaluator(Source source, EvalOperator.ExpressionEvaluator block,
      CompensatedSum sum, double p, DriverContext driverContext) {
    this.block = block;
    this.sum = sum;
    this.p = p;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock blockBlock = (DoubleBlock) block.eval(page)) {
      return eval(page.getPositionCount(), blockBlock);
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock blockBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!blockBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        MvPSeriesWeightedSum.process(result, p, blockBlock, this.sum, this.p);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvPSeriesWeightedSumDoubleEvaluator[" + "block=" + block + ", p=" + p + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(block);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory block;

    private final Function<DriverContext, CompensatedSum> sum;

    private final double p;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory block,
        Function<DriverContext, CompensatedSum> sum, double p) {
      this.source = source;
      this.block = block;
      this.sum = sum;
      this.p = p;
    }

    @Override
    public MvPSeriesWeightedSumDoubleEvaluator get(DriverContext context) {
      return new MvPSeriesWeightedSumDoubleEvaluator(source, block.get(context), sum.apply(context), p, context);
    }

    @Override
    public String toString() {
      return "MvPSeriesWeightedSumDoubleEvaluator[" + "block=" + block + ", p=" + p + "]";
    }
  }
}
