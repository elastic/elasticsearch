// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Warnings;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvPercentile}.
 * This class is generated. Do not edit it.
 */
public final class MvPercentileDoubleIntegerEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator values;

  private final EvalOperator.ExpressionEvaluator percentile;

  private final DriverContext driverContext;

  public MvPercentileDoubleIntegerEvaluator(Source source, EvalOperator.ExpressionEvaluator values,
      EvalOperator.ExpressionEvaluator percentile, DriverContext driverContext) {
    this.values = values;
    this.percentile = percentile;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock valuesBlock = (DoubleBlock) values.eval(page)) {
      try (IntBlock percentileBlock = (IntBlock) percentile.eval(page)) {
        return eval(page.getPositionCount(), valuesBlock, percentileBlock);
      }
    }
  }

  public DoubleBlock eval(int positionCount, DoubleBlock valuesBlock, IntBlock percentileBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        boolean allBlocksAreNulls = true;
        if (!valuesBlock.isNull(p)) {
          allBlocksAreNulls = false;
        }
        if (percentileBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (percentileBlock.getValueCount(p) != 1) {
          if (percentileBlock.getValueCount(p) > 1) {
            warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (allBlocksAreNulls) {
          result.appendNull();
          continue position;
        }
        try {
          MvPercentile.process(result, p, valuesBlock, percentileBlock.getInt(percentileBlock.getFirstValueIndex(p)));
        } catch (IllegalArgumentException e) {
          warnings.registerException(e);
          result.appendNull();
        }
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvPercentileDoubleIntegerEvaluator[" + "values=" + values + ", percentile=" + percentile + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(values, percentile);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory values;

    private final EvalOperator.ExpressionEvaluator.Factory percentile;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory values,
        EvalOperator.ExpressionEvaluator.Factory percentile) {
      this.source = source;
      this.values = values;
      this.percentile = percentile;
    }

    @Override
    public MvPercentileDoubleIntegerEvaluator get(DriverContext context) {
      return new MvPercentileDoubleIntegerEvaluator(source, values.get(context), percentile.get(context), context);
    }

    @Override
    public String toString() {
      return "MvPercentileDoubleIntegerEvaluator[" + "values=" + values + ", percentile=" + percentile + "]";
    }
  }
}
