// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
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
public final class MvPercentileLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator values;

  private final EvalOperator.ExpressionEvaluator percentile;

  private final MvPercentile.LongSortingScratch scratch;

  private final DriverContext driverContext;

  public MvPercentileLongEvaluator(Source source, EvalOperator.ExpressionEvaluator values,
      EvalOperator.ExpressionEvaluator percentile, MvPercentile.LongSortingScratch scratch,
      DriverContext driverContext) {
    this.values = values;
    this.percentile = percentile;
    this.scratch = scratch;
    this.driverContext = driverContext;
    this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock valuesBlock = (LongBlock) values.eval(page)) {
      try (DoubleBlock percentileBlock = (DoubleBlock) percentile.eval(page)) {
        return eval(page.getPositionCount(), valuesBlock, percentileBlock);
      }
    }
  }

  public LongBlock eval(int positionCount, LongBlock valuesBlock, DoubleBlock percentileBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
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
          MvPercentile.process(result, p, valuesBlock, percentileBlock.getDouble(percentileBlock.getFirstValueIndex(p)), this.scratch);
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
    return "MvPercentileLongEvaluator[" + "values=" + values + ", percentile=" + percentile + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(values, percentile);
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory values;

    private final EvalOperator.ExpressionEvaluator.Factory percentile;

    private final Function<DriverContext, MvPercentile.LongSortingScratch> scratch;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory values,
        EvalOperator.ExpressionEvaluator.Factory percentile,
        Function<DriverContext, MvPercentile.LongSortingScratch> scratch) {
      this.source = source;
      this.values = values;
      this.percentile = percentile;
      this.scratch = scratch;
    }

    @Override
    public MvPercentileLongEvaluator get(DriverContext context) {
      return new MvPercentileLongEvaluator(source, values.get(context), percentile.get(context), scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "MvPercentileLongEvaluator[" + "values=" + values + ", percentile=" + percentile + "]";
    }
  }
}
