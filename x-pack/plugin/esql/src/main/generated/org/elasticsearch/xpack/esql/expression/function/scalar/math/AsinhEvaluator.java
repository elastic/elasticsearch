// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Asinh}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class AsinhEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(AsinhEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator val;

  private final DriverContext driverContext;

  private Warnings warnings;

  public AsinhEvaluator(Source source, EvalOperator.ExpressionEvaluator val,
      DriverContext driverContext) {
    this.source = source;
    this.val = val;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (DoubleBlock valBlock = (DoubleBlock) val.eval(page)) {
      DoubleVector valVector = valBlock.asVector();
      if (valVector == null) {
        return eval(page.getPositionCount(), valBlock);
      }
      return eval(page.getPositionCount(), valVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += val.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public DoubleBlock eval(int positionCount, DoubleBlock valBlock) {
    try(DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (valBlock.getValueCount(p)) {
          case 0:
              result.appendNull();
              continue position;
          case 1:
              break;
          default:
              warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
              result.appendNull();
              continue position;
        }
        double val = valBlock.getDouble(valBlock.getFirstValueIndex(p));
        result.appendDouble(Asinh.process(val));
      }
      return result.build();
    }
  }

  public DoubleVector eval(int positionCount, DoubleVector valVector) {
    try(DoubleVector.FixedBuilder result = driverContext.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        double val = valVector.getDouble(p);
        result.appendDouble(p, Asinh.process(val));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "AsinhEvaluator[" + "val=" + val + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(val);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory val;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory val) {
      this.source = source;
      this.val = val;
    }

    @Override
    public AsinhEvaluator get(DriverContext context) {
      return new AsinhEvaluator(source, val.get(context), context);
    }

    @Override
    public String toString() {
      return "AsinhEvaluator[" + "val=" + val + "]";
    }
  }
}
