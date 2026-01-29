// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Random}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class RandomEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RandomEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator bound;

  private final DriverContext driverContext;

  private Warnings warnings;

  public RandomEvaluator(Source source, EvalOperator.ExpressionEvaluator bound,
      DriverContext driverContext) {
    this.source = source;
    this.bound = bound;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (IntBlock boundBlock = (IntBlock) bound.eval(page)) {
      IntVector boundVector = boundBlock.asVector();
      if (boundVector == null) {
        return eval(page.getPositionCount(), boundBlock);
      }
      return eval(page.getPositionCount(), boundVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += bound.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, IntBlock boundBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (boundBlock.getValueCount(p)) {
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
        int bound = boundBlock.getInt(boundBlock.getFirstValueIndex(p));
        result.appendInt(Random.process(bound));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, IntVector boundVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int bound = boundVector.getInt(p);
        result.appendInt(p, Random.process(bound));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "RandomEvaluator[" + "bound=" + bound + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(bound);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory bound;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory bound) {
      this.source = source;
      this.bound = bound;
    }

    @Override
    public RandomEvaluator get(DriverContext context) {
      return new RandomEvaluator(source, bound.get(context), context);
    }

    @Override
    public String toString() {
      return "RandomEvaluator[" + "bound=" + bound + "]";
    }
  }
}
