// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import java.lang.Class;
import java.lang.IllegalAccessException;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.InstantiationException;
import java.lang.Override;
import java.lang.String;
import java.lang.reflect.InvocationTargetException;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.JitConstantSpinner;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link Mod}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public abstract class ModIntsByConstantEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ModIntsByConstantEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator lhs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ModIntsByConstantEvaluator(Source source, ExpressionEvaluator lhs,
      DriverContext driverContext) {
    this.source = source;
    this.lhs = lhs;
    this.driverContext = driverContext;
  }

  protected abstract int rhs();

  @Override
  public Block eval(Page page) {
    try (IntBlock lhsBlock = (IntBlock) lhs.eval(page)) {
      IntVector lhsVector = lhsBlock.asVector();
      if (lhsVector == null) {
        return eval(page.getPositionCount(), lhsBlock);
      }
      return eval(page.getPositionCount(), lhsVector).asBlock();
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += lhs.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public IntBlock eval(int positionCount, IntBlock lhsBlock) {
    try(IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        switch (lhsBlock.getValueCount(p)) {
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
        int lhs = lhsBlock.getInt(lhsBlock.getFirstValueIndex(p));
        result.appendInt(Mod.processIntsByConstant(lhs, rhs()));
      }
      return result.build();
    }
  }

  public IntVector eval(int positionCount, IntVector lhsVector) {
    try(IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        int lhs = lhsVector.getInt(p);
        result.appendInt(p, Mod.processIntsByConstant(lhs, rhs()));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ModIntsByConstantEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs() + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(lhs);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory lhs;

    private final int rhs;

    public Factory(Source source, ExpressionEvaluator.Factory lhs, int rhs) {
      this.source = source;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public ModIntsByConstantEvaluator get(DriverContext context) {
      Class<? extends ModIntsByConstantEvaluator> spunClass = JitConstantSpinner.intConstantSubclass(ModIntsByConstantEvaluator.class, "rhs", this.rhs).orElseThrow(() -> new IllegalStateException("JitConstantSpinner cache exhausted for ModIntsByConstantEvaluator value=" + this.rhs));
      try {
        return (ModIntsByConstantEvaluator) spunClass.getDeclaredConstructors()[0].newInstance(source, lhs.get(context), context);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to construct JIT-spun evaluator for ModIntsByConstantEvaluator", e);
      }
    }

    @Override
    public String toString() {
      return "ModIntsByConstantEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }
}
