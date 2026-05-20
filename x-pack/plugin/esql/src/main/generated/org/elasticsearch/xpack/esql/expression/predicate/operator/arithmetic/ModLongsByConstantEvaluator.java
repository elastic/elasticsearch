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
import java.util.Optional;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
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
public abstract class ModLongsByConstantEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ModLongsByConstantEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator lhs;

  private final DriverContext driverContext;

  private Warnings warnings;

  public ModLongsByConstantEvaluator(Source source, ExpressionEvaluator lhs,
      DriverContext driverContext) {
    this.source = source;
    this.lhs = lhs;
    this.driverContext = driverContext;
  }

  protected abstract long rhs();

  protected String pathLabel() {
    return "jit-folded";
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock lhsBlock = (LongBlock) lhs.eval(page)) {
      LongVector lhsVector = lhsBlock.asVector();
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

  public LongBlock eval(int positionCount, LongBlock lhsBlock) {
    try(LongBlock.Builder result = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
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
        long lhs = lhsBlock.getLong(lhsBlock.getFirstValueIndex(p));
        result.appendLong(Mod.processLongsByConstant(lhs, rhs()));
      }
      return result.build();
    }
  }

  public LongVector eval(int positionCount, LongVector lhsVector) {
    try(LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        long lhs = lhsVector.getLong(p);
        result.appendLong(p, Mod.processLongsByConstant(lhs, rhs()));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "ModLongsByConstantEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs() + "]" + " (" + pathLabel() + ")";
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

    private final long rhs;

    public Factory(Source source, ExpressionEvaluator.Factory lhs, long rhs) {
      this.source = source;
      this.lhs = lhs;
      this.rhs = rhs;
    }

    @Override
    public ModLongsByConstantEvaluator get(DriverContext context) {
      Optional<Class<? extends ModLongsByConstantEvaluator>> spunClassOpt = JitConstantSpinner.longConstantSubclass(ModLongsByConstantEvaluator.class, "rhs", this.rhs);
      if (spunClassOpt.isPresent()) {
        Class<? extends ModLongsByConstantEvaluator> spunClass = spunClassOpt.get();
        try {
          return (ModLongsByConstantEvaluator) spunClass.getConstructors()[0].newInstance(source, lhs.get(context), context);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException("failed to construct JIT-spun evaluator for ModLongsByConstantEvaluator", e);
        }
      }
      return new Standard(source, lhs.get(context), this.rhs, context);
    }

    @Override
    public String toString() {
      return "ModLongsByConstantEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }
  }

  /**
   * Concrete non-spun subclass used when {@link JitConstantSpinner} returns {@code Optional.empty()}
   * (admission filter rejected the spin). The constant lives in a regular
   * instance field — no JIT-time constant folding, but the per-row work
   * runs correctly. The Factory chooses between this and the spun subclass.
   */
  public static final class Standard extends ModLongsByConstantEvaluator {
    private final long rhs;

    public Standard(Source source, ExpressionEvaluator lhs, long rhs, DriverContext driverContext) {
      super(source, lhs, driverContext);
      this.rhs = rhs;
    }

    @Override
    protected final long rhs() {
      return rhs;
    }

    @Override
    protected final String pathLabel() {
      return "standard";
    }
  }
}
