// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Class;
import java.lang.IllegalAccessException;
import java.lang.IllegalStateException;
import java.lang.InstantiationException;
import java.lang.Override;
import java.lang.String;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.ConstantMethodResultSpecializer;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link MvInRange}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public abstract class MvInRangeFoldedLongEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvInRangeFoldedLongEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator field;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvInRangeFoldedLongEvaluator(Source source, ExpressionEvaluator field,
      DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.driverContext = driverContext;
  }

  protected abstract MvInRange.LongBounds bounds();

  protected String pathLabel() {
    return "jit-folded";
  }

  @Override
  public Block eval(Page page) {
    try (LongBlock fieldBlock = (LongBlock) field.eval(page)) {
      return eval(page.getPositionCount(), fieldBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, LongBlock fieldBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(MvInRange.processFoldedLong(p, fieldBlock, bounds()));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvInRangeFoldedLongEvaluator[" + "field=" + field + ", bounds=" + bounds() + "]" + " (" + pathLabel() + ")";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory field;

    private final MvInRange.LongBounds bounds;

    public Factory(Source source, ExpressionEvaluator.Factory field, MvInRange.LongBounds bounds) {
      this.source = source;
      this.field = field;
      this.bounds = bounds;
    }

    @Override
    public MvInRangeFoldedLongEvaluator get(DriverContext context) {
      Optional<Class<? extends MvInRangeFoldedLongEvaluator>> constantSpecializedClassOpt = ConstantMethodResultSpecializer.SHARED.specializeReference(MvInRangeFoldedLongEvaluator.class, "bounds", MvInRange.LongBounds.class, this.bounds);
      if (constantSpecializedClassOpt.isPresent()) {
        Class<? extends MvInRangeFoldedLongEvaluator> constantSpecializedClass = constantSpecializedClassOpt.get();
        try {
          return (MvInRangeFoldedLongEvaluator) constantSpecializedClass.getConstructors()[0].newInstance(source, field.get(context), context);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException("failed to construct specialized evaluator for MvInRangeFoldedLongEvaluator", e);
        }
      }
      return new Standard(source, field.get(context), this.bounds, context);
    }

    @Override
    public String toString() {
      return "MvInRangeFoldedLongEvaluator[" + "field=" + field + ", bounds=" + bounds + "]";
    }
  }

  /**
   * Concrete non-constant-specialized subclass used when {@link ConstantMethodResultSpecializer} returns {@code Optional.empty()}
   * (admission filter rejected the spin). The constant lives in a regular
   * instance field — no JIT-time constant folding, but the per-row work
   * runs correctly. The Factory chooses between this and the constant-specialized subclass.
   */
  public static final class Standard extends MvInRangeFoldedLongEvaluator {
    private final MvInRange.LongBounds bounds;

    public Standard(Source source, ExpressionEvaluator field, MvInRange.LongBounds bounds,
        DriverContext driverContext) {
      super(source, field, driverContext);
      this.bounds = bounds;
    }

    @Override
    protected final MvInRange.LongBounds bounds() {
      return bounds;
    }

    @Override
    protected final String pathLabel() {
      return "standard";
    }
  }
}
